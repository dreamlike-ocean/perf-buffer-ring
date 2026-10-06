package io.github.dreamlike.proxy.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class PerfClientMain {

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("--help")) {
            System.out.println("PerfClientMain [qps=1000|max] [durationSeconds=60] [warmupSeconds=30] [requestBytes=2048]");
            System.out.println("Properties: perf.host, perf.port, perf.h1.connections, perf.h2.connections, perf.h2.streams");
            System.out.println("max: unlimited paired H1/H2 requests, sweeping 64/128/256/512/1024/2048 concurrent pairs");
            return;
        }
        boolean maxThroughput = args.length > 0 && args[0].equals("max");
        int qps = maxThroughput ? 0 : args.length > 0 ? Integer.parseInt(args[0]) : 1000;
        int duration = args.length > 1 ? Integer.parseInt(args[1]) : 60;
        int warmup = args.length > 2 ? Integer.parseInt(args[2]) : 30;
        int requestBytes = args.length > 3 ? Integer.parseInt(args[3]) : 2048;
        if ((!maxThroughput && qps <= 0) || qps % 2 != 0 || duration <= 0 || warmup < 0 || requestBytes < 14 || requestBytes > 16 * 1024) {
            throw new IllegalArgumentException("QPS must be positive and even, duration > 0, warmup >= 0, requestBytes 14..16384");
        }
        String host = System.getProperty("perf.host", "127.0.0.1");
        int port = Integer.getInteger("perf.port", 4399);
        if (maxThroughput) {
            MaxThroughputTest.run(host, port, duration, warmup, requestBytes);
            return;
        }
        int h1Connections = Integer.getInteger("perf.h1.connections", 64);
        int h2Connections = Integer.getInteger("perf.h2.connections", 4);
        int h2Streams = Integer.getInteger("perf.h2.streams", 32);
        if (h1Connections <= 0 || h2Connections <= 0 || h2Streams <= 0) {
            throw new IllegalArgumentException("Connection and stream counts must be positive");
        }
        int totalRequests = Math.multiplyExact(qps, Math.addExact(warmup, duration));
        int warmupRequests = Math.multiplyExact(qps, warmup);
        CountDownLatch completed = new CountDownLatch(totalRequests);
        Runnable onCompleted = completed::countDown;
        RequestStats h1 = new RequestStats("h1", qps / 2, duration);
        RequestStats h2 = new RequestStats("h2", qps / 2, duration);

        try (MixedHttpClient client = new MixedHttpClient(host, port, requestBytes, h1Connections, h2Connections, h2Streams)) {
            System.out.printf("target=%d QPS, h1=%d, h2=%d, warmup=%ds, duration=%ds, body=%d bytes%n",
                    qps, qps / 2, qps / 2, warmup, duration, requestBytes);
            System.out.printf("connections: h1=%d (one in-flight), h2=%d (streams/connection=%d)%n",
                    h1Connections, h2Connections, h2Streams);
            long start = System.nanoTime();
            long measurementStart = start + TimeUnit.SECONDS.toNanos(warmup);
            long measurementEnd = measurementStart + TimeUnit.SECONDS.toNanos(duration);
            h1.setWindow(measurementStart, measurementEnd);
            h2.setWindow(measurementStart, measurementEnd);
            for (int i = 0; i < totalRequests; i++) {
                long scheduled = start + (long) i * 1_000_000_000L / qps;
                waitUntil(scheduled);
                boolean http2 = (i & 1) != 0;
                client.send(http2, http2 ? h2 : h1, scheduled, i >= warmupRequests, onCompleted);
            }
            waitUntil(measurementEnd);
            if (!completed.await(10, TimeUnit.SECONDS)) {
                System.err.println("Drain timeout: " + completed.getCount() + " requests unfinished");
            }
        }
        RequestStats.print(h1, h2);
    }

    private static void waitUntil(long deadline) throws InterruptedException {
        for (long remaining; (remaining = deadline - System.nanoTime()) > 0;) {
            LockSupport.parkNanos(remaining);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }
}
