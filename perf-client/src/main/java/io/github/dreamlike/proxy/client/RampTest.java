package io.github.dreamlike.proxy.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

/**
 * Open-loop ramp: each step sends requests strictly on schedule at the target QPS, alternating H1/H2 by sequence
 * number, so both protocols get exactly half of the QPS. The ramp stops at the first step whose actual QPS falls behind
 * the target, or that has failed, pool-busy or unfinished requests.
 *
 * <p>The load is split across {@code perf.client.loops} independent {@link MixedHttpClient}s, each with its own event
 * loop and connections: request i of the global schedule is sent by client {@code i % loops}, and H1/H2 still
 * alternate by the global sequence number.
 */
final class RampTest {
    // A step falls behind when its actual QPS is below this fraction of the target.
    private static final double SATURATION_RATIO = 0.95;
    // NioIoHandler only does a non-blocking selectNow when the deadline is less than about 0.5 ms away, so waking up at
    // the request interval would spin the event loop. Wake up at most once per millisecond and send everything due.
    private static final long MIN_TICK_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

    static void run(String host, int port, int requestBytes) throws Exception {
        int startQps = Integer.getInteger("perf.ramp.start", 20_000);
        int stepQps = Integer.getInteger("perf.ramp.step", 20_000);
        int maxQps = Integer.getInteger("perf.ramp.max", 400_000);
        int initialWarmup = Integer.getInteger("perf.ramp.initialWarmup", 15);
        int settle = Integer.getInteger("perf.ramp.settle", 5);
        int duration = Integer.getInteger("perf.ramp.duration", 15);
        int loops = Integer.getInteger("perf.client.loops", 5);
        // Open loop with one H1 request per connection at a time: in-flight H1 requests are about QPS / 2 × latency, so size
        // the connections for the highest step. These are totals across all clients, split evenly between them.
        int h1Connections = Integer.getInteger("perf.h1.connections", 8192);
        int h2Connections = Integer.getInteger("perf.h2.connections", 128);
        // Below the server's default of 100 concurrent streams, so the stream limit does not affect the results.
        int h2Streams = Integer.getInteger("perf.h2.streams", 64);
        if (startQps <= 0 || stepQps <= 0 || maxQps < startQps || startQps % 2 != 0 || stepQps % 2 != 0
                || initialWarmup < 0 || settle < 0 || duration <= 0 || loops <= 0
                || h1Connections < loops || h2Connections < loops) {
            throw new IllegalArgumentException("Ramp QPS must be positive and even, max >= start, duration > 0, "
                    + "and every client loop needs at least one H1 and one H2 connection");
        }
        CpuUsage cpu = CpuUsage.fromSystemProperties();
        List<MixedHttpClient> clients = new ArrayList<>(loops);
        try {
            for (int i = 0; i < loops; i++) {
                clients.add(new MixedHttpClient(host, port, requestBytes, h1Connections / loops, h2Connections / loops, h2Streams));
            }
            System.out.printf("ramp: start=%d, step=%d, max=%d QPS, initialWarmup=%ds, settle=%ds, duration=%ds, body=%d bytes%n",
                    startQps, stepQps, maxQps, initialWarmup, settle, duration, requestBytes);
            System.out.printf("clients: loops=%d, h1=%d (one in-flight), h2=%d (streams/connection=%d)%n",
                    loops, h1Connections / loops * loops, h2Connections / loops * loops, h2Streams);
            if (initialWarmup > 0) {
                // JIT and connection warm-up only: the measurement duration is 0, so no request is counted.
                runStep(clients, cpu, startQps, initialWarmup, 0);
            }
            for (int qps = startQps; qps <= maxQps; qps += stepQps) {
                System.out.printf("ramp: target=%d QPS, h1=%d, h2=%d%n", qps, qps / 2, qps / 2);
                Step step = runStep(clients, cpu, qps, settle, duration);
                RequestStats h1 = RequestStats.merge(step.h1);
                RequestStats h2 = RequestStats.merge(step.h2);
                RequestStats.print(h1, h2);
                System.out.printf(Locale.ROOT, "cpu,%d,%.1f,%.1f,%.1f,%.1f%n", qps, step.window.serverPercent(),
                        step.window.workerPercent(), step.window.clientPercent(), step.window.clientLoopPercent());
                boolean saturated = RequestStats.actualQps(h1, h2) < qps * SATURATION_RATIO || RequestStats.hasErrors(h1, h2);
                if (saturated) {
                    System.out.printf("ramp: saturated at target=%d QPS%n", qps);
                    break;
                }
            }
        } finally {
            for (MixedHttpClient client : clients) {
                client.close();
            }
        }
    }

    private record Step(List<RequestStats> h1, List<RequestStats> h2, CpuUsage.Window window) {
    }

    /**
     * Sends unmeasured for {@code settle} seconds, then measured for {@code duration} seconds, then stops sending and
     * waits until every request of the step completed. Each client's own {@link Pacer} paces on its event loop; the main
     * thread only reads CPU around the measurement window and waits. With {@code duration} 0 there is no measurement
     * window and the window is null.
     */
    private static Step runStep(List<MixedHttpClient> clients, CpuUsage cpu, int qps, int settle, int duration)
            throws Exception {
        int loops = clients.size();
        long start = System.nanoTime();
        long measurementStart = start + TimeUnit.SECONDS.toNanos(settle);
        long measurementEnd = measurementStart + TimeUnit.SECONDS.toNanos(duration);
        int totalRequests = Math.multiplyExact(qps, settle + duration);
        int settleRequests = Math.multiplyExact(qps, settle);
        List<RequestStats> h1 = new ArrayList<>(loops);
        List<RequestStats> h2 = new ArrayList<>(loops);
        CompletableFuture<?>[] done = new CompletableFuture<?>[loops];
        for (int i = 0; i < loops; i++) {
            // Each client takes 1 / loops of the schedule; its stats are only touched on its own event loop and merged afterwards.
            RequestStats loopH1 = new RequestStats("h1", qps / 2, duration, loops);
            RequestStats loopH2 = new RequestStats("h2", qps / 2, duration, loops);
            loopH1.setWindow(measurementStart, measurementEnd);
            loopH2.setWindow(measurementStart, measurementEnd);
            h1.add(loopH1);
            h2.add(loopH2);
            Pacer pacer = new Pacer(clients.get(i), loopH1, loopH2, qps, start, i, loops, totalRequests, settleRequests);
            done[i] = pacer.done;
            clients.get(i).loop().execute(pacer);
        }
        CpuUsage.Window window = null;
        if (duration > 0) {
            waitUntil(measurementStart);
            window = cpu.start();
            waitUntil(measurementEnd);
            window.end();
        }
        try {
            CompletableFuture.allOf(done).get(TimeUnit.NANOSECONDS.toSeconds(measurementEnd - System.nanoTime()) + 10,
                    TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            System.err.println("Drain timeout: requests unfinished at target " + qps);
        }
        return new Step(h1, h2, window);
    }

    /**
     * Sends requests on schedule on one client's event loop: each wake-up sends every request that is due, then
     * schedules the next wake-up. Only requests whose global sequence number satisfies {@code i % loops == index} are
     * sent. Every request carries its scheduled time, so the arrival latency still includes client-side queueing, and the
     * completion callbacks run on the same event loop.
     */
    private static final class Pacer implements Runnable {
        private final MixedHttpClient client;
        private final RequestStats h1;
        private final RequestStats h2;
        private final int qps;
        private final long start;
        private final int loops;
        private final int totalRequests;
        private final int settleRequests;
        private final int ownRequests;
        private final Runnable onCompleted = this::onCompleted;
        final CompletableFuture<Void> done = new CompletableFuture<>();
        private int next;
        private int completed;

        private Pacer(MixedHttpClient client, RequestStats h1, RequestStats h2, int qps, long start,
                      int index, int loops, int totalRequests, int settleRequests) {
            this.client = client;
            this.h1 = h1;
            this.h2 = h2;
            this.qps = qps;
            this.start = start;
            this.loops = loops;
            this.totalRequests = totalRequests;
            this.settleRequests = settleRequests;
            next = index;
            ownRequests = index < totalRequests ? (totalRequests - index + loops - 1) / loops : 0;
            if (ownRequests == 0) {
                done.complete(null);
            }
        }

        private long scheduled(int i) {
            return start + (long) i * 1_000_000_000L / qps;
        }

        @Override
        public void run() {
            long now = System.nanoTime();
            for (; next < totalRequests; next += loops) {
                long scheduled = scheduled(next);
                if (scheduled > now) {
                    break;
                }
                boolean http2 = (next & 1) != 0;
                client.send(http2, http2 ? h2 : h1, scheduled, next >= settleRequests, onCompleted);
            }
            if (next < totalRequests) {
                long delay = Math.max(scheduled(next) - System.nanoTime(), MIN_TICK_NANOS);
                client.loop().schedule(this, delay, TimeUnit.NANOSECONDS);
            }
        }

        private void onCompleted() {
            if (++completed == ownRequests) {
                done.complete(null);
            }
        }
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
