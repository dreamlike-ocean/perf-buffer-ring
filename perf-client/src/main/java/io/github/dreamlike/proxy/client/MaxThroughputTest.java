package io.github.dreamlike.proxy.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

final class MaxThroughputTest {
    private static final int[] CONCURRENT_PAIRS = {64, 128, 256, 512, 1024, 2048};

    static void run(String host, int port, int duration, int warmup, int requestBytes) throws Exception {
        int maxPairs = CONCURRENT_PAIRS[CONCURRENT_PAIRS.length - 1];
        // 每条 H2 连接最多 64 个 stream，低于 server 默认的 100，避免连接限制影响吞吐。
        try (MixedHttpClient client = new MixedHttpClient(host, port, requestBytes, maxPairs, maxPairs / 64, 64)) {
            for (int pairs : CONCURRENT_PAIRS) {
                String target = "max-" + pairs;
                RequestStats h1 = new RequestStats("h1", target, duration);
                RequestStats h2 = new RequestStats("h2", target, duration);
                long start = System.nanoTime();
                long measurementStart = start + TimeUnit.SECONDS.toNanos(warmup);
                long measurementEnd = measurementStart + TimeUnit.SECONDS.toNanos(duration);
                h1.setWindow(measurementStart, measurementEnd);
                h2.setWindow(measurementStart, measurementEnd);
                CountDownLatch drained = new CountDownLatch(pairs);
                PairLoad load = new PairLoad(client, h1, h2, measurementStart, measurementEnd, drained);
                System.out.printf("max: pairs=%d, h1/h2=50/50, warmup=%ds, duration=%ds, body=%d bytes%n",
                        pairs, warmup, duration, requestBytes);
                for (int i = 0; i < pairs; i++) {
                    load.run();
                }
                if (!drained.await((long) warmup + duration + 10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Max throughput drain timeout: " + drained.getCount() + " pairs unfinished");
                }
                RequestStats.print(h1, h2);
            }
        }
    }

    private static final class PairLoad implements Runnable {
        private final MixedHttpClient client;
        private final RequestStats h1;
        private final RequestStats h2;
        private final long measurementStart;
        private final long measurementEnd;
        private final CountDownLatch drained;

        private PairLoad(MixedHttpClient client, RequestStats h1, RequestStats h2,
                         long measurementStart, long measurementEnd, CountDownLatch drained) {
            this.client = client;
            this.h1 = h1;
            this.h2 = h2;
            this.measurementStart = measurementStart;
            this.measurementEnd = measurementEnd;
            this.drained = drained;
        }

        @Override
        public void run() {
            long now = System.nanoTime();
            if (now >= measurementEnd) {
                drained.countDown();
                return;
            }
            // 每组 H1/H2 都完成后再补一组，避免两种协议的完成速度改变发压比例。
            client.sendPair(h1, h2, now, now >= measurementStart, this);
        }
    }
}
