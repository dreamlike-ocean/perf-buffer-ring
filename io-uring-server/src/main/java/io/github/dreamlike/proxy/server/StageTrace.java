package io.github.dreamlike.proxy.server;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Diagnostics only: records how long a request spends in each stage inside the server. Every worker thread prints its
 * own percentiles every 5 seconds and then resets them. Enabled only with -Dperf.trace=true, never in the real runs.
 * {@link #record} is only called on a worker event loop and the samples are kept per thread, so no synchronization is needed.
 */
final class StageTrace {
    static final boolean ENABLED = Boolean.getBoolean("perf.trace");
    private static final String[] STAGES = {"decoded_to_vt", "park", "park_to_loop", "loop_to_written", "total"};
    private static final long PRINT_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final ThreadLocal<PerThread> PER_THREAD = ThreadLocal.withInitial(PerThread::new);

    final long decoded = System.nanoTime();
    volatile long vtStart;
    volatile long parkEnd;
    long loopStart;

    void record(boolean http2, long written) {
        PerThread samples = PER_THREAD.get();
        (http2 ? samples.h2 : samples.h1).add(vtStart - decoded, parkEnd - vtStart, loopStart - parkEnd,
                written - loopStart, written - decoded);
        if (written - samples.lastPrint >= PRINT_INTERVAL_NANOS) {
            samples.lastPrint = written;
            samples.h1.printAndReset();
            samples.h2.printAndReset();
        }
    }

    private static final class PerThread {
        private final String thread = Thread.currentThread().getName();
        private final Samples h1 = new Samples(thread + ",h1");
        private final Samples h2 = new Samples(thread + ",h2");
        private long lastPrint = System.nanoTime();
    }

    private static final class Samples {
        private final String protocol;
        private long[][] values = new long[STAGES.length][1 << 16];
        private int size;

        private Samples(String protocol) {
            this.protocol = protocol;
        }

        void add(long... stageNanos) {
            if (size == values[0].length) {
                for (int i = 0; i < values.length; i++) {
                    values[i] = Arrays.copyOf(values[i], size * 2);
                }
            }
            for (int i = 0; i < stageNanos.length; i++) {
                values[i][size] = stageNanos[i];
            }
            size++;
        }

        void printAndReset() {
            if (size == 0) {
                return;
            }
            StringBuilder line = new StringBuilder("trace,").append(protocol).append(",n=").append(size);
            for (int i = 0; i < STAGES.length; i++) {
                long[] sorted = Arrays.copyOf(values[i], size);
                Arrays.sort(sorted);
                line.append(String.format(Locale.ROOT, ",%s_ms=p50:%.3f/p99:%.3f", STAGES[i],
                        sorted[size / 2] / 1e6, sorted[(int) Math.ceil(size * 0.99) - 1] / 1e6));
            }
            System.out.println(line);
            size = 0;
        }
    }
}
