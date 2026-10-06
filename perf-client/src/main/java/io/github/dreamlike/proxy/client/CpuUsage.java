package io.github.dreamlike.proxy.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * Reads utime + stime from /proc before and after the measurement window to compute the CPU usage of the server
 * process, all server worker event loop threads combined, the client process and all client event loop threads
 * combined. 100% is one core. run-perf.sh passes perf.server.pid and perf.clk.tck; without them the server columns are NaN.
 */
final class CpuUsage {
    // Matches HttpProxyServer.WORKER_THREAD_POOL_NAME.
    private static final String SERVER_WORKER_PREFIX = "perf-worker-";
    private static final String CLIENT_LOOP_PREFIX = MixedHttpClient.LOOP_THREAD_POOL_NAME + "-";

    private final Path serverProc;
    private final Path clientProc = Path.of("/proc/self");
    private final long clockTicks;

    private CpuUsage(Path serverProc, long clockTicks) {
        this.serverProc = serverProc;
        this.clockTicks = clockTicks;
    }

    static CpuUsage fromSystemProperties() {
        String pid = System.getProperty("perf.server.pid");
        return new CpuUsage(pid == null ? null : Path.of("/proc", pid), Long.getLong("perf.clk.tck", 100));
    }

    Window start() {
        return new Window();
    }

    /**
     * Threads found by name when the measurement window starts; the same threads are read again when it ends.
     */
    private static List<Path> threads(Path proc, String prefix) {
        if (proc == null) {
            return List.of();
        }
        try (Stream<Path> tasks = Files.list(proc.resolve("task"))) {
            return tasks.filter(task -> {
                try {
                    return Files.readString(task.resolve("comm")).startsWith(prefix);
                } catch (IOException e) {
                    return false;
                }
            }).map(task -> task.resolve("stat")).toList();
        } catch (IOException e) {
            return List.of();
        }
    }

    private static long ticks(Path stat) {
        if (stat == null) {
            return -1;
        }
        try {
            String line = Files.readString(stat);
            // comm may contain spaces, so count from after the last ')': field 0 is state, utime and stime are fields 11 and 12.
            String[] fields = line.substring(line.lastIndexOf(')') + 2).split(" ");
            return Long.parseLong(fields[11]) + Long.parseLong(fields[12]);
        } catch (IOException | RuntimeException e) {
            return -1;
        }
    }

    private static long ticks(List<Path> stats) {
        if (stats.isEmpty()) {
            return -1;
        }
        long sum = 0;
        for (Path stat : stats) {
            long ticks = ticks(stat);
            if (ticks < 0) {
                return -1;
            }
            sum += ticks;
        }
        return sum;
    }

    final class Window {
        private final List<Path> workers = threads(serverProc, SERVER_WORKER_PREFIX);
        private final List<Path> clientLoops = threads(clientProc, CLIENT_LOOP_PREFIX);
        private final long startNanos = System.nanoTime();
        private final long serverStart = ticks(serverProc == null ? null : serverProc.resolve("stat"));
        private final long workerStart = ticks(workers);
        private final long clientStart = ticks(clientProc.resolve("stat"));
        private final long clientLoopStart = ticks(clientLoops);
        private long endNanos;
        private long serverEnd;
        private long workerEnd;
        private long clientEnd;
        private long clientLoopEnd;

        void end() {
            endNanos = System.nanoTime();
            serverEnd = ticks(serverProc == null ? null : serverProc.resolve("stat"));
            workerEnd = ticks(workers);
            clientEnd = ticks(clientProc.resolve("stat"));
            clientLoopEnd = ticks(clientLoops);
        }

        double serverPercent() {
            return percent(serverStart, serverEnd);
        }

        double workerPercent() {
            return percent(workerStart, workerEnd);
        }

        double clientPercent() {
            return percent(clientStart, clientEnd);
        }

        double clientLoopPercent() {
            return percent(clientLoopStart, clientLoopEnd);
        }

        private double percent(long start, long end) {
            if (start < 0 || end < 0 || endNanos == 0) {
                return Double.NaN;
            }
            double seconds = (endNanos - startNanos) / 1e9;
            return (end - start) * 100.0 / clockTicks / seconds;
        }
    }
}
