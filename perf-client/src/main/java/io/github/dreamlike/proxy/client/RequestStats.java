package io.github.dreamlike.proxy.client;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.LongStream;

final class RequestStats {
    private final String protocol;
    private final int targetQps;
    private final int duration;
    private final int planned;
    private long[] requestLatency;
    private long[] arrivalLatency;
    private long windowStart;
    private long windowEnd;
    private int sent;
    private int success;
    private int failed;
    private int poolBusy;
    private int completionsInWindow;

    /**
     * {@code targetQps} and {@code duration} are the targets of the whole protocol. When the load is split across
     * {@code shares} clients, each client creates its own instance with latency arrays pre-sized to 1 / shares, and
     * the instances are combined with {@link #merge} afterwards.
     */
    RequestStats(String protocol, int targetQps, int duration, int shares) {
        this(protocol, targetQps, duration, (long) Math.multiplyExact(targetQps, duration) / shares + 16);
    }

    private RequestStats(String protocol, int targetQps, int duration, long latencyCapacity) {
        this.protocol = protocol;
        this.targetQps = targetQps;
        this.duration = duration;
        planned = Math.multiplyExact(targetQps, duration);
        requestLatency = new long[Math.toIntExact(latencyCapacity)];
        arrivalLatency = new long[requestLatency.length];
    }

    static RequestStats merge(List<RequestStats> parts) {
        RequestStats first = parts.get(0);
        int success = parts.stream().mapToInt(part -> part.success).sum();
        RequestStats merged = new RequestStats(first.protocol, first.targetQps, first.duration, (long) success);
        for (RequestStats part : parts) {
            System.arraycopy(part.requestLatency, 0, merged.requestLatency, merged.success, part.success);
            System.arraycopy(part.arrivalLatency, 0, merged.arrivalLatency, merged.success, part.success);
            merged.success += part.success;
            merged.sent += part.sent;
            merged.failed += part.failed;
            merged.poolBusy += part.poolBusy;
            merged.completionsInWindow += part.completionsInWindow;
        }
        return merged;
    }

    void setWindow(long start, long end) {
        windowStart = start;
        windowEnd = end;
    }

    void sent(boolean measured) {
        if (measured) {
            sent++;
        }
    }

    void rejected(boolean measured) {
        if (measured) {
            poolBusy++;
        }
    }

    void completed(boolean measured, long scheduled, long sentAt, boolean succeeded) {
        long now = System.nanoTime();
        if (succeeded && now >= windowStart && now < windowEnd) {
            completionsInWindow++;
        }
        if (!measured) {
            return;
        }
        if (succeeded) {
            if (success == requestLatency.length) {
                requestLatency = Arrays.copyOf(requestLatency, Math.multiplyExact(requestLatency.length, 2));
                arrivalLatency = Arrays.copyOf(arrivalLatency, requestLatency.length);
            }
            requestLatency[success] = now - sentAt;
            arrivalLatency[success] = now - scheduled;
            success++;
        } else {
            failed++;
        }
    }

    static void print(RequestStats h1, RequestStats h2) {
        System.out.println("protocol,target_qps,actual_qps,sent,success,failed,pool_busy,unfinished,p50_ms,p95_ms,p99_ms,arrival_p99_ms");
        h1.printRow();
        h2.printRow();
        long[] requests = LongStream.concat(Arrays.stream(h1.requestLatency, 0, h1.success),
                Arrays.stream(h2.requestLatency, 0, h2.success)).toArray();
        long[] arrivals = LongStream.concat(Arrays.stream(h1.arrivalLatency, 0, h1.success),
                Arrays.stream(h2.arrivalLatency, 0, h2.success)).toArray();
        printRow("mixed", Integer.toString(h1.targetQps + h2.targetQps),
                (h1.completionsInWindow + h2.completionsInWindow) / (double) h1.duration,
                h1.sent + h2.sent, h1.success + h2.success, h1.failed + h2.failed,
                h1.poolBusy + h2.poolBusy, h1.planned + h2.planned, requests, arrivals);
        System.out.println("actual_qps counts successful responses in the measurement window; arrival latency includes client scheduling and queueing.");
    }

    static double actualQps(RequestStats h1, RequestStats h2) {
        return (h1.completionsInWindow + h2.completionsInWindow) / (double) h1.duration;
    }

    static boolean hasErrors(RequestStats h1, RequestStats h2) {
        return h1.hasErrors() || h2.hasErrors();
    }

    private boolean hasErrors() {
        return failed > 0 || poolBusy > 0 || planned - success - failed - poolBusy > 0;
    }

    private void printRow() {
        printRow(protocol, Integer.toString(targetQps), completionsInWindow / (double) duration, sent, success, failed, poolBusy,
                planned, Arrays.copyOf(requestLatency, success), Arrays.copyOf(arrivalLatency, success));
    }

    private static void printRow(String protocol, String target, double actual, int sent, int success,
                                 int failed, int busy, int planned, long[] requests, long[] arrivals) {
        Arrays.sort(requests);
        Arrays.sort(arrivals);
        System.out.printf(Locale.ROOT, "%s,%s,%.2f,%d,%d,%d,%d,%d,%.3f,%.3f,%.3f,%.3f%n",
                protocol, target, actual, sent, success, failed, busy, planned - success - failed - busy,
                percentile(requests, .50), percentile(requests, .95), percentile(requests, .99),
                percentile(arrivals, .99));
    }

    private static double percentile(long[] sorted, double fraction) {
        return sorted.length == 0 ? Double.NaN : sorted[(int) Math.ceil(sorted.length * fraction) - 1] / 1_000_000.0;
    }
}
