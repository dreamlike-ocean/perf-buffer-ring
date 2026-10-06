package io.github.dreamlike.proxy.client;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.LongStream;

final class RequestStats {
    private final String protocol;
    private final int targetQps;
    private final String target;
    private final int duration;
    private final int planned;
    private long[] requestLatency;
    private long[] arrivalLatency;
    private long windowStart;
    private long windowEnd;
    private int offered;
    private int sent;
    private int success;
    private int failed;
    private int poolBusy;
    private int completionsInWindow;

    RequestStats(String protocol, int targetQps, int duration) {
        this.protocol = protocol;
        this.targetQps = targetQps;
        target = Integer.toString(targetQps);
        this.duration = duration;
        planned = Math.multiplyExact(targetQps, duration);
        requestLatency = new long[planned];
        arrivalLatency = new long[requestLatency.length];
    }

    RequestStats(String protocol, String target, int duration) {
        this.protocol = protocol;
        this.target = target;
        this.duration = duration;
        targetQps = 0;
        planned = 0;
        requestLatency = new long[16 * 1024];
        arrivalLatency = new long[requestLatency.length];
    }

    void setWindow(long start, long end) {
        windowStart = start;
        windowEnd = end;
    }

    void offered(boolean measured) {
        if (measured) {
            offered++;
        }
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
        String mixedTarget = h1.targetQps == 0 ? h1.target : Integer.toString(h1.targetQps + h2.targetQps);
        printRow("mixed", mixedTarget,
                (h1.completionsInWindow + h2.completionsInWindow) / (double) h1.duration,
                h1.sent + h2.sent, h1.success + h2.success, h1.failed + h2.failed,
                h1.poolBusy + h2.poolBusy, h1.planned() + h2.planned(), requests, arrivals);
        System.out.println("actual_qps counts successful responses in the measurement window; arrival latency includes client scheduling and queueing.");
    }

    private void printRow() {
        printRow(protocol, target, completionsInWindow / (double) duration, sent, success, failed, poolBusy,
                planned(), Arrays.copyOf(requestLatency, success), Arrays.copyOf(arrivalLatency, success));
    }

    private int planned() {
        return targetQps == 0 ? offered : planned;
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
