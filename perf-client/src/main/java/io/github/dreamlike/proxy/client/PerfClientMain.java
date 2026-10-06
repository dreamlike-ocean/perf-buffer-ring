package io.github.dreamlike.proxy.client;

public class PerfClientMain {

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && args[0].equals("--help")) {
            System.out.println("PerfClientMain [requestBytes=2048]: open-loop QPS ramp until saturation, H1/H2 50/50");
            System.out.println("Properties: perf.host, perf.port, perf.h1.connections, perf.h2.connections, perf.h2.streams, perf.ramp.*");
            return;
        }
        int requestBytes = args.length > 0 ? Integer.parseInt(args[0]) : 2048;
        if (requestBytes < 14 || requestBytes > 16 * 1024) {
            throw new IllegalArgumentException("requestBytes 14..16384");
        }
        RampTest.run(System.getProperty("perf.host", "127.0.0.1"), Integer.getInteger("perf.port", 4399), requestBytes);
    }
}
