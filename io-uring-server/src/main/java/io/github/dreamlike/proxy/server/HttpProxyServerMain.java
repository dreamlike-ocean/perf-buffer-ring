package io.github.dreamlike.proxy.server;

public class HttpProxyServerMain {

    public static void main(String[] args) {
        String allocator = args.length == 0 ? "adaptive" : args[0];
        boolean useAdaptiveBufferRingAllocator = switch (allocator) {
            case "adaptive" -> true;
            case "recycling" -> false;
            default -> throw new IllegalArgumentException("Usage: HttpProxyServerMain [adaptive|recycling]");
        };
        HttpProxyServer server = new HttpProxyServer(useAdaptiveBufferRingAllocator);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "proxy-shutdown"));
        server.start();
        System.out.printf("Listening on :4399, allocator=%s, workers=%d%n", allocator, HttpProxyServer.WORKERS);
    }
}
