package io.github.dreamlike.proxy.server;

import io.github.dreamlike.proxy.server.channel.HttpProtocolSniffHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringAdaptiveBufferRingAllocator;
import io.netty.channel.uring.IoUringBufferRingAllocator;
import io.netty.channel.uring.IoUringBufferRingConfig;
import io.netty.channel.uring.IoUringChannelOption;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringIoHandlerConfig;
import io.netty.channel.uring.IoUringRecyclingBufferRingAllocator;
import io.netty.channel.uring.IoUringServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;


/**
 *
 -Dio.netty.leakDetection.level=disabled
 -Dio.netty.noUnsafe=false
 --sun-misc-unsafe-memory-access=allow
 -Dio.netty.tryReflectionSetAccessible=true
 --add-opens=java.base/java.nio=ALL-UNNAMED
 --enable-native-access=ALL-UNNAMED
 */
public class HttpProxyServer {

    private static short DEFAULT_BUFFER_GROUP_ID = 1;
    // The client's CpuUsage sums the CPU of the worker event loop threads by this prefix in /proc/<pid>/task/*/comm (comm is at most 15 chars).
    static final String WORKER_THREAD_POOL_NAME = "perf-worker";
    static final int WORKERS = Integer.getInteger("perf.workers", 2);
    private IoUringRecyclingBufferRingAllocator recyclingAllocator;
    private final ServerBootstrap bootstrap;
    private final MultiThreadIoEventLoopGroup acceptorGroup;
    private final MultiThreadIoEventLoopGroup workerGroup;

    public HttpProxyServer(boolean useAdaptiveBufferRingAllocator) {
        acceptorGroup = new MultiThreadIoEventLoopGroup(1, new DefaultThreadFactory("perf-acceptor"),
                IoUringIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(WORKERS, new DefaultThreadFactory(WORKER_THREAD_POOL_NAME),
                ioHandler(useAdaptiveBufferRingAllocator));
        bootstrap = new ServerBootstrap();
        bootstrap.childOption(IoUringChannelOption.IO_URING_BUFFER_GROUP_ID, DEFAULT_BUFFER_GROUP_ID)
                .channel(IoUringServerSocketChannel.class)
                .group(acceptorGroup, workerGroup)
                .childHandler(new HttpProxyChannelInitializer());
    }

    public void start() {
        bootstrap.bind(4399).syncUninterruptibly();
    }

    public void stop() {
        if (recyclingAllocator != null) {
            // run-perf.sh reads this line from the server log into recycling-stats.csv.
            System.out.printf("recycling_stats,fallback=%d,foreign_thread=%d%n",
                    recyclingAllocator.fallbackAllocations(), recyclingAllocator.foreignThreadAllocations());
        }
        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        acceptorGroup.terminationFuture().syncUninterruptibly();
        workerGroup.terminationFuture().syncUninterruptibly();
    }

    protected IoHandlerFactory ioHandler(boolean useAdaptiveBufferRingAllocator) {
        // note: io_uring instance parameters are tuned here.
        // By default the SQ has 4096 entries and the CQ 8192.
        // A connection has at most 2 ops in flight at a time (recv and send), so 4096 is enough.
        // An SQE is usually 64 bytes and a CQE 16 bytes; both live in shared anonymous memory obtained via mmap.
        IoUringIoHandlerConfig ioUringIoHandlerConfiguration = new IoUringIoHandlerConfig();
        ioUringIoHandlerConfiguration.setRingSize(4096);
        // recv_multishot: Linux 6.0+
        // multishot_accept: Linux 5.19+
        // poll_multishot: Linux 5.13+; all of these satisfy the requirement of setupCqSize.
        if (IoUring.isAcceptMultishotEnabled() || IoUring.isRecvMultishotEnabled() || IoUring.isPollAddMultishotEnabled()) {
            // Following community discussions, the CQ should be larger when multishot is enabled.
            ioUringIoHandlerConfiguration.setCqSize(ioUringIoHandlerConfiguration.getRingSize() * 4);
        }
        if (IoUring.isRegisterBufferRingSupported()) {
            IoUringBufferRingConfig ioUringBufferRingConfig = IoUringBufferRingConfig.builder()//
                    .batchAllocation(true)//
                    .allocator(useAdaptiveBufferRingAllocator ? defaultIoUringBufferRingAllocator() : recyclingIoUringBufferRingAllocator())//
                    .bufferGroupId(DEFAULT_BUFFER_GROUP_ID)//
                    .bufferRingSize((short)4096)//
                    .batchSize(2048)//
                    .build();
            ioUringIoHandlerConfiguration.setBufferRingConfig(ioUringBufferRingConfig);
        }
        return IoUringIoHandler.newFactory(ioUringIoHandlerConfiguration);
    }

    private IoUringBufferRingAllocator defaultIoUringBufferRingAllocator() {
        return new IoUringAdaptiveBufferRingAllocator(ByteBufAllocator.DEFAULT, 1024, 1024, 4 * 1024, true);
    }

    private IoUringBufferRingAllocator recyclingIoUringBufferRingAllocator() {
        recyclingAllocator = new IoUringRecyclingBufferRingAllocator((short) 4096, 4 * 1024);
        return recyclingAllocator;
    }


    private static class HttpProxyChannelInitializer extends ChannelInitializer<Channel> {

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(new HttpProtocolSniffHandler(new ProxyServerMockChannelHandle()));
        }
    }
}
