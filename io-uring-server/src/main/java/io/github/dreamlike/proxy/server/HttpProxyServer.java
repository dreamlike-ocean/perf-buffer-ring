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
    private final ServerBootstrap bootstrap;
    private final MultiThreadIoEventLoopGroup acceptorGroup;
    private final MultiThreadIoEventLoopGroup workerGroup;

    public HttpProxyServer(boolean useAdaptiveBufferRingAllocator) {
        acceptorGroup = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(1, ioHandler(useAdaptiveBufferRingAllocator));
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
        acceptorGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        acceptorGroup.terminationFuture().syncUninterruptibly();
        workerGroup.terminationFuture().syncUninterruptibly();
    }

    protected IoHandlerFactory ioHandler(boolean useAdaptiveBufferRingAllocator) {
        // note: 对于IoUring的实例调整参数在这里
        // 默认 sqe为4096长 cqe为8192长
        // 对于一个链接同时最多只有2个op（recv and send）在使用 所以4096也够用了
        // 一般场景下 SQE 大小为64B，CQE 大小为16B 对应内存为mmap的得到的共享匿名内存
        IoUringIoHandlerConfig ioUringIoHandlerConfiguration = new IoUringIoHandlerConfig();
        ioUringIoHandlerConfiguration.setRingSize(4096);
        // recv_multishot：Linux 6.0+
        // multishot_accept：Linux 5.19+
        // poll_multishot：Linux 5.13+ 这些都满足setupCqSize的需求
        if (IoUring.isAcceptMultishotEnabled() || IoUring.isRecvMultishotEnabled() || IoUring.isPollAddMultishotEnabled()) {
            // 根据社区讨论 如果开启了multi-shot则最好把cq开大点
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
        return new IoUringRecyclingBufferRingAllocator((short) 4096, 1024);
    }


    private static class HttpProxyChannelInitializer extends ChannelInitializer<Channel> {

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ch.pipeline().addLast(new HttpProtocolSniffHandler(new ProxyServerMockChannelHandle()));
        }
    }
}
