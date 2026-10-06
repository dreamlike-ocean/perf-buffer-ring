package io.github.dreamlike.proxy.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.HttpConversionUtil;
import io.netty.util.AsciiString;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class ProxyServerMockChannelHandle extends SimpleChannelInboundHandler<FullHttpRequest> {
    private static final AsciiString PROXY_SERVER_NAME = AsciiString.cached("proxy-server");
    private static final Executor VT_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();
    private static final ByteBuf MOCK_RESPONSE = Unpooled.unreleasableBuffer(Unpooled.directBuffer(2 * 1024));

    static {
        String prefix = "{\"code\":200,\"message\":\"ok\",\"data\":{\"gateway\":\"proxy-server\",\"payload\":\"";
        String suffix = "\"}}";
        String json = prefix + "x".repeat(2 * 1024 - prefix.length() - suffix.length()) + suffix;
        MOCK_RESPONSE.writeCharSequence(json, StandardCharsets.UTF_8);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
        msg.retain();
        CompletableFuture.runAsync(() -> runBlockingHandle(msg), VT_EXECUTOR)
                .thenRunAsync(() -> {
                    echo(ctx, msg);
                    boolean crossThread = ThreadLocalRandom.current().nextInt(2) == 0;
                    if (crossThread) {
                        // 在 acceptor 线程归还 request，覆盖 allocator 的跨线程回收。
                        ctx.channel().parent().eventLoop().execute(msg::release);
                    } else {
                        msg.release();
                    }
                }, ctx.channel().eventLoop());
    }

    private void runBlockingHandle(FullHttpRequest msg) {
        // 一般是走redis或者其它的阻塞调用
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
        msg.headers().add(PROXY_SERVER_NAME, "proxy-server");
    }

    private void echo(ChannelHandlerContext ctx, FullHttpRequest fullHttpMessage) {
        // 每次发送使用独立的读写索引，共享 JSON 的内容。
        DefaultFullHttpResponse defaultFullHttpResponse = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, MOCK_RESPONSE.duplicate());
        defaultFullHttpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, MOCK_RESPONSE.readableBytes());
        defaultFullHttpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        int streamId = fullHttpMessage.headers().getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), -1);
        if (streamId != -1) {
            defaultFullHttpResponse.headers().add(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);
        }
        ctx.channel().writeAndFlush(defaultFullHttpResponse);
    }
}
