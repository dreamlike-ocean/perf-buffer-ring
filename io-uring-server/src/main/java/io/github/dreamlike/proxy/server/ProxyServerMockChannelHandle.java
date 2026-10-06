package io.github.dreamlike.proxy.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
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
    // Duration of the simulated blocking call (Redis and the like); run-perf.sh runs 5, 10 and 20 ms via -Dperf.blocking.ms.
    private static final long BLOCKING_NANOS = TimeUnit.MILLISECONDS.toNanos(Long.getLong("perf.blocking.ms", 5));
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
        StageTrace trace = StageTrace.ENABLED ? new StageTrace() : null;
        CompletableFuture.runAsync(() -> {
                    if (trace != null) {
                        trace.vtStart = System.nanoTime();
                    }
                    runBlockingHandle(msg);
                    if (trace != null) {
                        trace.parkEnd = System.nanoTime();
                    }
                }, VT_EXECUTOR)
                .thenRunAsync(() -> {
                    if (trace != null) {
                        trace.loopStart = System.nanoTime();
                    }
                    echo(ctx, msg, trace);
                    boolean crossThread = ThreadLocalRandom.current().nextInt(2) == 0;
                    if (crossThread) {
                        // Release the request on the acceptor thread to exercise the allocator's cross-thread release path.
                        ctx.channel().parent().eventLoop().execute(msg::release);
                    } else {
                        msg.release();
                    }
                }, ctx.channel().eventLoop());
    }

    private void runBlockingHandle(FullHttpRequest msg) {
        // Usually a call to Redis or another blocking service.
        LockSupport.parkNanos(BLOCKING_NANOS);
        msg.headers().add(PROXY_SERVER_NAME, "proxy-server");
    }

    private void echo(ChannelHandlerContext ctx, FullHttpRequest fullHttpMessage, StageTrace trace) {
        // Each response gets its own reader/writer indices over the shared JSON content.
        DefaultFullHttpResponse defaultFullHttpResponse = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, MOCK_RESPONSE.duplicate());
        defaultFullHttpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH, MOCK_RESPONSE.readableBytes());
        defaultFullHttpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
        int streamId = fullHttpMessage.headers().getInt(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), -1);
        if (streamId != -1) {
            defaultFullHttpResponse.headers().add(HttpConversionUtil.ExtensionHeaderNames.STREAM_ID.text(), streamId);
        }
        ChannelFuture written = ctx.channel().writeAndFlush(defaultFullHttpResponse);
        if (trace != null) {
            boolean http2 = streamId != -1;
            written.addListener(future -> trace.record(http2, System.nanoTime()));
        }
    }
}
