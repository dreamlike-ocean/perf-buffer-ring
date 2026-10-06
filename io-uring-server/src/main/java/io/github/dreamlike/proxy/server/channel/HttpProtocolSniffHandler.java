package io.github.dreamlike.proxy.server.channel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http2.DefaultHttp2Connection;
import io.netty.handler.codec.http2.DefaultHttp2ConnectionDecoder;
import io.netty.handler.codec.http2.DefaultHttp2ConnectionEncoder;
import io.netty.handler.codec.http2.DefaultHttp2FrameReader;
import io.netty.handler.codec.http2.DefaultHttp2FrameWriter;
import io.netty.handler.codec.http2.Http2CodecUtil;
import io.netty.handler.codec.http2.Http2ConnectionEncoder;
import io.netty.handler.codec.http2.Http2FrameReader;
import io.netty.handler.codec.http2.Http2FrameWriter;
import io.netty.handler.codec.http2.Http2HeadersEncoder;
import io.netty.handler.codec.http2.Http2PromisedRequestVerifier;
import io.netty.handler.codec.http2.Http2Settings;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandler;
import io.netty.handler.codec.http2.HttpToHttp2ConnectionHandlerBuilder;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapter;
import io.netty.handler.codec.http2.InboundHttp2ToHttpAdapterBuilder;
import io.netty.util.ReferenceCountUtil;

public class HttpProtocolSniffHandler extends ChannelInboundHandlerAdapter {
    private static final ByteBuf HTTP2_PREFACE_MAGIC_NUMBER = Http2CodecUtil.connectionPrefaceBuf();
    private CompositeByteBuf compositeByteBuf;
    private final ChannelInboundHandler businessHandler;

    public HttpProtocolSniffHandler(ChannelInboundHandler businessHandler) {
        this.businessHandler = businessHandler;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ByteBuf request)) {
            super.channelRead(ctx, msg);
            return;
        }
        boolean first = compositeByteBuf == null;
        int http2MagicNumberLength = HTTP2_PREFACE_MAGIC_NUMBER.readableBytes();
        if (first && request.readableBytes() >= http2MagicNumberLength) {
            boolean isHttp2 = ByteBufUtil.equals(HTTP2_PREFACE_MAGIC_NUMBER, request.slice(0, http2MagicNumberLength));
            determiningProtocol(isHttp2, ctx, request);
            return;
        }

        if (first) {
            compositeByteBuf = ctx.alloc().compositeBuffer();
        }
        compositeByteBuf.addComponents(true, request);
        if (compositeByteBuf.readableBytes() >= http2MagicNumberLength) {
            boolean isHttp2 = ByteBufUtil.equals(HTTP2_PREFACE_MAGIC_NUMBER, compositeByteBuf.slice(0, http2MagicNumberLength));
            CompositeByteBuf byteBufHolder = compositeByteBuf;
            compositeByteBuf = null;
            determiningProtocol(isHttp2, ctx, byteBufHolder);
        }
    }

    private void determiningProtocol(boolean isHttp2, ChannelHandlerContext ctx, ByteBuf firstRequest) throws Exception {
        ChannelPipeline pipeline = ctx.pipeline();
        // After replacing the handlers, pass the buffered data to the new pipeline unchanged.
        if (isHttp2) {
            configureHttp2(pipeline);
        } else {
            configureHttp1(pipeline);
        }
        ctx.fireChannelRead(firstRequest);
        pipeline.remove(this);
    }

    private void configureHttp1(ChannelPipeline pipeline) {
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("aggregator", new HttpObjectAggregator(16 * 1024));
        pipeline.addLast("business", businessHandler);
    }

    private void configureHttp2(ChannelPipeline pipeline) {
        DefaultHttp2Connection clientToProxyHttp2Connection = new DefaultHttp2Connection(true);
        // The H2 adapter copies DATA into the aggregated content, so the business code holds the copy while it waits.
        // The H1 aggregator's content references buffers derived from the ring; each mirrors how its protocol is handled.
        InboundHttp2ToHttpAdapter listener = new InboundHttp2ToHttpAdapterBuilder(clientToProxyHttp2Connection).propagateSettings(false)
                .validateHttpHeaders(false).maxContentLength(16 * 1024).build();
        Http2Settings http2Settings =
                Http2Settings.defaultSettings().maxFrameSize(16 * 1024);
        HttpToHttp2ConnectionHandlerBuilder connectionHandlerBuilder =
                new HttpToHttp2ConnectionHandlerBuilder().gracefulShutdownTimeoutMillis(10 * 1000).initialSettings(http2Settings);
        Http2FrameReader http2FrameReader =
                new DefaultHttp2FrameReader();
        Http2FrameWriter http2FrameWriter = new DefaultHttp2FrameWriter(Http2HeadersEncoder.ALWAYS_SENSITIVE);

        Http2ConnectionEncoder encoder = new DefaultHttp2ConnectionEncoder(clientToProxyHttp2Connection, http2FrameWriter);
        DefaultHttp2ConnectionDecoder decoder = new DefaultHttp2ConnectionDecoder(clientToProxyHttp2Connection, encoder, http2FrameReader,
                Http2PromisedRequestVerifier.ALWAYS_VERIFY, true, true, false);
        connectionHandlerBuilder.codec(decoder, encoder);
        connectionHandlerBuilder.frameListener(listener);
        HttpToHttp2ConnectionHandler connectionHandler = connectionHandlerBuilder.build();
        pipeline.addLast("httpToHttp2", connectionHandler);
        pipeline.addLast("business", businessHandler);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        CompositeByteBuf localBytebuf = compositeByteBuf;
        if (localBytebuf != null) {
            ReferenceCountUtil.safeRelease(localBytebuf);
        }
    }
}
