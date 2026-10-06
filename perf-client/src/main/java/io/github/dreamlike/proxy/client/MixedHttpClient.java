package io.github.dreamlike.proxy.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import io.netty.handler.codec.http2.Http2ResetFrame;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.handler.codec.http2.Http2StreamChannelBootstrap;
import io.netty.handler.codec.http2.Http2StreamFrame;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

final class MixedHttpClient implements AutoCloseable {
    private final MultiThreadIoEventLoopGroup group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final EventLoop loop = group.next();
    private final ArrayDeque<Http1Connection> idleHttp1 = new ArrayDeque<>();
    private final List<Channel> channels = new ArrayList<>();
    private final List<Http2Connection> http2 = new ArrayList<>();
    private final String authority;
    private final ByteBuf payload;
    private final int maxStreams;
    private int nextHttp2;

    MixedHttpClient(String host, int port, int requestBytes, int h1Connections, int h2Connections, int maxStreams) throws Exception {
        authority = host + ":" + port;
        this.maxStreams = maxStreams;
        String json = "{\"payload\":\"" + "x".repeat(requestBytes - 14) + "\"}";
        payload = Unpooled.unreleasableBuffer(Unpooled.directBuffer(requestBytes));
        payload.writeCharSequence(json, StandardCharsets.UTF_8);
        Bootstrap bootstrap = new Bootstrap().group(group).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true).option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000);
        try {
            for (int i = 0; i < h1Connections; i++) {
                Http1Connection connection = new Http1Connection();
                Channel channel = bootstrap.clone().handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        connection.channel = ch;
                        ch.pipeline().addLast(new HttpClientCodec(), new HttpObjectAggregator(64 * 1024), connection);
                    }
                }).connect(host, port).syncUninterruptibly().channel();
                channels.add(channel);
                idleHttp1.add(connection);
            }
            for (int i = 0; i < h2Connections; i++) {
                Channel channel = bootstrap.clone().handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ch.pipeline().addLast(Http2FrameCodecBuilder.forClient().gracefulShutdownTimeoutMillis(0).build(),
                                new Http2MultiplexHandler(new ChannelInitializer<Channel>() {
                                    @Override
                                    protected void initChannel(Channel pushedStream) {
                                        pushedStream.close();
                                    }
                                }), new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        System.err.println("H2 connection: " + cause);
                                        ctx.close();
                                    }
                                });
                    }
                }).connect(host, port).syncUninterruptibly().channel();
                channels.add(channel);
                http2.add(new Http2Connection(channel));
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    void send(boolean useHttp2, RequestStats stats, long scheduled, boolean measured, Runnable completed) {
        Request request = new Request(stats, scheduled, measured, completed);
        loop.execute(() -> {
            if (useHttp2) {
                sendHttp2(request);
            } else {
                sendHttp1(request);
            }
        });
    }

    void sendPair(RequestStats h1, RequestStats h2, long scheduled, boolean measured, Runnable completed) {
        loop.execute(() -> {
            PairCompletion pair = new PairCompletion(completed);
            sendHttp1(new Request(h1, scheduled, measured, pair));
            sendHttp2(new Request(h2, scheduled, measured, pair));
        });
    }

    private void sendHttp1(Request request) {
        Http1Connection connection = idleHttp1.poll();
        if (connection == null) {
            request.reject();
            return;
        }
        connection.pending = request;
        request.releaseSlot = () -> {
            connection.pending = null;
            if (connection.channel.isActive()) {
                idleHttp1.add(connection);
            }
        };
        DefaultFullHttpRequest message = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", payload.duplicate());
        message.headers().set(HttpHeaderNames.HOST, authority)
                .set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                .setInt(HttpHeaderNames.CONTENT_LENGTH, payload.readableBytes());
        request.sent();
        connection.channel.writeAndFlush(message).addListener(future -> {
            if (!future.isSuccess()) {
                connection.channel.close();
                request.finish(false);
            }
        });
    }

    private void sendHttp2(Request request) {
        Http2Connection connection = null;
        for (int i = 0; i < http2.size(); i++) {
            Http2Connection candidate = http2.get(nextHttp2++ % http2.size());
            if (candidate.channel.isActive() && candidate.inFlight < maxStreams) {
                connection = candidate;
                break;
            }
        }
        if (connection == null) {
            request.reject();
            return;
        }
        Http2Connection selected = connection;
        selected.inFlight++;
        request.releaseSlot = () -> selected.inFlight--;
        new Http2StreamChannelBootstrap(selected.channel).handler(new Http2ResponseHandler(request)).open().addListener(future -> {
            if (!future.isSuccess()) {
                request.finish(false);
                return;
            }
            Http2StreamChannel stream = (Http2StreamChannel) future.getNow();
            DefaultHttp2Headers headers = new DefaultHttp2Headers();
            headers.method("POST").scheme("http").authority(authority).path("/");
            headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                    .setInt(HttpHeaderNames.CONTENT_LENGTH, payload.readableBytes());
            request.sent();
            stream.write(new DefaultHttp2HeadersFrame(headers, false));
            stream.writeAndFlush(new DefaultHttp2DataFrame(payload.duplicate(), true)).addListener(write -> {
                if (!write.isSuccess()) {
                    request.finish(false);
                    stream.close();
                }
            });
        });
    }

    @Override
    public void close() {
        for (Channel channel : channels) {
            channel.close().syncUninterruptibly();
        }
        group.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
    }

    private final class Http1Connection extends SimpleChannelInboundHandler<FullHttpResponse> {
        private Channel channel;
        private Request pending;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) {
            if (pending != null) {
                pending.finish(response.status().code() == 200 && response.content().readableBytes() == 2048);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (pending != null) {
                pending.finish(false);
            }
            idleHttp1.remove(this);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("H1 connection: " + cause);
            ctx.close();
        }
    }

    private static final class Http2Connection {
        private final Channel channel;
        private int inFlight;

        private Http2Connection(Channel channel) {
            this.channel = channel;
        }
    }

    private static final class Http2ResponseHandler extends SimpleChannelInboundHandler<Http2StreamFrame> {
        private final Request request;
        private int status;
        private int bytes;

        private Http2ResponseHandler(Request request) {
            this.request = request;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Http2StreamFrame frame) {
            boolean endStream = false;
            if (frame instanceof Http2HeadersFrame headers) {
                if (headers.headers().status() != null) {
                    status = Integer.parseInt(headers.headers().status().toString());
                }
                endStream = headers.isEndStream();
            } else if (frame instanceof Http2DataFrame data) {
                bytes += data.content().readableBytes();
                endStream = data.isEndStream();
            } else if (frame instanceof Http2ResetFrame) {
                request.finish(false);
                ctx.close();
            }
            if (endStream) {
                request.finish(status == 200 && bytes == 2048);
                ctx.close();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            request.finish(false);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.err.println("H2 stream: " + cause);
            request.finish(false);
            ctx.close();
        }
    }

    private static final class Request {
        private final RequestStats stats;
        private final long scheduled;
        private final boolean measured;
        private final Runnable completed;
        private Runnable releaseSlot;
        private long sentAt;
        private boolean finished;

        private Request(RequestStats stats, long scheduled, boolean measured, Runnable completed) {
            this.stats = stats;
            this.scheduled = scheduled;
            this.measured = measured;
            this.completed = completed;
            stats.offered(measured);
        }

        private void sent() {
            sentAt = System.nanoTime();
            stats.sent(measured);
        }

        private void finish(boolean success) {
            if (finished) {
                return;
            }
            finished = true;
            releaseSlot.run();
            stats.completed(measured, scheduled, sentAt, success);
            completed.run();
        }

        private void reject() {
            finished = true;
            stats.rejected(measured);
            completed.run();
        }
    }

    private static final class PairCompletion implements Runnable {
        private final Runnable completed;
        private int remaining = 2;

        private PairCompletion(Runnable completed) {
            this.completed = completed;
        }

        @Override
        public void run() {
            if (--remaining == 0) {
                completed.run();
            }
        }
    }
}
