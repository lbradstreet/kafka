/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.network.netty;

import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.security.ssl.SslFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLEngine;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * Netty-backed {@link ClientTransport}.
 *
 * <p>One instance owns (or shares) an {@link EventLoopGroup} and a pooled allocator across all
 * connections; producer, consumer and admin clients built on the same transport share the same
 * event loop threads (D10). The frame decoder mirrors Kafka's 4-byte, length-exclusive prefix;
 * outbound frames come from {@code SendBuilder} with the prefix already included, so bytes on
 * the wire are identical to the classic client's.
 */
public final class NettyClientTransport implements ClientTransport {

    private static final Logger log = LoggerFactory.getLogger(NettyClientTransport.class);
    private static final AttributeKey<NettyKafkaConnection> CONNECTION_KEY =
        AttributeKey.valueOf("kafkaConnection");

    private final EventLoopGroup group;
    private final boolean ownsGroup;
    private final Map<SecuritySpec.Tls, SslFactory> sslFactories = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public NettyClientTransport() {
        this(new NioEventLoopGroup(0, new DefaultThreadFactory("kafka-transport-netty", true)), true);
    }

    public NettyClientTransport(EventLoopGroup group) {
        this(group, false);
    }

    private NettyClientTransport(EventLoopGroup group, boolean ownsGroup) {
        this.group = group;
        this.ownsGroup = ownsGroup;
    }

    @Override
    public CompletableFuture<KafkaConnection> connect(String connectionId,
                                                      InetSocketAddress address,
                                                      ConnectionSpec spec) {
        if (closed)
            return CompletableFuture.failedFuture(new IllegalStateException("Transport is closed"));

        Bootstrap bootstrap = new Bootstrap()
            .group(group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) spec.connectTimeout().toMillis())
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        if (spec.sendBufferBytes() != ConnectionSpec.USE_DEFAULT_BUFFER_SIZE)
            bootstrap.option(ChannelOption.SO_SNDBUF, spec.sendBufferBytes());
        if (spec.receiveBufferBytes() != ConnectionSpec.USE_DEFAULT_BUFFER_SIZE)
            bootstrap.option(ChannelOption.SO_RCVBUF, spec.receiveBufferBytes());

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                if (spec.security() instanceof SecuritySpec.Tls tls) {
                    SSLEngine engine = sslFactoryFor(tls)
                        .createSslEngine(address.getHostString(), address.getPort());
                    ch.pipeline().addLast("ssl", new SslHandler(engine));
                }
                // maxReceiveBytes is a payload limit; the decoder's frameLength includes the
                // 4-byte length prefix, so add 4 to admit a response whose payload is exactly
                // maxReceiveBytes (matching the classic client's prefix-excluded comparison).
                ch.pipeline().addLast("frameDecoder",
                    new LengthFieldBasedFrameDecoder(spec.maxReceiveBytes() + 4, 0, 4, 0, 4));
                NettyKafkaConnection connection = new NettyKafkaConnection(connectionId, ch, spec);
                ch.attr(CONNECTION_KEY).set(connection);
                ch.pipeline().addLast("dispatch", new ResponseDispatchHandler(connection));
            }
        });

        CompletableFuture<KafkaConnection> result = new CompletableFuture<>();
        bootstrap.connect(address).addListener(connectFuture -> {
            if (!connectFuture.isSuccess()) {
                log.debug("Connection {} to {} failed", connectionId, address, connectFuture.cause());
                result.completeExceptionally(connectFuture.cause());
                return;
            }
            Channel channel = ((io.netty.channel.ChannelFuture) connectFuture).channel();
            NettyKafkaConnection connection = channel.attr(CONNECTION_KEY).get();
            // TLS handshake (if any) proceeds concurrently; SslHandler buffers the negotiation
            // request until the handshake completes.
            connection.beginNegotiation();
            connection.readyFuture().whenComplete((conn, error) -> {
                if (error != null)
                    spec.callbackExecutor().execute(() -> result.completeExceptionally(error));
                else
                    spec.callbackExecutor().execute(() -> result.complete(conn));
            });
        });
        return result;
    }

    private SslFactory sslFactoryFor(SecuritySpec.Tls tls) {
        return sslFactories.computeIfAbsent(tls, spec -> {
            SslFactory factory = new SslFactory(ConnectionMode.CLIENT);
            factory.configure(spec.sslConfigs());
            return factory;
        });
    }

    @Override
    public void close() {
        closed = true;
        if (ownsGroup)
            group.shutdownGracefully();
    }
}
