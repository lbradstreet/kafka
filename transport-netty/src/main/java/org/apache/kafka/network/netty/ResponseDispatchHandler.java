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

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Terminal inbound handler: hands decoded response frames to the connection.
 *
 * <p>The frame is copied out of the pooled buffer before parsing so the pooled memory can be
 * released immediately. Responses that retain their payload (fetch) therefore hold plain heap
 * memory — parity with the classic client, which also allocates a fresh heap buffer per
 * receive. Retained pooled payloads with explicit release (D7) are a later optimization.
 */
final class ResponseDispatchHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final NettyKafkaConnection connection;

    ResponseDispatchHandler(NettyKafkaConnection connection) {
        this.connection = connection;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf frame) {
        ByteBuffer payload = ByteBuffer.allocate(frame.readableBytes());
        frame.readBytes(payload);
        payload.flip();
        connection.onResponseFrame(payload);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        connection.onChannelClosed(null);
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        connection.onChannelClosed(cause);
        ctx.close();
    }
}
