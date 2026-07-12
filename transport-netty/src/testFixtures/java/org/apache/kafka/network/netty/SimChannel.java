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

import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;

/**
 * An {@link EmbeddedChannel} whose flushed outbound frames are delivered to the simulated
 * network instead of an outbound queue. The pipeline on top — frame decoder, dispatch
 * handler, {@code NettyKafkaConnection} — is the unmodified production code (D13): the sim
 * replaces only the wire.
 *
 * <p>{@code EmbeddedChannel} executes all channel work inline on the calling thread, which
 * is exactly the determinism the harness needs.
 */
public final class SimChannel extends EmbeddedChannel {

    private Consumer<ByteBuf> outboundSink;

    /** Set after pipeline construction; frames flushed before this are dropped loudly. */
    public void outboundSink(Consumer<ByteBuf> sink) {
        this.outboundSink = sink;
    }

    @Override
    protected void handleOutboundMessage(Object msg) {
        if (outboundSink != null && msg instanceof ByteBuf frame)
            outboundSink.accept(frame);
        else
            super.handleOutboundMessage(msg);
    }
}
