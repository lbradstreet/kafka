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

import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NettyKafkaConnectionTest {

    private EmbeddedChannel channel;
    private NettyKafkaConnection connection;

    @BeforeEach
    public void setUp() {
        channel = new EmbeddedChannel();
        ConnectionSpec spec = ConnectionSpec.newBuilder("test-client")
            .requestTimeout(Duration.ofSeconds(5))
            .maxInFlight(2)
            .build();
        connection = new NettyKafkaConnection("node-1", channel, spec);
        channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(64 * 1024 * 1024, 0, 4, 0, 4));
        channel.pipeline().addLast(new ResponseDispatchHandler(connection));
    }

    /** Read one outbound frame, assert its header, and return the header for correlation. */
    private RequestHeader readRequest(ApiKeys expectedApiKey) {
        ByteBuf frame = channel.readOutbound();
        assertNotNull(frame, "expected an outbound frame");
        ByteBuffer wire = ByteBuffer.allocate(frame.readableBytes());
        frame.readBytes(wire);
        frame.release();
        wire.flip();
        int size = wire.getInt();
        assertEquals(wire.remaining(), size, "length prefix must exclude itself and cover the payload");
        RequestHeader header = RequestHeader.parse(wire);
        assertEquals(expectedApiKey, header.apiKey());
        assertEquals("test-client", header.clientId());
        return header;
    }

    private void respond(AbstractResponse response, short version, int correlationId) {
        ByteBuffer payload = RequestTestUtils.serializeResponseWithHeader(response, version, correlationId);
        ByteBuf framed = Unpooled.buffer(4 + payload.remaining());
        framed.writeInt(payload.remaining());
        framed.writeBytes(payload);
        channel.writeInbound(framed);
    }

    private void negotiate() {
        connection.beginNegotiation();
        channel.flushOutbound();
        RequestHeader header = readRequest(ApiKeys.API_VERSIONS);
        respond(TestUtils.defaultApiVersionsResponse(ApiMessageType.ListenerType.BROKER),
            header.apiVersion(), header.correlationId());
    }

    @Test
    public void testNegotiationCompletesReadyFuture() throws Exception {
        assertFalse(connection.readyFuture().isDone());
        negotiate();
        assertTrue(connection.readyFuture().isDone());
        assertNotNull(connection.apiVersions());
        assertTrue(connection.apiVersions().latestUsableVersion(ApiKeys.METADATA) >= 0);
    }

    @Test
    public void testRequestResponseRoundTrip() throws Exception {
        negotiate();
        CompletableFuture<AbstractResponse> future =
            connection.send(new MetadataRequest.Builder(new MetadataRequestData()));
        channel.runPendingTasks();
        RequestHeader header = readRequest(ApiKeys.METADATA);
        assertFalse(future.isDone());
        respond(new MetadataResponse(new MetadataResponseData(), header.apiVersion()),
            header.apiVersion(), header.correlationId());
        assertInstanceOf(MetadataResponse.class, future.get());
    }

    @Test
    public void testPipeliningWindowQueuesExcessRequests() throws Exception {
        negotiate();
        // maxInFlight is 2: the first two requests hit the wire, the third waits locally.
        CompletableFuture<AbstractResponse> f1 = connection.send(metadataRequest());
        CompletableFuture<AbstractResponse> f2 = connection.send(metadataRequest());
        CompletableFuture<AbstractResponse> f3 = connection.send(metadataRequest());
        channel.runPendingTasks();

        RequestHeader h1 = readRequest(ApiKeys.METADATA);
        RequestHeader h2 = readRequest(ApiKeys.METADATA);
        assertNull(channel.readOutbound(), "third request must wait for the in-flight window");
        assertEquals(2, connection.inFlightCount());

        // Responses arrive strictly in order; completing the first opens the window for the third.
        respond(new MetadataResponse(new MetadataResponseData(), h1.apiVersion()), h1.apiVersion(), h1.correlationId());
        assertTrue(f1.isDone());
        RequestHeader h3 = readRequest(ApiKeys.METADATA);
        respond(new MetadataResponse(new MetadataResponseData(), h2.apiVersion()), h2.apiVersion(), h2.correlationId());
        respond(new MetadataResponse(new MetadataResponseData(), h3.apiVersion()), h3.apiVersion(), h3.correlationId());
        assertTrue(f2.isDone());
        assertTrue(f3.isDone());
    }

    @Test
    public void testCorrelationMismatchClosesConnection() {
        negotiate();
        CompletableFuture<AbstractResponse> future = connection.send(metadataRequest());
        channel.runPendingTasks();
        RequestHeader header = readRequest(ApiKeys.METADATA);
        // Respond with the wrong correlation id.
        respond(new MetadataResponse(new MetadataResponseData(), header.apiVersion()),
            header.apiVersion(), header.correlationId() + 42);
        ExecutionException e = assertThrows(ExecutionException.class, future::get);
        assertNotNull(e.getCause());
        assertFalse(channel.isActive(), "connection must close on correlation mismatch");
    }

    @Test
    public void testDisconnectFailsInFlightRequests() {
        negotiate();
        CompletableFuture<AbstractResponse> future = connection.send(metadataRequest());
        channel.runPendingTasks();
        readRequest(ApiKeys.METADATA);
        channel.close();
        ExecutionException e = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(DisconnectException.class, e.getCause());
        assertTrue(connection.closeFuture().isDone());
    }

    @Test
    public void testRequestsBeforeReadyAreQueuedUntilNegotiated() throws Exception {
        CompletableFuture<AbstractResponse> future = connection.send(metadataRequest());
        channel.runPendingTasks();
        assertNull(channel.readOutbound(), "user requests must not precede negotiation");
        negotiate();
        channel.runPendingTasks();
        RequestHeader header = readRequest(ApiKeys.METADATA);
        respond(new MetadataResponse(new MetadataResponseData(), header.apiVersion()),
            header.apiVersion(), header.correlationId());
        assertInstanceOf(MetadataResponse.class, future.get());
    }

    private static MetadataRequest.Builder metadataRequest() {
        return new MetadataRequest.Builder(new MetadataRequestData().setTopics(List.of()));
    }
}
