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

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Production {@link KafkaConnection}: correlation, in-flight windowing, request timeouts and
 * ApiVersions negotiation over a Netty channel.
 *
 * <p>All mutable state is confined to the channel's event loop; public methods hop onto it.
 * User-visible futures complete on {@link ConnectionSpec#callbackExecutor()} (D10). Responses
 * are matched FIFO with the correlation id verified by
 * {@code AbstractResponse.parseResponse} (D8).
 */
final class NettyKafkaConnection implements KafkaConnection {

    private static final Logger log = LoggerFactory.getLogger(NettyKafkaConnection.class);

    private enum State { NEGOTIATING, READY, CLOSED }

    private final String id;
    private final Channel channel;
    private final ConnectionSpec spec;

    // Event-loop confined.
    private final ArrayDeque<Entry> pending = new ArrayDeque<>();
    private final ArrayDeque<Entry> inFlight = new ArrayDeque<>();
    private int nextCorrelationId = 0;
    private State state = State.NEGOTIATING;

    // Read from arbitrary threads.
    private volatile NodeApiVersions apiVersions;
    private final AtomicInteger inFlightCount = new AtomicInteger();

    /** Completes on the event loop when negotiation finishes; the transport re-dispatches it. */
    private final CompletableFuture<KafkaConnection> readyFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    private static final class Entry {
        final AbstractRequest.Builder<?> builder;
        final CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        final boolean negotiation;
        RequestHeader header;
        ScheduledFuture<?> timeoutTask;

        Entry(AbstractRequest.Builder<?> builder, boolean negotiation) {
            this.builder = builder;
            this.negotiation = negotiation;
        }

        void cancelTimeout() {
            if (timeoutTask != null)
                timeoutTask.cancel(false);
        }
    }

    NettyKafkaConnection(String id, Channel channel, ConnectionSpec spec) {
        this.id = id;
        this.channel = channel;
        this.spec = spec;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public NodeApiVersions apiVersions() {
        return apiVersions;
    }

    @Override
    public boolean isOpen() {
        return state != State.CLOSED && channel.isActive();
    }

    @Override
    public int inFlightCount() {
        return inFlightCount.get();
    }

    CompletableFuture<KafkaConnection> readyFuture() {
        return readyFuture;
    }

    @Override
    public CompletableFuture<Void> closeFuture() {
        return dispatchToCallbackExecutor(closeFuture);
    }

    @Override
    public CompletableFuture<AbstractResponse> send(AbstractRequest.Builder<?> request) {
        Entry entry = new Entry(request, false);
        runOnEventLoop(() -> enqueue(entry));
        return dispatchToCallbackExecutor(entry.future);
    }

    @Override
    public void close() {
        runOnEventLoop(() -> channel.close());
    }

    /** Kick off ApiVersions negotiation; called by the transport once the channel is active. */
    void beginNegotiation() {
        runOnEventLoop(() -> enqueue(new Entry(new ApiVersionsRequest.Builder(), true)));
    }

    // ---------------------------------------------------------------- event-loop internals

    private void runOnEventLoop(Runnable task) {
        if (channel.eventLoop().inEventLoop())
            task.run();
        else
            channel.eventLoop().execute(task);
    }

    private <T> CompletableFuture<T> dispatchToCallbackExecutor(CompletableFuture<T> inner) {
        return inner.whenCompleteAsync((r, e) -> { }, spec.callbackExecutor());
    }

    private void enqueue(Entry entry) {
        if (state == State.CLOSED) {
            entry.future.completeExceptionally(new DisconnectException("Connection to " + id + " is closed"));
            return;
        }
        // The timeout callback always runs on the event loop; the scheduler seam lets the
        // deterministic simulation harness own all timed events (D13).
        Runnable onTimeout = () -> runOnEventLoop(() -> onRequestTimeout(entry));
        entry.timeoutTask = spec.timeoutScheduler() != null
            ? spec.timeoutScheduler().schedule(onTimeout, spec.requestTimeout().toMillis(), TimeUnit.MILLISECONDS)
            : channel.eventLoop().schedule(onTimeout, spec.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
        // Negotiation must precede any user request that may already be queued.
        if (entry.negotiation)
            pending.addFirst(entry);
        else
            pending.addLast(entry);
        writePending();
    }

    /**
     * Write queued requests while the in-flight window has room. With Netty's write queue this
     * lets serialization and flush of request N+1 overlap the flush of request N.
     */
    private void writePending() {
        boolean wroteAny = false;
        while (state != State.CLOSED
            && inFlight.size() < spec.maxInFlight()
            && !pending.isEmpty()
            && (state == State.READY || pending.peekFirst().negotiation)) {
            Entry entry = pending.pollFirst();
            if (writeOne(entry))
                wroteAny = true;
        }
        if (wroteAny)
            channel.flush();
    }

    /** @return true if the request went onto the channel write queue */
    private boolean writeOne(Entry entry) {
        if (entry.future.isDone()) // e.g. already timed out while queued
            return false;
        ApiKeys apiKey = entry.builder.apiKey();
        AbstractRequest request;
        short version;
        try {
            NodeApiVersions versions = this.apiVersions;
            if (versions == null)
                version = entry.builder.latestAllowedVersion();
            else
                version = versions.latestUsableVersion(apiKey, entry.builder.oldestAllowedVersion(),
                    entry.builder.latestAllowedVersion());
            request = entry.builder.build(version);
        } catch (UnsupportedVersionException e) {
            entry.cancelTimeout();
            entry.future.completeExceptionally(e);
            return false;
        }
        RequestHeader header = new RequestHeader(
            new RequestHeaderData()
                .setRequestApiKey(apiKey.id)
                .setRequestApiVersion(version)
                .setClientId(spec.clientId())
                .setCorrelationId(nextCorrelationId++),
            apiKey.requestHeaderVersion(version));
        entry.header = header;

        ByteBuf frame;
        try {
            Send send = request.toSend(header);
            frame = ByteBufSinkChannel.drain(send, channel.alloc());
        } catch (IOException e) {
            entry.cancelTimeout();
            entry.future.completeExceptionally(e);
            return false;
        }

        inFlight.addLast(entry);
        inFlightCount.incrementAndGet();
        channel.write(frame).addListener(f -> {
            if (!f.isSuccess())
                closeExceptionally(new DisconnectException("Failed to write " + apiKey + " request to " + id,
                    asException(f.cause())));
        });
        return true;
    }

    /** Called by {@link ResponseDispatchHandler} with the frame payload (length prefix stripped). */
    void onResponseFrame(ByteBuffer payload) {
        Entry entry = inFlight.pollFirst();
        if (entry == null) {
            closeExceptionally(new IllegalStateException(
                "Received a response from " + id + " with no request in flight"));
            return;
        }
        inFlightCount.decrementAndGet();
        entry.cancelTimeout();

        AbstractResponse response;
        try {
            response = AbstractResponse.parseResponse(payload, entry.header);
        } catch (RuntimeException e) {
            entry.future.completeExceptionally(e);
            closeExceptionally(e);
            return;
        }

        if (entry.negotiation)
            handleApiVersionsResponse(entry, (ApiVersionsResponse) response);
        else
            entry.future.complete(response);
        writePending();
    }

    private void handleApiVersionsResponse(Entry entry, ApiVersionsResponse response) {
        short errorCode = response.data().errorCode();
        if (errorCode == Errors.NONE.code()) {
            this.apiVersions = new NodeApiVersions(
                response.data().apiKeys(),
                response.data().supportedFeatures(),
                response.data().finalizedFeatures(),
                response.data().finalizedFeaturesEpoch());
            state = State.READY;
            entry.future.complete(response);
            log.debug("Connection {} negotiated {} broker APIs", id, response.data().apiKeys().size());
            readyFuture.complete(this);
            writePending();
        } else if (errorCode == Errors.UNSUPPORTED_VERSION.code() && entry.header.apiVersion() > 0) {
            // Brokers reply with the versions they do support (or nothing pre-2.4, in which
            // case fall back to v0) — same downgrade path as the classic NetworkClient.
            short maxApiVersion = 0;
            if (!response.data().apiKeys().isEmpty()) {
                ApiVersionsResponseData.ApiVersion apiVersion =
                    response.data().apiKeys().find(ApiKeys.API_VERSIONS.id);
                if (apiVersion != null)
                    maxApiVersion = apiVersion.maxVersion();
            }
            entry.future.complete(response);
            log.debug("Connection {} retrying ApiVersions negotiation at version {}", id, maxApiVersion);
            enqueue(new Entry(new ApiVersionsRequest.Builder(maxApiVersion), true));
        } else {
            Exception failure = Errors.forCode(errorCode).exception(
                "ApiVersions negotiation with " + id + " failed");
            entry.future.completeExceptionally(failure);
            closeExceptionally(failure);
        }
    }

    private void onRequestTimeout(Entry entry) {
        if (entry.future.isDone())
            return;
        entry.future.completeExceptionally(new TimeoutException(
            "Request " + entry.builder.apiKey() + " to " + id + " timed out after "
                + spec.requestTimeout().toMillis() + " ms"));
        // Correlation can no longer be trusted once a response is abandoned: close, exactly
        // like the classic client disconnects a node on request timeout.
        closeExceptionally(new TimeoutException("Connection to " + id + " closed after request timeout"));
    }

    /** Called by {@link ResponseDispatchHandler} when the channel goes inactive or errors. */
    void onChannelClosed(Throwable cause) {
        if (state == State.CLOSED)
            return;
        state = State.CLOSED;
        Exception failure = cause == null
            ? new DisconnectException("Connection to " + id + " was closed")
            : new DisconnectException("Connection to " + id + " was closed", asException(cause));
        Entry entry;
        while ((entry = inFlight.pollFirst()) != null) {
            inFlightCount.decrementAndGet();
            entry.cancelTimeout();
            entry.future.completeExceptionally(failure);
        }
        while ((entry = pending.pollFirst()) != null) {
            entry.cancelTimeout();
            entry.future.completeExceptionally(failure);
        }
        readyFuture.completeExceptionally(failure);
        closeFuture.complete(null);
    }

    private void closeExceptionally(Throwable cause) {
        if (state == State.CLOSED)
            return;
        log.debug("Closing connection {}", id, cause);
        onChannelClosed(cause);
        channel.close();
    }

    private static Exception asException(Throwable t) {
        return t instanceof Exception e ? e : new RuntimeException(t);
    }
}
