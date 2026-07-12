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
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.concurrent.CompletableFuture;

/**
 * A single negotiated connection to a broker.
 *
 * <p>Requests are correlated to responses positionally (FIFO per connection) with the
 * correlation id verified, matching classic {@code NetworkClient} semantics (D8). Up to
 * {@link ConnectionSpec#maxInFlight()} requests await responses concurrently; further requests
 * queue locally and are written eagerly as the window opens, so serialization and flush of
 * request N+1 overlap the flush of request N (the pipelining improvement).
 *
 * <p>The request version is chosen via the negotiated {@link NodeApiVersions} intersected with
 * the builder's allowed range, and the header version via
 * {@code ApiKeys.requestHeaderVersion(version)} — identical to the classic client.
 */
public interface KafkaConnection {

    /** @return the transport-scoped connection id supplied to {@link ClientTransport#connect} */
    String id();

    /**
     * Send a request; the future completes with the parsed response on the spec's callback
     * executor. It fails with {@code TimeoutException} if the request timeout elapses (the
     * connection is then closed, since correlation can no longer be trusted), or with
     * {@code DisconnectException} if the connection drops first.
     */
    CompletableFuture<AbstractResponse> send(AbstractRequest.Builder<?> request);

    /** @return the ApiVersions negotiated when this connection was established */
    NodeApiVersions apiVersions();

    /** @return true while the connection is open and usable */
    boolean isOpen();

    /** @return the number of requests written and awaiting a response */
    int inFlightCount();

    /** Completes (on the callback executor) when the connection is fully closed. */
    CompletableFuture<Void> closeFuture();

    /** Begin an orderly close: in-flight requests fail with {@code DisconnectException}. */
    void close();
}
