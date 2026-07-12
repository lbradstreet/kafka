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
package org.apache.kafka.clients.v2;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.network.netty.ConnectionSpec;
import org.apache.kafka.network.netty.KafkaConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The v2 request layer: routes requests to per-node connections, establishing them on demand.
 *
 * <p>This replaces the upper half of the classic {@code NetworkClient}: connection lifecycle,
 * node selection and request routing. Correlation, versioning and timeouts live below, in the
 * transport's {@code KafkaConnection}. (Named to avoid a clash with the trunk consumer's
 * {@code RequestManager} interface.)
 */
public final class NetworkRequestDispatcher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(NetworkRequestDispatcher.class);

    private final KafkaClientRuntime runtime;
    private final ClientSettings settings;
    private final ConnectionSpec connectionSpec;
    private final Map<String, CompletableFuture<KafkaConnection>> connections = new ConcurrentHashMap<>();
    private final AtomicInteger bootstrapIndex = new AtomicInteger();

    public NetworkRequestDispatcher(KafkaClientRuntime runtime, ClientSettings settings) {
        this.runtime = runtime;
        this.settings = settings;
        this.connectionSpec = ConnectionSpec.newBuilder(settings.clientId())
            .security(settings.security())
            .requestTimeout(settings.requestTimeout())
            .connectTimeout(settings.connectTimeout())
            .maxInFlight(settings.maxInFlight())
            .callbackExecutor(runtime.callbackExecutor())
            .build();
    }

    /** Send to a specific broker node on the pool's first connection (metadata, coordinators). */
    public CompletableFuture<AbstractResponse> send(Node node, AbstractRequest.Builder<?> request) {
        return send(node, 0, request);
    }

    /**
     * Send to a specific broker over a specific connection of its pool (connection pooling, #4).
     * The producer pins a partition to a connection via {@link #connectionIndex(TopicPartition)}
     * so all requests for that partition share one connection and stay ordered, while different
     * partitions spread across the pool for parallel pipelines.
     */
    public CompletableFuture<AbstractResponse> send(Node node, int connectionIndex,
                                                    AbstractRequest.Builder<?> request) {
        InetSocketAddress address = InetSocketAddress.createUnresolved(node.host(), node.port());
        return connectionTo(connectionId(node, connectionIndex), address)
            .thenCompose(connection -> connection.send(request));
    }

    /** The pool connection index a partition is pinned to (stable, ordering-preserving). */
    public int connectionIndex(TopicPartition partition) {
        int k = settings.connectionsPerBroker();
        return k == 1 ? 0 : Math.floorMod(partition.hashCode(), k);
    }

    private static String connectionId(Node node, int connectionIndex) {
        return node.idString() + "#" + connectionIndex;
    }

    /**
     * Send to any broker: prefers the ready connection with the fewest requests in flight,
     * otherwise bootstraps a new connection (round-robin over the bootstrap list).
     */
    public CompletableFuture<AbstractResponse> sendToAnyBroker(AbstractRequest.Builder<?> request) {
        KafkaConnection leastLoaded = null;
        for (CompletableFuture<KafkaConnection> future : connections.values()) {
            KafkaConnection connection = future.getNow(null);
            if (connection != null && connection.isOpen()
                && (leastLoaded == null || connection.inFlightCount() < leastLoaded.inFlightCount()))
                leastLoaded = connection;
        }
        if (leastLoaded != null)
            return leastLoaded.send(request);

        List<InetSocketAddress> bootstrap = settings.bootstrapServers();
        InetSocketAddress address = bootstrap.get(
            Math.floorMod(bootstrapIndex.getAndIncrement(), bootstrap.size()));
        return connectionTo("bootstrap-" + address, address)
            .thenCompose(connection -> connection.send(request));
    }

    /** @see #isSaturated(Node, int) — the pool's first connection. */
    public boolean isSaturated(Node node) {
        return isSaturated(node, 0);
    }

    /**
     * @return true if the given pool connection exists and cannot accept another request right
     *         now — its in-flight window is full or the broker has throttled it (KIP-219).
     *         Because backpressure is judged per connection, a partition on a full connection
     *         is backpressured while a partition on an idle connection of the same broker is
     *         not — pipeline depth is per-partition, not per-broker (#3). Advisory: races only
     *         shift batching behavior.
     */
    public boolean isSaturated(Node node, int connectionIndex) {
        CompletableFuture<KafkaConnection> future = connections.get(connectionId(node, connectionIndex));
        KafkaConnection connection = future == null ? null : future.getNow(null);
        return connection != null && connection.isOpen() && connection.isSendBlocked();
    }

    private CompletableFuture<KafkaConnection> connectionTo(String id, InetSocketAddress address) {
        return connections.computeIfAbsent(id, connectionId -> {
            log.debug("Opening connection {} to {}", connectionId, address);
            CompletableFuture<KafkaConnection> future =
                runtime.transport().connect(connectionId, resolve(address), connectionSpec);
            future.whenComplete((connection, error) -> {
                if (error != null) {
                    log.info("Connection {} to {} failed", connectionId, address, error);
                    connections.remove(connectionId);
                } else {
                    connection.closeFuture().whenComplete((v, e) -> {
                        log.debug("Connection {} closed", connectionId);
                        connections.remove(connectionId);
                    });
                }
            });
            return future;
        });
    }

    private static InetSocketAddress resolve(InetSocketAddress address) {
        return address.isUnresolved()
            ? new InetSocketAddress(address.getHostString(), address.getPort())
            : address;
    }

    @Override
    public void close() {
        List<CompletableFuture<KafkaConnection>> open = new ArrayList<>(connections.values());
        connections.clear();
        for (CompletableFuture<KafkaConnection> future : open) {
            KafkaConnection connection = future.getNow(null);
            if (connection != null)
                connection.close();
            else
                future.thenAccept(KafkaConnection::close);
        }
    }
}
