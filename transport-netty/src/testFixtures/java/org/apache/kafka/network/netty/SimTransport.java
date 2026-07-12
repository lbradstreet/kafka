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

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Deterministic in-memory {@link ClientTransport}: builds real {@code NettyKafkaConnection}s
 * over {@link SimChannel}s wired to the {@link SimNetwork} (D13). Broker addresses use the
 * {@code sim-broker-<id>} host convention from {@link SimCluster}.
 */
public final class SimTransport implements ClientTransport {

    private final SimNetwork network;
    private final SimCluster cluster;
    private final SimScheduler scheduler;
    private final SimTrace trace;

    public SimTransport(SimNetwork network, SimCluster cluster, SimScheduler scheduler, SimTrace trace) {
        this.network = network;
        this.cluster = cluster;
        this.scheduler = scheduler;
        this.trace = trace;
    }

    @Override
    public CompletableFuture<KafkaConnection> connect(String connectionId,
                                                      InetSocketAddress address,
                                                      ConnectionSpec spec) {
        CompletableFuture<KafkaConnection> result = new CompletableFuture<>();
        scheduler.schedule(() -> {
            SimBroker broker;
            try {
                broker = network.broker(cluster.brokerIdFor(address.getHostString()));
            } catch (IllegalArgumentException e) {
                result.completeExceptionally(new ConnectException("Unknown sim address " + address));
                return;
            }
            if (broker == null) {
                result.completeExceptionally(new ConnectException("No sim broker at " + address));
                return;
            }
            trace.add("connect " + connectionId + " -> broker-" + broker.id());
            // All timed events (request timeouts included) go through the one sim queue.
            ConnectionSpec simSpec = spec.withTimeoutScheduler(scheduler);
            SimChannel channel = new SimChannel();
            NettyKafkaConnection connection = new NettyKafkaConnection(connectionId, channel, simSpec);
            channel.pipeline().addLast(
                new LengthFieldBasedFrameDecoder(simSpec.maxReceiveBytes(), 0, 4, 0, 4));
            channel.pipeline().addLast(new ResponseDispatchHandler(connection));
            network.register(connectionId, channel, broker);
            connection.beginNegotiation();
            channel.runPendingTasks();
            connection.readyFuture().whenComplete((conn, error) -> {
                if (error != null)
                    result.completeExceptionally(error);
                else
                    result.complete(conn);
            });
        }, SimNetwork.BASE_LATENCY_MS, TimeUnit.MILLISECONDS);
        return result;
    }

    @Override
    public void close() {
        // Channels are owned by their connections; nothing transport-global to release.
    }
}
