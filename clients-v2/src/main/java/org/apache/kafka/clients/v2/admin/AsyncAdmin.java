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
package org.apache.kafka.clients.v2.admin;

import org.apache.kafka.clients.v2.ClientSettings;
import org.apache.kafka.clients.v2.KafkaClientRuntime;
import org.apache.kafka.clients.v2.MetadataManager;
import org.apache.kafka.clients.v2.NetworkRequestDispatcher;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A minimal v2 admin client: {@code CompletableFuture}-native, no {@code KafkaFuture} or
 * {@code *Result} wrappers. Operations are added incrementally (DESIGN.md phase 7); this
 * phase covers cluster description and topic creation, which is enough to bootstrap
 * end-to-end tests.
 */
public final class AsyncAdmin implements AutoCloseable {

    private final KafkaClientRuntime runtime;
    private final ClientSettings settings;
    private final NetworkRequestDispatcher dispatcher;
    private final MetadataManager metadata;

    public AsyncAdmin(KafkaClientRuntime runtime, ClientSettings settings) {
        this.runtime = runtime;
        this.settings = settings;
        this.dispatcher = new NetworkRequestDispatcher(runtime, settings);
        this.metadata = new MetadataManager(runtime, dispatcher, settings);
    }

    /** @param clusterId may be null on very old brokers */
    public record ClusterDescription(String clusterId, Node controller, List<Node> nodes) { }

    public CompletableFuture<ClusterDescription> describeCluster() {
        return metadata.refresh().thenApply(cluster ->
            new ClusterDescription(cluster.clusterResource().clusterId(), cluster.controller(),
                List.copyOf(cluster.nodes())));
    }

    /**
     * Create a topic and wait for it to appear in metadata (so an immediate produce works).
     */
    public CompletableFuture<Void> createTopic(String name, int numPartitions, short replicationFactor) {
        CreateTopicsRequestData.CreatableTopicCollection topics =
            new CreateTopicsRequestData.CreatableTopicCollection();
        topics.add(new CreateTopicsRequestData.CreatableTopic()
            .setName(name)
            .setNumPartitions(numPartitions)
            .setReplicationFactor(replicationFactor));
        CreateTopicsRequest.Builder request = new CreateTopicsRequest.Builder(
            new CreateTopicsRequestData()
                .setTopics(topics)
                .setTimeoutMs((int) settings.requestTimeout().toMillis()));
        return dispatcher.sendToAnyBroker(request).thenCompose(response -> {
            CreateTopicsResponse createTopicsResponse = (CreateTopicsResponse) response;
            for (var result : createTopicsResponse.data().topics()) {
                Errors error = Errors.forCode(result.errorCode());
                if (error != Errors.NONE)
                    return CompletableFuture.failedFuture(
                        error.exception("Failed to create topic " + result.name()
                            + (result.errorMessage() != null ? ": " + result.errorMessage() : "")));
            }
            // Wait until the new topic's metadata (with leaders) has propagated.
            return metadata.cluster(Set.of(name)).thenApply(cluster -> null);
        });
    }

    @Override
    public void close() {
        dispatcher.close();
    }
}
