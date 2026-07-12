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
package org.apache.kafka.clients.v2.consumer;

import org.apache.kafka.clients.v2.KafkaClientRuntime;
import org.apache.kafka.clients.v2.MetadataManager;
import org.apache.kafka.clients.v2.NetworkRequestDispatcher;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Assignment-based v2 consumer: async fetch from partition leaders with explicit or
 * earliest-reset positions.
 *
 * <p>Fetch requests are capped at version 12 (topic-name addressing, sessionless full fetches)
 * — incremental fetch sessions and topic-id addressing are the phase-6 follow-up along with
 * group membership. Decompression happens during record iteration, as in the classic client.
 */
public final class DefaultAsyncConsumer<K, V> implements AsyncConsumer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DefaultAsyncConsumer.class);
    private static final short MAX_FETCH_VERSION = 12;

    private final KafkaClientRuntime runtime;
    private final ConsumerSettings settings;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    private final NetworkRequestDispatcher dispatcher;
    private final MetadataManager metadata;

    private final Set<TopicPartition> assigned = new CopyOnWriteArraySet<>();
    private final ConcurrentMap<TopicPartition, Long> positions = new ConcurrentHashMap<>();

    public DefaultAsyncConsumer(KafkaClientRuntime runtime, ConsumerSettings settings,
                                Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        this.runtime = runtime;
        this.settings = settings;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.dispatcher = new NetworkRequestDispatcher(runtime, settings.client());
        this.metadata = new MetadataManager(runtime, dispatcher, settings.client());
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        assigned.retainAll(partitions);
        assigned.addAll(partitions);
        positions.keySet().retainAll(new HashSet<>(partitions));
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        positions.put(partition, offset);
    }

    @Override
    public CompletableFuture<Void> seekToBeginning(Collection<TopicPartition> partitions) {
        return listOffsets(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP)
            .thenAccept(positions::putAll);
    }

    @Override
    public CompletableFuture<Void> seekToEnd(Collection<TopicPartition> partitions) {
        return listOffsets(partitions, ListOffsetsRequest.LATEST_TIMESTAMP)
            .thenAccept(positions::putAll);
    }

    @Override
    public Long position(TopicPartition partition) {
        return positions.get(partition);
    }

    @Override
    public CompletableFuture<List<ConsumerRecordV2<K, V>>> poll() {
        if (assigned.isEmpty())
            return CompletableFuture.completedFuture(List.of());
        List<TopicPartition> unpositioned = assigned.stream()
            .filter(tp -> !positions.containsKey(tp))
            .toList();
        CompletableFuture<Void> ready = unpositioned.isEmpty()
            ? CompletableFuture.completedFuture(null)
            : seekToBeginning(unpositioned); // earliest reset, like auto.offset.reset=earliest
        return ready.thenCompose(v -> fetchAll());
    }

    @Override
    public void close() {
        dispatcher.close();
    }

    // ---------------------------------------------------------------- internals

    private CompletableFuture<List<ConsumerRecordV2<K, V>>> fetchAll() {
        Set<String> topics = new HashSet<>();
        for (TopicPartition tp : assigned)
            topics.add(tp.topic());
        return metadata.cluster(topics).thenCompose(cluster -> {
            Map<Node, Map<TopicPartition, FetchRequest.PartitionData>> byNode = new HashMap<>();
            for (TopicPartition tp : assigned) {
                Long position = positions.get(tp);
                Node leader = cluster.leaderFor(tp);
                if (position == null || leader == null || leader.isEmpty()) {
                    metadata.refresh();
                    continue;
                }
                byNode.computeIfAbsent(leader, n -> new HashMap<>()).put(tp,
                    new FetchRequest.PartitionData(cluster.topicId(tp.topic()), position,
                        FetchRequest.INVALID_LOG_START_OFFSET,
                        settings.maxPartitionFetchBytes(), Optional.empty()));
            }
            List<CompletableFuture<List<ConsumerRecordV2<K, V>>>> fetches = new ArrayList<>();
            for (Map.Entry<Node, Map<TopicPartition, FetchRequest.PartitionData>> entry : byNode.entrySet()) {
                FetchRequest.Builder request = FetchRequest.Builder.forConsumer(
                    MAX_FETCH_VERSION,
                    (int) settings.fetchMaxWait().toMillis(),
                    settings.fetchMinBytes(),
                    entry.getValue());
                fetches.add(dispatcher.send(entry.getKey(), request)
                    .thenApply(response -> collectRecords((FetchResponse) response)));
            }
            return CompletableFuture.allOf(fetches.toArray(new CompletableFuture<?>[0]))
                .thenApply(nothing -> {
                    List<ConsumerRecordV2<K, V>> all = new ArrayList<>();
                    for (CompletableFuture<List<ConsumerRecordV2<K, V>>> fetch : fetches)
                        all.addAll(fetch.join());
                    return all;
                });
        });
    }

    private List<ConsumerRecordV2<K, V>> collectRecords(FetchResponse response) {
        List<ConsumerRecordV2<K, V>> collected = new ArrayList<>();
        response.responseData(Map.of(), MAX_FETCH_VERSION).forEach((tp, partitionData) -> {
            Errors error = Errors.forCode(partitionData.errorCode());
            if (error != Errors.NONE) {
                if (error.exception() instanceof RetriableException) {
                    log.debug("Retriable fetch error for {}: {}", tp, error);
                    metadata.refresh();
                    return;
                }
                if (error == Errors.OFFSET_OUT_OF_RANGE) {
                    log.info("Offset out of range for {}; resetting to earliest", tp);
                    positions.remove(tp);
                    return;
                }
                throw error.exception("Fetch failed for " + tp);
            }
            long position = positions.getOrDefault(tp, 0L);
            Records records = FetchResponse.recordsOrFail(partitionData);
            for (Record record : records.records()) {
                if (record.offset() < position)
                    continue; // compressed batches may begin before the fetch offset
                collected.add(new ConsumerRecordV2<>(tp, record.offset(), record.timestamp(),
                    deserialize(keyDeserializer, tp.topic(), record.key()),
                    deserialize(valueDeserializer, tp.topic(), record.value()),
                    List.of(record.headers())));
                position = record.offset() + 1;
            }
            positions.put(tp, position);
        });
        return collected;
    }

    private static <T> T deserialize(Deserializer<T> deserializer, String topic, java.nio.ByteBuffer data) {
        return data == null ? null : deserializer.deserialize(topic, Utils.toNullableArray(data));
    }

    private CompletableFuture<Map<TopicPartition, Long>> listOffsets(Collection<TopicPartition> partitions,
                                                                     long timestamp) {
        Set<String> topics = new HashSet<>();
        for (TopicPartition tp : partitions)
            topics.add(tp.topic());
        return metadata.cluster(topics).thenCompose(cluster -> {
            Map<Node, Map<String, ListOffsetsRequestData.ListOffsetsTopic>> byNode = new HashMap<>();
            for (TopicPartition tp : partitions) {
                Node leader = cluster.leaderFor(tp);
                if (leader == null || leader.isEmpty())
                    return failedAfterRefresh(tp);
                Map<String, ListOffsetsRequestData.ListOffsetsTopic> nodeTopics =
                    byNode.computeIfAbsent(leader, n -> new HashMap<>());
                nodeTopics.computeIfAbsent(tp.topic(), t ->
                        new ListOffsetsRequestData.ListOffsetsTopic().setName(t))
                    .partitions().add(new ListOffsetsRequestData.ListOffsetsPartition()
                        .setPartitionIndex(tp.partition())
                        .setTimestamp(timestamp));
            }
            List<CompletableFuture<Map<TopicPartition, Long>>> lookups = new ArrayList<>();
            for (Map.Entry<Node, Map<String, ListOffsetsRequestData.ListOffsetsTopic>> entry : byNode.entrySet()) {
                ListOffsetsRequest.Builder request = ListOffsetsRequest.Builder
                    .forConsumer(true, IsolationLevel.READ_UNCOMMITTED)
                    .setTargetTimes(new ArrayList<>(entry.getValue().values()));
                lookups.add(dispatcher.send(entry.getKey(), request)
                    .thenApply(response -> extractOffsets((ListOffsetsResponse) response)));
            }
            return CompletableFuture.allOf(lookups.toArray(new CompletableFuture<?>[0]))
                .thenApply(nothing -> {
                    Map<TopicPartition, Long> offsets = new HashMap<>();
                    for (CompletableFuture<Map<TopicPartition, Long>> lookup : lookups)
                        offsets.putAll(lookup.join());
                    return offsets;
                });
        });
    }

    private static Map<TopicPartition, Long> extractOffsets(ListOffsetsResponse response) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        for (ListOffsetsResponseData.ListOffsetsTopicResponse topic : response.data().topics()) {
            for (ListOffsetsResponseData.ListOffsetsPartitionResponse partition : topic.partitions()) {
                Errors error = Errors.forCode(partition.errorCode());
                if (error != Errors.NONE)
                    throw error.exception("ListOffsets failed for " + topic.name() + "-"
                        + partition.partitionIndex());
                offsets.put(new TopicPartition(topic.name(), partition.partitionIndex()),
                    partition.offset());
            }
        }
        return offsets;
    }

    private <T> CompletableFuture<T> failedAfterRefresh(TopicPartition tp) {
        metadata.refresh();
        return CompletableFuture.failedFuture(
            new org.apache.kafka.common.errors.LeaderNotAvailableException(
                "No leader for " + tp + "; metadata refresh triggered"));
    }
}
