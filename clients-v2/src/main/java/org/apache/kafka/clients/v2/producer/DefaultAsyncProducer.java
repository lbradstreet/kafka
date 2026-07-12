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
package org.apache.kafka.clients.v2.producer;

import org.apache.kafka.clients.v2.KafkaClientRuntime;
import org.apache.kafka.clients.v2.MetadataManager;
import org.apache.kafka.clients.v2.NetworkRequestDispatcher;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Production {@link AsyncProducer} on the Netty transport.
 *
 * <p>{@code send()} never blocks: metadata is awaited asynchronously, memory-budget exhaustion
 * fails the future, and all I/O happens on the transport's event loops plus one drain thread
 * ({@link SenderV2}).
 */
public final class DefaultAsyncProducer<K, V> implements AsyncProducer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(DefaultAsyncProducer.class);
    private static final Header[] NO_HEADERS = new Header[0];

    private final KafkaClientRuntime runtime;
    private final ProducerSettings settings;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final PartitionerV2 partitioner;

    private final NetworkRequestDispatcher dispatcher;
    private final MetadataManager metadata;
    private final RecordAccumulatorV2 accumulator;
    private final SenderV2 sender;

    private volatile boolean closed = false;

    public DefaultAsyncProducer(KafkaClientRuntime runtime, ProducerSettings settings,
                                Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(runtime, settings, keySerializer, valueSerializer, PartitionerV2.defaultPartitioner());
    }

    public DefaultAsyncProducer(KafkaClientRuntime runtime, ProducerSettings settings,
                                Serializer<K> keySerializer, Serializer<V> valueSerializer,
                                PartitionerV2 partitioner) {
        this.runtime = runtime;
        this.settings = settings;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.partitioner = partitioner;
        this.dispatcher = new NetworkRequestDispatcher(runtime, settings.client());
        this.metadata = new MetadataManager(runtime, dispatcher, settings.client());
        this.accumulator = new RecordAccumulatorV2(settings,
            new MemoryLimiter(settings.bufferMemory()), BatchSealer.NO_OP,
            new LeaderBackpressureSignal(dispatcher, metadata),
            dispatcher::connectionIndex);
        this.sender = new SenderV2(runtime, settings, dispatcher, metadata, accumulator);
        this.sender.start();
    }

    /**
     * Backpressure = the connection a partition is pinned to cannot send right now (its
     * in-flight window is full or the broker throttled it). Because the signal is judged per
     * the partition's own pool connection (#3/#4), a partition on a busy connection batches
     * larger while a partition on an idle connection of the same broker keeps sealing normally.
     * Uses only the cached cluster view — never triggers I/O from the send path.
     */
    private record LeaderBackpressureSignal(NetworkRequestDispatcher dispatcher,
                                            MetadataManager metadata) implements BackpressureSignal {
        @Override
        public boolean isSaturated(org.apache.kafka.common.TopicPartition partition) {
            org.apache.kafka.common.Cluster cluster = metadata.cachedCluster();
            if (cluster == null)
                return false;
            org.apache.kafka.common.Node leader = cluster.leaderFor(partition);
            return leader != null && !leader.isEmpty()
                && dispatcher.isSaturated(leader, dispatcher.connectionIndex(partition));
        }

        @Override
        public boolean isTopicSaturated(String topic) {
            org.apache.kafka.common.Cluster cluster = metadata.cachedCluster();
            if (cluster == null)
                return false;
            var partitions = cluster.availablePartitionsForTopic(topic);
            if (partitions.isEmpty())
                return false;
            for (org.apache.kafka.common.PartitionInfo info : partitions) {
                var tp = new org.apache.kafka.common.TopicPartition(info.topic(), info.partition());
                if (!dispatcher.isSaturated(info.leader(), dispatcher.connectionIndex(tp)))
                    return false; // at least one partition's connection can take a request now
            }
            return true;
        }
    }

    @Override
    public CompletableFuture<RecordMetadataV2> send(ProducerRecordV2<K, V> record) {
        if (closed)
            return CompletableFuture.failedFuture(new IllegalStateException("Producer is closed"));
        final byte[] keyBytes;
        final byte[] valueBytes;
        try {
            keyBytes = record.key() == null ? null
                : keySerializer.serialize(record.topic(), record.key());
            valueBytes = record.value() == null ? null
                : valueSerializer.serialize(record.topic(), record.value());
        } catch (Exception e) {
            return CompletableFuture.failedFuture(new KafkaException("Serialization failed", e));
        }
        long timestamp = record.timestamp().orElseGet(() -> runtime.time().milliseconds());
        Header[] headers = headersOf(record.headers());

        if (record.partition().isPresent()) {
            return append(record.topic(),
                new PartitionAssignment.Fixed(record.partition().getAsInt()),
                timestamp, keyBytes, valueBytes, headers);
        }
        // The partitioner needs the partition count, so resolve metadata first (usually cached).
        return metadata.cluster(Set.of(record.topic())).thenCompose(cluster -> {
            Integer partitionCount = cluster.partitionCountForTopic(record.topic());
            if (partitionCount == null || partitionCount == 0)
                return CompletableFuture.failedFuture(
                    new UnknownTopicOrPartitionException("Unknown topic: " + record.topic()));
            PartitionAssignment assignment = partitioner.assign(record.topic(), keyBytes, partitionCount);
            return append(record.topic(), assignment, timestamp, keyBytes, valueBytes, headers);
        });
    }

    private CompletableFuture<RecordMetadataV2> append(String topic, PartitionAssignment assignment,
                                                       long timestamp, byte[] key, byte[] value,
                                                       Header[] headers) {
        try {
            return accumulator.append(topic, assignment, timestamp, key, value, headers,
                runtime.time().milliseconds());
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> flush() {
        accumulator.requestFlush();
        List<CompletableFuture<Void>> outstanding = accumulator.incompleteBatchFutures();
        return CompletableFuture.allOf(outstanding.toArray(new CompletableFuture<?>[0]));
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        closed = true;
        return sender.closeAsync().whenComplete((v, e) -> dispatcher.close());
    }

    @Override
    public void close() {
        try {
            closeAsync().get(2 * settings.client().requestTimeout().toMillis() + 5_000,
                java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("v2 producer close did not finish cleanly", e);
        }
    }

    private static Header[] headersOf(List<Header> headers) {
        return headers.isEmpty() ? NO_HEADERS : headers.toArray(new Header[0]);
    }
}
