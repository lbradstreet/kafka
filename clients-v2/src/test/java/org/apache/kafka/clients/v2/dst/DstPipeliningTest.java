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
package org.apache.kafka.clients.v2.dst;

import org.apache.kafka.clients.v2.producer.AsyncProducer;
import org.apache.kafka.clients.v2.producer.DefaultAsyncProducer;
import org.apache.kafka.clients.v2.producer.ProducerRecordV2;
import org.apache.kafka.clients.v2.producer.ProducerSettings;
import org.apache.kafka.clients.v2.producer.RecordMetadataV2;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.network.netty.BrokerTimingModel;
import org.apache.kafka.network.netty.FaultInjector.FaultProfile;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic simulation of produce batching and request pipelining under different broker
 * timing profiles (broker processing latency is what fills the client's in-flight window and
 * thereby drives batching/pipelining). Uses the {@code ProduceObserver} to assert on
 * outcomes directly rather than scraping the trace.
 */
public class DstPipeliningTest {

    private static final String TOPIC = "pipe-topic";

    /**
     * A busy broker (20 ms/request) with a 5-deep in-flight window: the pipeline fills to
     * its limit, and backpressure sealing then packs more records per request — fewer,
     * larger requests than with backpressure batching disabled, with identical delivery.
     */
    @Test
    public void testSlowBrokerFillsPipelineAndBackpressureBatches() {
        Result disabled = runSteady(1, 1, 512, 512, 5, 20);       // backpressure == batch.size
        Result enabled = runSteady(1, 1, 512, 16 * 512, 5, 20);   // 32x headroom

        assertEquals(disabled.stored, enabled.stored, "both configs deliver every record");
        assertTrue(enabled.maxInFlight >= 4,
            "a slow broker + 5-deep window should fill the pipeline; maxInFlight=" + enabled.maxInFlight);
        assertTrue(enabled.maxRecordsPerRequest > disabled.maxRecordsPerRequest,
            "backpressure should pack more records per request under latency: enabled="
                + enabled.maxRecordsPerRequest + " disabled=" + disabled.maxRecordsPerRequest);
        assertTrue(enabled.produceRequests < disabled.produceRequests,
            "larger batches mean fewer requests: enabled=" + enabled.produceRequests
                + " disabled=" + disabled.produceRequests);
    }

    /**
     * Fixed assignment to both partitions of a cluster where one leader is 50x slower:
     * every record on both partitions must still be delivered, and the fast broker keeps
     * flowing rather than stalling behind the slow one (per-node in-flight independence).
     */
    @Test
    public void testOneSlowNodeDoesNotStallTheFastNode() {
        // broker 1 fast (2ms), broker 2 slow (100ms); partitions 0->b1, 1->b2 (round-robin).
        try (DstHarness harness = new DstHarness(7, FaultProfile.NONE, 2,
            brokerId -> brokerId == 2
                ? BrokerTimingModel.constantProduceLatency(100)
                : BrokerTimingModel.constantProduceLatency(2))) {
            harness.cluster.createTopic(TOPIC, 2);
            ProducerSettings settings = ProducerSettings
                .newBuilder(harness.clientSettings().maxInFlight(5).build())
                .batchSize(512).linger(Duration.ofMillis(2)).build();
            AsyncProducer<String, String> producer = new DefaultAsyncProducer<>(
                harness.runtime, settings, new StringSerializer(), new StringSerializer());

            int perPartition = 150;
            List<CompletableFuture<RecordMetadataV2>> futures = new ArrayList<>();
            for (int i = 0; i < perPartition; i++) {
                final int idx = i;
                harness.scheduler.schedule(() -> {
                    futures.add(producer.send(new ProducerRecordV2<>(TOPIC,
                        java.util.OptionalInt.of(0), null, "p0-" + idx, java.util.OptionalLong.empty(), List.of())));
                    futures.add(producer.send(new ProducerRecordV2<>(TOPIC,
                        java.util.OptionalInt.of(1), null, "p1-" + idx, java.util.OptionalLong.empty(), List.of())));
                }, i, TimeUnit.MILLISECONDS);
            }
            harness.runUntil(() -> futures.size() == 2 * perPartition
                && futures.stream().allMatch(CompletableFuture::isDone));
            CompletableFuture<Void> closed = producer.closeAsync();
            harness.runUntil(closed::isDone);

            long failures = futures.stream().filter(CompletableFuture::isCompletedExceptionally).count();
            assertEquals(0, failures, "all records on both partitions must be delivered");
            assertEquals(perPartition, harness.cluster.log(new TopicPartition(TOPIC, 0)).size());
            assertEquals(perPartition, harness.cluster.log(new TopicPartition(TOPIC, 1)).size());
            // The fast broker handled its partition in far more (smaller, timely) requests
            // than the slow broker managed — it was never blocked behind the slow node.
            assertTrue(harness.observer.requestsToBroker(1) > harness.observer.requestsToBroker(2),
                "fast broker should out-serve the slow one: b1=" + harness.observer.requestsToBroker(1)
                    + " b2=" + harness.observer.requestsToBroker(2));
        }
    }

    /**
     * Late binding (Deferred) across the same asymmetric cluster: unbound batches bind to the
     * shortest-queue available leader, so load naturally shifts onto the fast broker.
     */
    @Test
    public void testLateBindingShiftsLoadToFasterBroker() {
        try (DstHarness harness = new DstHarness(11, FaultProfile.NONE, 2,
            brokerId -> brokerId == 2
                ? BrokerTimingModel.constantProduceLatency(100)
                : BrokerTimingModel.constantProduceLatency(2))) {
            harness.cluster.createTopic(TOPIC, 2);
            ProducerSettings settings = ProducerSettings
                .newBuilder(harness.clientSettings().maxInFlight(5).build())
                .batchSize(512).linger(Duration.ofMillis(2)).build();
            AsyncProducer<String, String> producer = new DefaultAsyncProducer<>(
                harness.runtime, settings, new StringSerializer(), new StringSerializer());

            int records = 300;
            List<CompletableFuture<RecordMetadataV2>> futures = new ArrayList<>();
            for (int i = 0; i < records; i++) {
                final int idx = i;
                harness.scheduler.schedule(() -> {
                    futures.add(producer.send(ProducerRecordV2.of(TOPIC, "v-" + idx)));
                }, i / 3, TimeUnit.MILLISECONDS);
            }
            harness.runUntil(() -> futures.size() == records
                && futures.stream().allMatch(CompletableFuture::isDone));
            CompletableFuture<Void> closed = producer.closeAsync();
            harness.runUntil(closed::isDone);

            long failures = futures.stream().filter(CompletableFuture::isCompletedExceptionally).count();
            assertEquals(0, failures, "every record delivered");
            assertEquals(records, harness.cluster.log(new TopicPartition(TOPIC, 0)).size()
                + harness.cluster.log(new TopicPartition(TOPIC, 1)).size());
            assertTrue(harness.observer.recordsToBroker(1) > harness.observer.recordsToBroker(2),
                "late binding should route most records to the fast broker: b1="
                    + harness.observer.recordsToBroker(1) + " b2=" + harness.observer.recordsToBroker(2));
        }
    }

    /** A timing-driven run is still a pure function of its seed. */
    @Test
    public void testSlowBrokerRunIsDeterministic() {
        List<String> first = traceOf();
        List<String> second = traceOf();
        assertEquals(first, second, "same scenario + seed must yield an identical trace");
    }

    private List<String> traceOf() {
        try (DstHarness harness = new DstHarness(3, FaultProfile.NONE, 1,
            brokerId -> BrokerTimingModel.seededJitter(99, 5, 25))) {
            harness.cluster.createTopic(TOPIC, 1);
            ProducerSettings settings = ProducerSettings
                .newBuilder(harness.clientSettings().maxInFlight(5).build())
                .batchSize(512).backpressureBatchSize(4096).linger(Duration.ofMillis(2)).build();
            AsyncProducer<String, String> producer = new DefaultAsyncProducer<>(
                harness.runtime, settings, new StringSerializer(), new StringSerializer());
            List<CompletableFuture<RecordMetadataV2>> futures = new ArrayList<>();
            for (int i = 0; i < 120; i++) {
                final int idx = i;
                harness.scheduler.schedule(() -> {
                    futures.add(producer.send(new ProducerRecordV2<>(TOPIC, java.util.OptionalInt.of(0),
                        null, "v-" + idx, java.util.OptionalLong.empty(), List.of())));
                }, i / 3, TimeUnit.MILLISECONDS);
            }
            harness.runUntil(() -> futures.size() == 120
                && futures.stream().allMatch(CompletableFuture::isDone));
            harness.runUntil(producer.closeAsync()::isDone);
            return harness.traceEvents();
        }
    }

    private record Result(int produceRequests, int maxRecordsPerRequest, int maxInFlight, int stored) { }

    private Result runSteady(long seed, int partitions, int batchSize, int backpressureBatchSize,
                             int maxInFlight, long brokerLatencyMs) {
        try (DstHarness harness = new DstHarness(seed, FaultProfile.NONE, 1,
            brokerId -> BrokerTimingModel.constantProduceLatency(brokerLatencyMs))) {
            harness.cluster.createTopic(TOPIC, partitions);
            ProducerSettings settings = ProducerSettings
                .newBuilder(harness.clientSettings().maxInFlight(maxInFlight).build())
                .batchSize(batchSize)
                .backpressureBatchSize(backpressureBatchSize)
                .maxRequestSize(1024 * 1024)
                .linger(Duration.ofMillis(1))
                .build();
            AsyncProducer<String, String> producer = new DefaultAsyncProducer<>(
                harness.runtime, settings, new StringSerializer(), new StringSerializer());

            int records = 600;
            List<CompletableFuture<RecordMetadataV2>> futures = new ArrayList<>();
            for (int i = 0; i < records; i++) {
                final int idx = i;
                // Produce faster than the pipe drains, so a backlog forms while the window is full.
                harness.scheduler.schedule(() -> {
                    futures.add(producer.send(new ProducerRecordV2<>(TOPIC, java.util.OptionalInt.of(0),
                        null, "v-" + idx, java.util.OptionalLong.empty(), List.of())));
                }, i / 6, TimeUnit.MILLISECONDS);
            }
            harness.runUntil(() -> futures.size() == records
                && futures.stream().allMatch(CompletableFuture::isDone));
            harness.runUntil(producer.closeAsync()::isDone);

            futures.forEach(f -> assertTrue(!f.isCompletedExceptionally(), "no failures expected"));
            int stored = harness.cluster.log(new TopicPartition(TOPIC, 0)).size();
            return new Result(harness.observer.produceRequests(),
                harness.observer.maxRecordsPerRequest(), harness.observer.maxInFlight(), stored);
        }
    }
}
