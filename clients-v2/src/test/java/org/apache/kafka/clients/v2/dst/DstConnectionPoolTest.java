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
import org.apache.kafka.network.netty.SimCluster;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic simulation of connection pooling (#4) and per-partition pipeline depth (#3):
 * against a high-latency broker, opening several connections per broker (partitions pinned to
 * a connection by affinity) multiplies aggregate pipeline depth, so the same workload drains
 * in far less (virtual) time — higher throughput — with per-partition ordering intact.
 */
public class DstConnectionPoolTest {

    private static final String TOPIC = "pool-topic";
    private static final int PARTITIONS = 16;
    private static final int RECORDS = 2400;

    /**
     * A 50 ms broker leading 8 partitions: with one connection the whole broker shares a
     * 5-deep window; with four connections the partitions spread across four independent
     * 5-deep windows, so the run completes markedly faster (higher throughput) — the fix for
     * the latency-bound-throughput problem.
     */
    @Test
    public void testPoolingRaisesThroughputToHighLatencyBroker() {
        Run single = run(1, 50);
        Run pooled = run(4, 50);

        assertEquals(RECORDS, single.stored, "single-connection run delivers everything");
        assertEquals(RECORDS, pooled.stored, "pooled run delivers everything");
        // Same records, same 50ms broker: more parallel connections ⇒ less virtual time.
        assertTrue(pooled.virtualTimeMs * 3 < single.virtualTimeMs,
            "4 connections should be well over 3x faster than 1 against a 50ms broker: pooled="
                + pooled.virtualTimeMs + "ms single=" + single.virtualTimeMs + "ms");
    }

    /** Partition→connection affinity keeps every partition's records complete and duplicate-free. */
    @Test
    public void testAffinityPreservesPerPartitionDeliveryAcrossThePool() {
        Run pooled = run(4, 20);
        assertEquals(RECORDS, pooled.stored, "no record lost across the pool");
        int partitionsUsed = 0;
        for (List<String> partitionLog : pooled.perPartitionValues) {
            if (!partitionLog.isEmpty())
                partitionsUsed++;
            assertEquals(partitionLog.size(), new HashSet<>(partitionLog).size(),
                "no duplicate records on a partition");
        }
        assertTrue(partitionsUsed > 1, "records should have spread across partitions/connections");
    }

    /** Pooled runs stay seed-deterministic. */
    @Test
    public void testPooledRunIsDeterministic() {
        assertEquals(run(4, 30).virtualTimeMs, run(4, 30).virtualTimeMs,
            "same scenario + seed ⇒ identical virtual-time outcome");
    }

    private record Run(int stored, long virtualTimeMs, List<List<String>> perPartitionValues) { }

    private Run run(int connectionsPerBroker, long brokerLatencyMs) {
        try (DstHarness harness = new DstHarness(1, FaultProfile.NONE, 1,
            brokerId -> BrokerTimingModel.constantProduceLatency(brokerLatencyMs))) {
            harness.cluster.createTopic(TOPIC, PARTITIONS);
            // Small max.request.size makes the workload request-count bound, so each broker's
            // in-flight window (5) is the bottleneck — exactly where extra connections help.
            ProducerSettings settings = ProducerSettings
                .newBuilder(harness.clientSettings().maxInFlight(5)
                    .connectionsPerBroker(connectionsPerBroker).build())
                .batchSize(512).maxRequestSize(1024).linger(Duration.ofMillis(1)).build();
            AsyncProducer<String, String> producer = new DefaultAsyncProducer<>(
                harness.runtime, settings, new StringSerializer(), new StringSerializer());

            List<CompletableFuture<RecordMetadataV2>> futures = new ArrayList<>();
            int recordsPerMs = RECORDS / 5; // dump the whole workload in ~5ms so a backlog forms
            for (int i = 0; i < RECORDS; i++) {
                final int idx = i;
                // Explicit partition spreads records evenly across all partitions.
                harness.scheduler.schedule(() -> {
                    futures.add(producer.send(new ProducerRecordV2<>(TOPIC,
                        java.util.OptionalInt.of(idx % PARTITIONS), null, "v-" + idx,
                        java.util.OptionalLong.empty(), List.of())));
                }, i / recordsPerMs, TimeUnit.MILLISECONDS);
            }
            harness.runUntil(() -> futures.size() == RECORDS
                && futures.stream().allMatch(CompletableFuture::isDone));
            harness.runUntil(producer.closeAsync()::isDone);

            futures.forEach(f -> assertTrue(!f.isCompletedExceptionally(), "no failures expected"));
            List<List<String>> perPartition = new ArrayList<>();
            int stored = 0;
            for (int p = 0; p < PARTITIONS; p++) {
                List<String> values = new ArrayList<>();
                for (SimCluster.StoredRecord record : harness.cluster.log(new TopicPartition(TOPIC, p)))
                    values.add(new String(record.value(), StandardCharsets.UTF_8));
                perPartition.add(values);
                stored += values.size();
            }
            return new Run(stored, harness.time.milliseconds(), perPartition);
        }
    }
}
