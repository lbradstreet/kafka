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
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.network.netty.FaultInjector.FaultProfile;
import org.apache.kafka.network.netty.SimCluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic simulation tests for the v2 producer (D13): the real transport pipeline and
 * producer run against an in-memory protocol-accurate broker under virtual time, with
 * seed-reproducible fault injection. The broker's log is the verification oracle.
 */
public class DstProducerTest {

    private static final String TOPIC = "dst-topic";
    private static final int PARTITIONS = 3;

    private record RunResult(List<String> trace,
                             int successes,
                             int failures,
                             Map<String, Integer> storedValueCounts,
                             Set<TopicPartition> partitionsUsed,
                             int faultsInjected) { }

    /**
     * One full scenario: create a topic, send records (half keyed, half deferred/late-bound,
     * lz4-compressed), drive the sim until every future has an outcome and the producer has
     * closed, then read the broker logs.
     */
    private RunResult run(long seed, FaultProfile profile, int records, int retries) {
        try (DstHarness harness = new DstHarness(seed, profile, 2)) {
            harness.cluster.createTopic(TOPIC, PARTITIONS);
            ProducerSettings settings = ProducerSettings
                .newBuilder(harness.clientSettings().build())
                .compression(Compression.lz4().build())
                .linger(Duration.ofMillis(5))
                .retries(retries)
                .build();
            AsyncProducer<String, String> producer = new DefaultAsyncProducer<>(
                harness.runtime, settings, new StringSerializer(), new StringSerializer());

            List<CompletableFuture<RecordMetadataV2>> futures = new ArrayList<>();
            for (int i = 0; i < records; i++) {
                String key = i % 2 == 0 ? "key-" + i : null;
                futures.add(producer.send(ProducerRecordV2.of(TOPIC, key, "value-" + i)));
            }
            harness.runUntil(() -> futures.stream().allMatch(CompletableFuture::isDone));
            CompletableFuture<Void> closed = producer.closeAsync();
            harness.runUntil(closed::isDone);

            int successes = 0;
            int failures = 0;
            Set<TopicPartition> partitionsUsed = new HashSet<>();
            for (CompletableFuture<RecordMetadataV2> future : futures) {
                if (future.isCompletedExceptionally()) {
                    failures++;
                } else {
                    successes++;
                    partitionsUsed.add(future.join().topicPartition());
                }
            }
            Map<String, Integer> stored = new HashMap<>();
            for (int p = 0; p < PARTITIONS; p++) {
                for (SimCluster.StoredRecord record : harness.cluster.log(new TopicPartition(TOPIC, p)))
                    stored.merge(new String(record.value(), StandardCharsets.UTF_8), 1, Integer::sum);
            }
            return new RunResult(harness.traceEvents(), successes, failures, stored,
                partitionsUsed, harness.faults.faultsInjected());
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19})
    public void testQuietNetworkDeliversExactlyOnce(long seed) {
        int records = 200;
        RunResult result = run(seed, FaultProfile.NONE, records, 3);
        assertEquals(0, result.failures(), "no failures expected on a quiet network");
        assertEquals(records, result.successes());
        assertEquals(records, result.storedValueCounts().size(), "every value stored");
        result.storedValueCounts().forEach((value, count) ->
            assertEquals(1, count, "no duplicates on a quiet network: " + value));
        assertTrue(result.partitionsUsed().size() > 1,
            "keyed hashing + late binding should spread partitions, used " + result.partitionsUsed());
    }

    @ParameterizedTest
    @ValueSource(longs = {7, 21, 42})
    public void testSameSeedYieldsIdenticalTrace(long seed) {
        FaultProfile faulty = new FaultProfile(0.05, 0.05, 0.01, 20);
        RunResult first = run(seed, faulty, 80, 8);
        RunResult second = run(seed, faulty, 80, 8);
        assertTrue(first.faultsInjected() > 0,
            "these profiles/seeds are expected to inject faults — otherwise the test proves little");
        assertEquals(first.trace(), second.trace(),
            "a DST run must be a pure function of (scenario, seed)");
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19})
    public void testFaultyNetworkNeverHangsOrLosesAcks(long seed) {
        int records = 120;
        FaultProfile faulty = new FaultProfile(0.05, 0.05, 0.01, 20);
        // runUntil throws on a simulated hang, so completing at all asserts liveness.
        RunResult result = run(seed, faulty, records, 8);
        assertEquals(records, result.successes() + result.failures(), "every future has an outcome");
        // Acks may only be lost, never invented: every acked record must be in the log.
        // (Without idempotence, duplicates are legal when faults fired — count them only.)
        int storedTotal = result.storedValueCounts().values().stream().mapToInt(Integer::intValue).sum();
        assertTrue(storedTotal >= result.successes(),
            "stored " + storedTotal + " < acked " + result.successes() + " — an ack was invented");
        if (result.faultsInjected() == 0)
            result.storedValueCounts().forEach((value, count) ->
                assertEquals(1, count, "duplicates require injected faults: " + value));
    }

    /**
     * Adaptive batch sealing: with the in-flight window saturated (maxInFlight=1, staggered
     * sends racing a slow round trip), batches keep growing past batch.size up to
     * backpressure.batch.size — fewer, larger produce requests than with the feature off.
     */
    @Test
    public void testBackpressureSealingBatchesMoreWhileWindowIsSaturated() {
        BatchingStats disabled = runStaggered(4242, 512, 512);        // backpressure = batch.size
        BatchingStats enabled = runStaggered(4242, 512, 8 * 512);     // 8x headroom

        assertEquals(disabled.recordsStored(), enabled.recordsStored(), "both must deliver everything");
        assertTrue(enabled.maxRecordsPerRequest() > disabled.maxRecordsPerRequest(),
            "saturation should grow batches past batch.size: enabled max="
                + enabled.maxRecordsPerRequest() + " vs disabled max=" + disabled.maxRecordsPerRequest());
        assertTrue(enabled.produceRequests() < disabled.produceRequests(),
            "larger batches must mean fewer requests: enabled=" + enabled.produceRequests()
                + " vs disabled=" + disabled.produceRequests());
    }

    private record BatchingStats(int produceRequests, int maxRecordsPerRequest, int recordsStored) { }

    private BatchingStats runStaggered(long seed, int batchSize, int backpressureBatchSize) {
        try (DstHarness harness = new DstHarness(seed, FaultProfile.NONE, 1)) {
            harness.cluster.createTopic(TOPIC, 1); // one partition, one saturated pipe
            ProducerSettings settings = ProducerSettings
                .newBuilder(harness.clientSettings().maxInFlight(1).build())
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
                String value = "value-" + i;
                // Six records per virtual millisecond: production outruns the single-request
                // window (RTT ≈ 4ms), so a genuine backlog forms while the window is closed.
                harness.scheduler.schedule(() -> {
                    futures.add(producer.send(ProducerRecordV2.of(TOPIC, value)));
                }, i / 6, TimeUnit.MILLISECONDS);
            }
            harness.runUntil(() -> futures.size() == records
                && futures.stream().allMatch(CompletableFuture::isDone));
            CompletableFuture<Void> closed = producer.closeAsync();
            harness.runUntil(closed::isDone);

            futures.forEach(f -> assertTrue(!f.isCompletedExceptionally(), "no failures expected"));
            int produceRequests = 0;
            int maxCount = 0;
            for (String event : harness.traceEvents()) {
                int idx = event.indexOf(" count=");
                if (event.contains(" produce " + TOPIC) && idx >= 0) {
                    produceRequests++;
                    maxCount = Math.max(maxCount, Integer.parseInt(event.substring(idx + 7)));
                }
            }
            int stored = harness.cluster.log(new TopicPartition(TOPIC, 0)).size();
            assertEquals(records, stored, "exactly-once delivery expected without faults");
            return new BatchingStats(produceRequests, maxCount, stored);
        }
    }

    @Test
    public void testDeferredRecordsRebindAcrossLeaderFailover() {
        try (DstHarness harness = new DstHarness(99, FaultProfile.NONE, 2)) {
            harness.cluster.createTopic(TOPIC, PARTITIONS);
            // Start with every leader on broker 1; move all leadership to broker 2 mid-run.
            for (int p = 0; p < PARTITIONS; p++)
                harness.cluster.moveLeader(new TopicPartition(TOPIC, p), 1);
            harness.scheduler.schedule(() -> {
                for (int p = 0; p < PARTITIONS; p++)
                    harness.cluster.moveLeader(new TopicPartition(TOPIC, p), 2);
                harness.trace.add("test moved all leaders to broker-2");
            }, 40, TimeUnit.MILLISECONDS);

            ProducerSettings settings = ProducerSettings
                .newBuilder(harness.clientSettings().build())
                .linger(Duration.ofMillis(30))
                .retries(10)
                .build();
            AsyncProducer<String, String> producer = new DefaultAsyncProducer<>(
                harness.runtime, settings, new StringSerializer(), new StringSerializer());

            int records = 90;
            List<CompletableFuture<RecordMetadataV2>> futures = new ArrayList<>();
            for (int i = 0; i < records; i++) // all unkeyed → Deferred → late-bound (D14)
                futures.add(producer.send(ProducerRecordV2.of(TOPIC, "value-" + i)));

            harness.runUntil(() -> futures.stream().allMatch(CompletableFuture::isDone));
            CompletableFuture<Void> closed = producer.closeAsync();
            harness.runUntil(closed::isDone);

            long failures = futures.stream().filter(CompletableFuture::isCompletedExceptionally).count();
            assertEquals(0, failures, "failover of every leader must be absorbed by retries/rebinding");
            // NOT_LEADER responses never append, so delivery is exact even across the move.
            int stored = 0;
            for (int p = 0; p < PARTITIONS; p++)
                stored += harness.cluster.log(new TopicPartition(TOPIC, p)).size();
            assertEquals(records, stored, "exactly-once delivery expected without network faults");
        }
    }
}
