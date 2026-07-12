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

import org.apache.kafka.clients.v2.ClientSettings;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RecordAccumulatorV2Test {

    private static final String TOPIC = "test";
    private static final Node NODE = new Node(1, "localhost", 9092);
    private static final Header[] NO_HEADERS = new Header[0];
    private static final byte[] VALUE = new byte[64];

    private static Cluster cluster(int partitions) {
        PartitionInfo[] infos = new PartitionInfo[partitions];
        for (int i = 0; i < partitions; i++)
            infos[i] = new PartitionInfo(TOPIC, i, NODE, new Node[]{NODE}, new Node[]{NODE});
        return new Cluster("test-cluster", List.of(NODE), List.of(infos), Set.of(), Set.of());
    }

    private static RecordAccumulatorV2 accumulator(int batchSize, Duration linger) {
        return accumulator(batchSize, linger, BackpressureSignal.NEVER);
    }

    private static RecordAccumulatorV2 accumulator(int batchSize, Duration linger,
                                                   BackpressureSignal backpressure) {
        ClientSettings client = ClientSettings.newBuilder("localhost:9092").build();
        ProducerSettings settings = ProducerSettings.newBuilder(client)
            .batchSize(batchSize)
            .backpressureBatchSize(batchSize * 4)
            .linger(linger)
            .build();
        return new RecordAccumulatorV2(settings, new MemoryLimiter(1 << 20), BatchSealer.NO_OP,
            backpressure);
    }

    @Test
    public void testDrainsAtMostOneBatchPerPartitionPerRound() {
        // Regression test: two batches of the same partition in one produce request would
        // clobber each other in the per-partition request/response maps and hang futures.
        RecordAccumulatorV2 accumulator = accumulator(256, Duration.ZERO);
        PartitionAssignment fixed = new PartitionAssignment.Fixed(0);
        for (int i = 0; i < 50; i++)
            accumulator.append(TOPIC, fixed, 0L, null, VALUE, NO_HEADERS, 0L);

        Map<Node, List<BatchV2>> first = accumulator.drain(cluster(1), 1L);
        assertEquals(1, first.get(NODE).size(), "one batch per partition per drain round");
        assertTrue(first.get(NODE).get(0).isSealed());

        int rounds = 1;
        while (!accumulator.drain(cluster(1), 1L).isEmpty())
            rounds++;
        assertTrue(rounds > 1, "remaining batches must drain in later rounds");
        assertTrue(accumulator.topics().contains(TOPIC));
    }

    @Test
    public void testDeferredBatchesBindAtDrainTime() {
        RecordAccumulatorV2 accumulator = accumulator(16 * 1024, Duration.ZERO);
        accumulator.append(TOPIC, PartitionAssignment.DEFERRED, 0L, null, VALUE, NO_HEADERS, 0L);

        Map<Node, List<BatchV2>> drained = accumulator.drain(cluster(3), 1L);
        assertEquals(1, drained.get(NODE).size());
        BatchV2 batch = drained.get(NODE).get(0);
        assertNotNull(batch.partition(), "deferred batch must be bound at drain");
        assertEquals(TOPIC, batch.partition().topic());
        assertTrue(batch.isSealed());
    }

    @Test
    public void testDeferredBatchesSpreadByQueueDepth() {
        RecordAccumulatorV2 accumulator = accumulator(256, Duration.ZERO);
        for (int i = 0; i < 40; i++)
            accumulator.append(TOPIC, PartitionAssignment.DEFERRED, 0L, null, VALUE, NO_HEADERS, 0L);

        Set<TopicPartition> used = new java.util.HashSet<>();
        Map<Node, List<BatchV2>> drained;
        while (!(drained = accumulator.drain(cluster(3), 1L)).isEmpty())
            for (BatchV2 batch : drained.get(NODE))
                used.add(batch.partition());
        assertTrue(used.size() > 1, "late binding should use multiple partitions, used " + used);
    }

    /** Saturation signal switchable mid-test. */
    private static final class SwitchableSignal implements BackpressureSignal {
        volatile boolean saturated = false;

        @Override
        public boolean isSaturated(TopicPartition partition) {
            return saturated;
        }

        @Override
        public boolean isTopicSaturated(String topic) {
            return saturated;
        }
    }

    @Test
    public void testSaturationGrowsBatchesPastBatchSizeAndDefersDrain() {
        SwitchableSignal signal = new SwitchableSignal();
        int batchSize = 256;
        RecordAccumulatorV2 accumulator = accumulator(batchSize, Duration.ZERO, signal);
        PartitionAssignment fixed = new PartitionAssignment.Fixed(0);

        // Saturated: the open batch must keep absorbing records past batch.size (up to the
        // 4x backpressure limit) and the drain must skip the partition entirely.
        signal.saturated = true;
        for (int i = 0; i < 12; i++) // 12 * 64B values ≈ 3x batch.size uncompressed
            accumulator.append(TOPIC, fixed, 0L, null, VALUE, NO_HEADERS, 0L);
        assertTrue(accumulator.drain(cluster(1), 1L).isEmpty(),
            "saturated leaders must not be drained (no premature sealing)");

        // Window opens: everything drains, and the batches are larger than batch.size —
        // fewer, bigger requests than non-backpressured accumulation would produce.
        signal.saturated = false;
        int batches = 0;
        int maxBatchBytes = 0;
        Map<Node, List<BatchV2>> drained;
        while (!(drained = accumulator.drain(cluster(1), 1L)).isEmpty()) {
            for (BatchV2 batch : drained.get(NODE)) {
                batches++;
                maxBatchBytes = Math.max(maxBatchBytes, batch.records().sizeInBytes());
            }
        }
        assertTrue(maxBatchBytes > batchSize,
            "backpressured batch should exceed batch.size, was " + maxBatchBytes);
        assertTrue(batches < 12 * 70 / batchSize,
            "expected fewer, larger batches under backpressure; got " + batches);
    }

    @Test
    public void testWithoutSaturationBatchesSealNearBatchSize() {
        int batchSize = 256;
        RecordAccumulatorV2 accumulator = accumulator(batchSize, Duration.ZERO);
        PartitionAssignment fixed = new PartitionAssignment.Fixed(0);
        for (int i = 0; i < 12; i++)
            accumulator.append(TOPIC, fixed, 0L, null, VALUE, NO_HEADERS, 0L);
        Map<Node, List<BatchV2>> drained;
        while (!(drained = accumulator.drain(cluster(1), 1L)).isEmpty()) {
            for (BatchV2 batch : drained.get(NODE))
                assertTrue(batch.records().sizeInBytes() <= batchSize + 2 * VALUE.length + 128,
                    "unsaturated batches stay near batch.size, was " + batch.records().sizeInBytes());
        }
    }

    @Test
    public void testBackpressureGrowthIsFundedAgainstTheBudget() {
        SwitchableSignal signal = new SwitchableSignal();
        signal.saturated = true;
        int batchSize = 256;
        MemoryLimiter limiter = new MemoryLimiter(600); // less than the 1024 backpressure limit
        ClientSettings client = ClientSettings.newBuilder("localhost:9092").build();
        ProducerSettings settings = ProducerSettings.newBuilder(client)
            .batchSize(batchSize)
            .backpressureBatchSize(4 * batchSize)
            .linger(Duration.ZERO)
            .build();
        RecordAccumulatorV2 accumulator =
            new RecordAccumulatorV2(settings, limiter, BatchSealer.NO_OP, signal);
        PartitionAssignment fixed = new PartitionAssignment.Fixed(0);

        // Growth past batch.size is funded incrementally; once the budget is dry the batch
        // stops growing and the next batch's charge fails fast.
        boolean exhausted = false;
        for (int i = 0; i < 40 && !exhausted; i++) {
            try {
                accumulator.append(TOPIC, fixed, 0L, null, VALUE, NO_HEADERS, 0L);
            } catch (org.apache.kafka.common.errors.TimeoutException e) {
                exhausted = true;
            }
        }
        assertTrue(exhausted, "budget exhaustion must surface once growth cannot be funded");

        // Every batch's actual size stays within what was charged for it — the budget was
        // never silently exceeded.
        signal.saturated = false;
        long released = 0;
        Map<Node, List<BatchV2>> drained;
        while (!(drained = accumulator.drain(cluster(1), 1L)).isEmpty()) {
            for (BatchV2 batch : drained.get(NODE)) {
                assertTrue(batch.records().sizeInBytes() <= batch.memoryCharged(),
                    "batch bytes " + batch.records().sizeInBytes()
                        + " exceed its charge " + batch.memoryCharged());
                released += batch.memoryCharged();
                batch.completeSuccessfully(0L, -1L);
            }
        }
        assertTrue(released > batchSize, "growth top-ups should have been charged");
        assertEquals(600, limiter.available(), "completing all batches must return the full budget");
    }

    @Test
    public void testFailAllCompletesEveryPendingFutureAndReleasesBudget() {
        // Close-deadline path: batches that never got sent must be failed, not stranded —
        // every send() future completes and the whole budget comes back.
        MemoryLimiter limiter = new MemoryLimiter(1 << 20);
        ClientSettings client = ClientSettings.newBuilder("localhost:9092").build();
        ProducerSettings settings = ProducerSettings.newBuilder(client)
            .batchSize(256).backpressureBatchSize(1024).linger(Duration.ofMinutes(1)).build();
        RecordAccumulatorV2 accumulator =
            new RecordAccumulatorV2(settings, limiter, BatchSealer.NO_OP, BackpressureSignal.NEVER);
        PartitionAssignment fixed = new PartitionAssignment.Fixed(0);

        List<CompletableFuture<RecordMetadataV2>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++)
            futures.add(accumulator.append(TOPIC, fixed, 0L, null, VALUE, NO_HEADERS, 0L));
        assertTrue(limiter.available() < (1 << 20), "budget is charged while batches are open");

        accumulator.failAll(new org.apache.kafka.common.errors.TimeoutException("closed"));

        for (CompletableFuture<RecordMetadataV2> future : futures)
            assertTrue(future.isCompletedExceptionally(), "every pending record future must fail");
        assertTrue(accumulator.isEmpty(), "failed batches leave the accumulator empty");
        assertEquals(1 << 20, limiter.available(), "failAll must release all charged budget");
    }

    @Test
    public void testDrainNeverExceedsMaxRequestSizeWithBackpressureGrownBatches() {
        // The per-(node,connection) drain cap must estimate an unsealed batch at its real grown
        // size, not the batch.size soft limit — otherwise several backpressure-grown batches
        // sharing one connection assemble a request past max.request.size.
        SwitchableSignal signal = new SwitchableSignal();
        signal.saturated = true;
        int batchSize = 256;
        int maxRequestSize = 2048;
        MemoryLimiter limiter = new MemoryLimiter(1 << 20);
        ClientSettings client = ClientSettings.newBuilder("localhost:9092").build();
        ProducerSettings settings = ProducerSettings.newBuilder(client)
            .batchSize(batchSize).backpressureBatchSize(4 * batchSize)
            .maxRequestSize(maxRequestSize).linger(Duration.ZERO).build();
        RecordAccumulatorV2 accumulator =
            new RecordAccumulatorV2(settings, limiter, BatchSealer.NO_OP, signal);

        // Grow six partitions well past batch.size under saturation. Default connectionIndex
        // pins them all to one (node, connection), so each drain round is one request.
        for (int p = 0; p < 6; p++) {
            PartitionAssignment fixed = new PartitionAssignment.Fixed(p);
            for (int i = 0; i < 12; i++)
                accumulator.append(TOPIC, fixed, 0L, null, VALUE, NO_HEADERS, 0L);
        }

        signal.saturated = false;
        boolean sawMultiBatchRound = false;
        Map<Node, List<BatchV2>> drained;
        while (!(drained = accumulator.drain(cluster(6), 1L)).isEmpty()) {
            int roundBytes = 0;
            for (BatchV2 batch : drained.get(NODE))
                roundBytes += batch.records().sizeInBytes();
            assertTrue(roundBytes <= maxRequestSize,
                "a drain round must stay within max.request.size, was " + roundBytes);
            sawMultiBatchRound |= drained.get(NODE).size() > 1;
            for (BatchV2 batch : drained.get(NODE))
                batch.completeSuccessfully(0L, -1L);
        }
        assertTrue(sawMultiBatchRound, "cap should still pack multiple batches per request");
    }

    @Test
    public void testSealedBatchKeepsPartitionOnReenqueue() {
        RecordAccumulatorV2 accumulator = accumulator(16 * 1024, Duration.ZERO);
        accumulator.append(TOPIC, PartitionAssignment.DEFERRED, 0L, null, VALUE, NO_HEADERS, 0L);
        BatchV2 batch = accumulator.drain(cluster(3), 1L).get(NODE).get(0);
        TopicPartition boundTo = batch.partition();

        accumulator.reenqueue(batch);
        BatchV2 redrained = accumulator.drain(cluster(3), 1L).get(NODE).get(0);
        assertEquals(boundTo, redrained.partition(), "sealed batches are pinned to their partition (D14/D15)");
    }
}
