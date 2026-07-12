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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Accumulates records into batches per partition — plus, for {@code Deferred} assignments
 * (D14), per <em>topic</em>: unbound batches are bound to a concrete partition only at drain
 * time, preferring partitions with available leaders and the shortest bound queue.
 *
 * <p>Appends synchronize on the per-queue lock, like the classic accumulator. Batches are
 * sealed (D15 seam) exactly once, at drain.
 */
final class RecordAccumulatorV2 {

    private final ProducerSettings settings;
    private final MemoryLimiter limiter;
    private final BatchSealer sealer;

    private final ConcurrentMap<TopicPartition, Deque<BatchV2>> bound = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Deque<BatchV2>> unbound = new ConcurrentHashMap<>();
    private final Set<BatchV2> incomplete = ConcurrentHashMap.newKeySet();
    private volatile boolean flushRequested = false;

    RecordAccumulatorV2(ProducerSettings settings, MemoryLimiter limiter, BatchSealer sealer) {
        this.settings = settings;
        this.limiter = limiter;
        this.sealer = sealer;
    }

    /** Append to a bound (Fixed) or unbound (Deferred) queue. */
    CompletableFuture<RecordMetadataV2> append(String topic, PartitionAssignment assignment,
                                               long timestamp, byte[] key, byte[] value,
                                               Header[] headers, long nowMs) {
        Deque<BatchV2> queue;
        TopicPartition partition;
        if (assignment instanceof PartitionAssignment.Fixed fixed) {
            partition = new TopicPartition(topic, fixed.partition());
            queue = bound.computeIfAbsent(partition, tp -> new ArrayDeque<>());
        } else {
            partition = null;
            queue = unbound.computeIfAbsent(topic, t -> new ArrayDeque<>());
        }
        synchronized (queue) {
            BatchV2 last = queue.peekLast();
            if (last != null && last.hasRoomFor(timestamp, key, value, headers))
                return last.append(timestamp, key, value, headers);
            limiter.acquireNow(settings.batchSize());
            BatchV2 batch = new BatchV2(partition, settings.batchSize(), settings.compression(),
                nowMs, settings.batchSize());
            incomplete.add(batch);
            batch.doneFuture().whenComplete((v, e) -> {
                incomplete.remove(batch);
                limiter.release(batch.memoryCharged());
            });
            queue.addLast(batch);
            return batch.append(timestamp, key, value, headers);
        }
    }

    Set<String> topics() {
        Set<String> topics = new HashSet<>(unbound.keySet());
        for (TopicPartition tp : bound.keySet())
            topics.add(tp.topic());
        return topics;
    }

    boolean isEmpty() {
        return incomplete.isEmpty();
    }

    /** Makes every currently open batch drainable regardless of linger. */
    void requestFlush() {
        flushRequested = true;
    }

    /** Re-arms linger once a flush has fully drained. */
    void clearFlushIfDrained() {
        if (flushRequested && incomplete.isEmpty())
            flushRequested = false;
    }

    List<CompletableFuture<Void>> incompleteBatchFutures() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (BatchV2 batch : incomplete)
            futures.add(batch.doneFuture());
        return futures;
    }

    /** Re-enqueue a sealed batch for retry; it keeps its partition (D14/D15 rule). */
    void reenqueue(BatchV2 batch) {
        Deque<BatchV2> queue = bound.computeIfAbsent(batch.partition(), tp -> new ArrayDeque<>());
        synchronized (queue) {
            queue.addFirst(batch);
        }
    }

    /**
     * Drain sealed, leader-routable batches grouped by destination node, up to
     * {@code max.request.size} per node per round.
     */
    Map<Node, List<BatchV2>> drain(Cluster cluster, long nowMs) {
        bindReadyUnboundBatches(cluster, nowMs);

        Map<Node, List<BatchV2>> drained = new HashMap<>();
        Map<Node, Integer> nodeBytes = new HashMap<>();
        for (Map.Entry<TopicPartition, Deque<BatchV2>> entry : bound.entrySet()) {
            Node leader = cluster.leaderFor(entry.getKey());
            if (leader == null || leader.isEmpty())
                continue; // metadata refresh will find the new leader
            Deque<BatchV2> queue = entry.getValue();
            synchronized (queue) {
                BatchV2 batch;
                while ((batch = queue.peekFirst()) != null) {
                    if (!isDrainable(batch, nowMs))
                        break;
                    int size = batch.isSealed()
                        ? batch.records().sizeInBytes()
                        : settings.batchSize();
                    int used = nodeBytes.getOrDefault(leader, 0);
                    if (used > 0 && used + size > settings.maxRequestSize())
                        break;
                    queue.pollFirst();
                    if (!batch.isSealed()) {
                        batch.seal();
                        sealer.seal(batch, batch.partition());
                    }
                    nodeBytes.merge(leader, batch.records().sizeInBytes(), Integer::sum);
                    drained.computeIfAbsent(leader, n -> new ArrayList<>()).add(batch);
                }
            }
        }
        return drained;
    }

    private boolean isDrainable(BatchV2 batch, long nowMs) {
        if (batch.isEmpty())
            return false;
        return batch.isSealed() // a retry re-enqueue
            || batch.isFull()
            || flushRequested
            || nowMs - batch.createdMs() >= settings.linger().toMillis();
    }

    /**
     * Late binding (D14): assign drainable unbound batches to the shortest bound queue among
     * partitions whose leader is currently available.
     */
    private void bindReadyUnboundBatches(Cluster cluster, long nowMs) {
        for (Map.Entry<String, Deque<BatchV2>> entry : unbound.entrySet()) {
            String topic = entry.getKey();
            Deque<BatchV2> queue = entry.getValue();
            synchronized (queue) {
                BatchV2 batch;
                while ((batch = queue.peekFirst()) != null) {
                    if (!isDrainable(batch, nowMs))
                        break;
                    List<PartitionInfo> available = cluster.availablePartitionsForTopic(topic);
                    if (available.isEmpty())
                        break; // no live leaders; batch stays unbound and re-bindable
                    TopicPartition target = shortestQueuePartition(available);
                    queue.pollFirst();
                    batch.bind(target);
                    Deque<BatchV2> boundQueue = bound.computeIfAbsent(target, tp -> new ArrayDeque<>());
                    synchronized (boundQueue) {
                        boundQueue.addLast(batch);
                    }
                }
            }
        }
    }

    private TopicPartition shortestQueuePartition(List<PartitionInfo> available) {
        TopicPartition best = null;
        int bestDepth = Integer.MAX_VALUE;
        for (PartitionInfo info : available) {
            TopicPartition tp = new TopicPartition(info.topic(), info.partition());
            Deque<BatchV2> queue = bound.get(tp);
            int depth = queue == null ? 0 : queue.size();
            if (depth < bestDepth) {
                bestDepth = depth;
                best = tp;
            }
        }
        return best;
    }
}
