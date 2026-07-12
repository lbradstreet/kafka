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
public final class RecordAccumulatorV2 {

    private final ProducerSettings settings;
    private final MemoryLimiter limiter;
    private final BatchSealer sealer;
    private final BackpressureSignal backpressure;
    private final BatchBufferSource buffers;
    private final org.apache.kafka.common.compress.Compression compression;

    private final ConcurrentMap<TopicPartition, Deque<BatchV2>> bound = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Deque<BatchV2>> unbound = new ConcurrentHashMap<>();
    private final Set<BatchV2> incomplete = ConcurrentHashMap.newKeySet();
    private volatile boolean flushRequested = false;

    public RecordAccumulatorV2(ProducerSettings settings, MemoryLimiter limiter, BatchSealer sealer,
                        BackpressureSignal backpressure) {
        this.settings = settings;
        this.limiter = limiter;
        this.sealer = sealer;
        this.backpressure = backpressure;
        this.buffers = new BatchBufferSource(settings.batchSize(),
            (int) Math.min(1024, Math.max(2, settings.bufferMemory() / settings.batchSize())));
        // Pool per-batch compression workspaces (lz4's two ~64KB arrays) across batches (D12).
        this.compression = WorkspacePooledCompression.wrapIfPoolable(settings.compression());
    }

    /**
     * Append to a bound (Fixed) or unbound (Deferred) queue.
     *
     * <p>Adaptive sealing: a batch normally stops accepting records at {@code batch.size},
     * but while the destination's in-flight window is saturated it keeps growing up to
     * {@code backpressure.batch.size} — it cannot be sent yet anyway, and one large request
     * beats several small ones once the window opens.
     */
    public CompletableFuture<RecordMetadataV2> append(String topic, PartitionAssignment assignment,
                                               long timestamp, byte[] key, byte[] value,
                                               Header[] headers, long nowMs) {
        Deque<BatchV2> queue;
        TopicPartition partition;
        boolean saturated;
        if (assignment instanceof PartitionAssignment.Fixed fixed) {
            partition = new TopicPartition(topic, fixed.partition());
            queue = bound.computeIfAbsent(partition, tp -> new ArrayDeque<>());
            saturated = backpressure.isSaturated(partition);
        } else {
            partition = null;
            queue = unbound.computeIfAbsent(topic, t -> new ArrayDeque<>());
            saturated = backpressure.isTopicSaturated(topic);
        }
        int softLimit = saturated ? settings.backpressureBatchSize() : settings.batchSize();
        int recordUpperBound = recordSizeUpperBound(key, value, headers);
        synchronized (queue) {
            BatchV2 last = queue.peekLast();
            if (last != null && last.hasRoomFor(timestamp, key, value, headers, softLimit)
                && fundGrowth(last, recordUpperBound))
                return last.append(timestamp, key, value, headers);

            // A batch born under saturation will (soft-limit permitting) grow to the
            // backpressure size anyway — allocate it full-size up front to avoid a chain of
            // 1.1x realloc-copies, budget permitting. Otherwise a pooled batch.size buffer.
            java.nio.ByteBuffer buffer;
            int initialCharge;
            int backpressureCharge = Math.max(settings.backpressureBatchSize(), recordUpperBound);
            if (saturated && limiter.tryAcquire(backpressureCharge)) {
                initialCharge = backpressureCharge;
                buffer = java.nio.ByteBuffer.allocate(settings.backpressureBatchSize());
            } else {
                initialCharge = Math.max(settings.batchSize(), recordUpperBound);
                limiter.acquireNow(initialCharge);
                buffer = buffers.acquire();
            }
            BatchV2 batch = new BatchV2(partition, topic, buffer,
                settings.backpressureBatchSize(), compression, nowMs, initialCharge);
            incomplete.add(batch);
            batch.doneFuture().whenComplete((v, e) -> {
                incomplete.remove(batch);
                limiter.release(batch.memoryCharged());
                // Reuse is only safe after success (see BatchBufferSource ownership rule);
                // odd-sized (backpressure/expanded) buffers are rejected by the pool itself.
                if (batch.succeeded())
                    buffers.recycle(batch.backingBuffer());
            });
            queue.addLast(batch);
            return batch.append(timestamp, key, value, headers);
        }
    }

    /**
     * Ensure the batch's budget charge covers the append about to happen. Growth past the
     * initial {@code batch.size} charge (backpressure sealing) is funded incrementally; if
     * the budget cannot cover it the batch simply stops growing — the caller falls through
     * to a new batch, whose own charge applies normal fail-fast semantics.
     */
    private boolean fundGrowth(BatchV2 batch, int recordUpperBound) {
        int shortfall = batch.estimatedSizeInBytes() + recordUpperBound - batch.memoryCharged();
        if (shortfall <= 0)
            return true;
        // Round top-ups up to reduce limiter traffic on hot paths.
        int topUp = Math.max(shortfall, settings.batchSize() / 4);
        if (!limiter.tryAcquire(topUp))
            return false;
        batch.addMemoryCharge(topUp);
        return true;
    }

    /** Conservative per-record budget estimate (varint framing overestimated, never under). */
    private static int recordSizeUpperBound(byte[] key, byte[] value, Header[] headers) {
        int size = 64;
        if (key != null)
            size += key.length;
        if (value != null)
            size += value.length;
        for (Header header : headers) {
            size += 16 + 4 * header.key().length();
            if (header.value() != null)
                size += header.value().length;
        }
        return size;
    }

    public Set<String> topics() {
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
     *
     * <p>Partitions whose leader is saturated (in-flight window full) are skipped
     * <em>without sealing</em>: their open batch keeps accepting records up to
     * {@code backpressure.batch.size} until the window opens.
     */
    public Map<Node, List<BatchV2>> drain(Cluster cluster, long nowMs) {
        bindReadyUnboundBatches(cluster, nowMs);

        Map<Node, List<BatchV2>> drained = new HashMap<>();
        Map<Node, Integer> nodeBytes = new HashMap<>();
        for (Map.Entry<TopicPartition, Deque<BatchV2>> entry : bound.entrySet()) {
            Node leader = cluster.leaderFor(entry.getKey());
            if (leader == null || leader.isEmpty())
                continue; // metadata refresh will find the new leader
            if (backpressure.isSaturated(entry.getKey()))
                continue; // can't send anyway; let the open batch keep batching
            Deque<BatchV2> queue = entry.getValue();
            synchronized (queue) {
                // At most ONE batch per partition per request: a ProduceRequest carries a single
                // records blob per partition, and response completion is keyed by partition.
                BatchV2 batch = queue.peekFirst();
                if (batch == null || !isDrainable(batch, nowMs))
                    continue;
                int size = batch.isSealed()
                    ? batch.records().sizeInBytes()
                    : settings.batchSize();
                int used = nodeBytes.getOrDefault(leader, 0);
                if (used > 0 && used + size > settings.maxRequestSize())
                    continue;
                queue.pollFirst();
                if (!batch.isSealed()) {
                    batch.seal();
                    sealer.seal(batch, batch.partition());
                }
                nodeBytes.merge(leader, batch.records().sizeInBytes(), Integer::sum);
                drained.computeIfAbsent(leader, n -> new ArrayList<>()).add(batch);
            }
        }
        return drained;
    }

    private boolean isDrainable(BatchV2 batch, long nowMs) {
        if (batch.isEmpty())
            return false;
        return batch.isSealed() // a retry re-enqueue
            || batch.isFull() // hard (backpressure.batch.size) limit
            || batch.estimatedSizeInBytes() >= settings.batchSize() // normal target reached
            || flushRequested
            || nowMs - batch.createdMs() >= settings.linger().toMillis();
    }

    /**
     * Late binding (D14): assign drainable unbound batches to the shortest bound queue among
     * partitions whose leader is currently available <em>and not saturated</em>. If every
     * leader is saturated the batch stays unbound and keeps batching (backpressure sealing).
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
                    if (target == null)
                        break; // all leaders saturated; can't send anyway, keep batching
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
            if (backpressure.isSaturated(tp))
                continue;
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
