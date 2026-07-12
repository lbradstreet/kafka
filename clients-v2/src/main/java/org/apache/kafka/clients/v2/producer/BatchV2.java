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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.RecordBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A producer batch: an in-progress record batch plus the per-record futures to complete.
 *
 * <p>Lifecycle: {@code OPEN} (appendable, and re-bindable if it originated from a
 * {@code Deferred} assignment, D14) &rarr; {@code SEALED} (drained: contents and partition
 * immutable — the D15 seam where idempotent sequences will later be stamped) &rarr; done.
 *
 * <p>Not internally synchronized for appends: the accumulator serializes appends via its
 * per-queue locks. Completion may come from any thread.
 */
public final class BatchV2 {

    private final MemoryRecordsBuilder builder;
    private final List<CompletableFuture<RecordMetadataV2>> futures = new ArrayList<>();
    private final List<Long> timestamps = new ArrayList<>();
    private final CompletableFuture<Void> done = new CompletableFuture<>();
    private final long createdMs;
    private final int memoryCharged;

    private volatile TopicPartition partition; // null while unbound (D14)
    private volatile boolean sealed = false;
    private volatile MemoryRecords records;
    private final java.util.concurrent.atomic.AtomicInteger attempts =
        new java.util.concurrent.atomic.AtomicInteger();

    /**
     * @param initialCapacity  buffer pre-allocation, normally {@code batch.size}
     * @param hardLimitBytes   absolute write limit, normally {@code backpressure.batch.size};
     *                         appends beyond it are refused regardless of backpressure state
     */
    BatchV2(TopicPartition partition, int initialCapacity, int hardLimitBytes,
            Compression compression, long nowMs, int memoryCharged) {
        this.partition = partition;
        this.createdMs = nowMs;
        this.memoryCharged = memoryCharged;
        this.builder = MemoryRecords.builder(ByteBuffer.allocate(initialCapacity), compression,
            TimestampType.CREATE_TIME, 0L, hardLimitBytes);
    }

    /**
     * @param softLimitBytes the currently applicable size target: {@code batch.size} when the
     *                       destination can send right away, {@code backpressure.batch.size}
     *                       while its in-flight window is saturated
     */
    boolean hasRoomFor(long timestamp, byte[] key, byte[] value, Header[] headers, int softLimitBytes) {
        if (sealed || !builder.hasRoomFor(timestamp, key, value, headers))
            return false;
        return builder.estimatedSizeInBytes() < softLimitBytes;
    }

    CompletableFuture<RecordMetadataV2> append(long timestamp, byte[] key, byte[] value, Header[] headers) {
        builder.append(timestamp, key, value, headers);
        timestamps.add(timestamp);
        CompletableFuture<RecordMetadataV2> future = new CompletableFuture<>();
        futures.add(future);
        return future;
    }

    /** Full against the hard (backpressure) limit. */
    boolean isFull() {
        return sealed || builder.isFull();
    }

    int estimatedSizeInBytes() {
        return builder.estimatedSizeInBytes();
    }

    boolean isEmpty() {
        return futures.isEmpty();
    }

    long createdMs() {
        return createdMs;
    }

    int memoryCharged() {
        return memoryCharged;
    }

    int recordCount() {
        return futures.size();
    }

    int attempts() {
        return attempts.get();
    }

    void incrementAttempts() {
        attempts.incrementAndGet();
    }

    TopicPartition partition() {
        return partition;
    }

    boolean isSealed() {
        return sealed;
    }

    /**
     * Re-bind an unbound (Deferred) batch to a partition. Only legal before sealing (D14).
     */
    void bind(TopicPartition target) {
        if (sealed)
            throw new IllegalStateException("Cannot re-bind a sealed batch (bound to " + partition + ")");
        this.partition = target;
    }

    /**
     * Close the batch for appends and freeze its partition. From this point the batch is
     * immutable; this is where producer state (id/epoch/sequence) will be stamped once
     * idempotence lands (D15).
     */
    void seal() {
        if (sealed)
            return;
        if (partition == null)
            throw new IllegalStateException("Cannot seal an unbound batch");
        sealed = true;
        records = builder.build();
    }

    MemoryRecords records() {
        if (!sealed)
            throw new IllegalStateException("Batch not sealed");
        return records;
    }

    /** Completes every record future with its final offset and timestamp. */
    void completeSuccessfully(long baseOffset, long logAppendTime) {
        for (int i = 0; i < futures.size(); i++) {
            long timestamp = logAppendTime != RecordBatch.NO_TIMESTAMP ? logAppendTime : timestamps.get(i);
            futures.get(i).complete(new RecordMetadataV2(partition, baseOffset + i, timestamp));
        }
        done.complete(null);
    }

    void completeExceptionally(Throwable error) {
        for (CompletableFuture<RecordMetadataV2> future : futures)
            future.completeExceptionally(error);
        done.complete(null);
    }

    /** Completes when every record in the batch has an outcome (used by flush/close). */
    CompletableFuture<Void> doneFuture() {
        return done;
    }
}
