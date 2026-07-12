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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Buffer <em>reuse</em> for batch buffers — the other half of the D9 split from the
 * {@link MemoryLimiter} budget. A bounded free-list of exactly {@code batch.size}-capacity
 * heap buffers; anything else (backpressure-sized allocations, buffers grown by
 * {@code expandBuffer}) is simply left to GC, like the classic {@code BufferPool}'s
 * non-poolable allocations.
 *
 * <p><b>Ownership rule:</b> a built {@code MemoryRecords} shares the backing array of its
 * batch buffer, and on failure paths a frame wrapping that array may still sit in a closing
 * channel. Callers must therefore recycle only after a batch's <em>successful</em>
 * completion (response received &rArr; the request was fully written), and never touch the
 * {@code MemoryRecords} afterwards.
 *
 * <p>Deterministic (no time, no randomness) so simulation runs are unaffected (D13).
 */
final class BatchBufferSource {

    private final int bufferSize;
    private final int maxPooled;
    private final ConcurrentLinkedDeque<ByteBuffer> free = new ConcurrentLinkedDeque<>();
    private final AtomicInteger pooled = new AtomicInteger();

    BatchBufferSource(int bufferSize, int maxPooled) {
        this.bufferSize = bufferSize;
        this.maxPooled = Math.max(1, maxPooled);
    }

    /** A cleared {@code bufferSize}-capacity buffer, recycled when possible. */
    ByteBuffer acquire() {
        ByteBuffer buffer = free.pollFirst();
        if (buffer != null) {
            pooled.decrementAndGet();
            buffer.clear();
            return buffer;
        }
        return ByteBuffer.allocate(bufferSize);
    }

    /** Return a buffer; ignored unless its capacity matches exactly and the pool has room. */
    void recycle(ByteBuffer buffer) {
        if (buffer == null || buffer.capacity() != bufferSize)
            return;
        if (pooled.incrementAndGet() > maxPooled) {
            pooled.decrementAndGet();
            return;
        }
        buffer.clear();
        free.addFirst(buffer);
    }

    int pooledBuffers() {
        return pooled.get();
    }
}
