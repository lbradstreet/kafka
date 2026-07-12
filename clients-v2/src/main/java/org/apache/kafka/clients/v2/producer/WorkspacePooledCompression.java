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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.compress.Lz4Compression;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.internals.BufferSupplier;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Wraps a {@link Compression} so its per-batch compression <em>workspace</em> buffers are
 * pooled across batches (design decision D12). Today this benefits lz4, whose block output
 * stream otherwise allocates two ~64&nbsp;KB arrays for every batch; zstd already recycles
 * internally, and gzip's {@code Deflater} lifecycle is future work. Codecs without an
 * output-side workspace hook pass through untouched.
 *
 * <p>The supplier is shared by every batch of a producer and appends run on caller threads,
 * so unlike {@code BufferSupplier.create()} the embedded supplier is thread-safe (and
 * bounded). Deterministic — no time, no randomness (D13).
 */
final class WorkspacePooledCompression implements Compression {

    private final Compression delegate;
    private final BufferSupplier workspace = new ConcurrentBufferSupplier(16);

    private WorkspacePooledCompression(Compression delegate) {
        this.delegate = delegate;
    }

    /** Wraps codecs with a poolable output workspace; returns others unchanged. */
    static Compression wrapIfPoolable(Compression compression) {
        return compression instanceof Lz4Compression
            ? new WorkspacePooledCompression(compression)
            : compression;
    }

    @Override
    public CompressionType type() {
        return delegate.type();
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream bufferStream, byte messageVersion) {
        if (delegate instanceof Lz4Compression lz4)
            return lz4.wrapForOutput(bufferStream, messageVersion, workspace);
        return delegate.wrapForOutput(bufferStream, messageVersion);
    }

    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion,
                                    BufferSupplier decompressionBufferSupplier) {
        return delegate.wrapForInput(buffer, messageVersion, decompressionBufferSupplier);
    }

    @Override
    public int decompressionOutputSize() {
        return delegate.decompressionOutputSize();
    }

    /** Thread-safe, per-capacity-bounded {@link BufferSupplier}. */
    private static final class ConcurrentBufferSupplier extends BufferSupplier {
        private final int maxPerCapacity;
        private final ConcurrentMap<Integer, ConcurrentLinkedDeque<ByteBuffer>> free =
            new ConcurrentHashMap<>();
        private final ConcurrentMap<Integer, AtomicInteger> counts = new ConcurrentHashMap<>();

        ConcurrentBufferSupplier(int maxPerCapacity) {
            this.maxPerCapacity = maxPerCapacity;
        }

        @Override
        public ByteBuffer get(int capacity) {
            ConcurrentLinkedDeque<ByteBuffer> queue = free.get(capacity);
            ByteBuffer buffer = queue == null ? null : queue.pollFirst();
            if (buffer != null) {
                counts.get(capacity).decrementAndGet();
                buffer.clear();
                return buffer;
            }
            return ByteBuffer.allocate(capacity);
        }

        @Override
        public void release(ByteBuffer buffer) {
            int capacity = buffer.capacity();
            AtomicInteger count = counts.computeIfAbsent(capacity, c -> new AtomicInteger());
            if (count.incrementAndGet() > maxPerCapacity) {
                count.decrementAndGet();
                return;
            }
            buffer.clear();
            free.computeIfAbsent(capacity, c -> new ConcurrentLinkedDeque<>()).addFirst(buffer);
        }

        @Override
        public void close() {
            free.clear();
            counts.clear();
        }
    }
}
