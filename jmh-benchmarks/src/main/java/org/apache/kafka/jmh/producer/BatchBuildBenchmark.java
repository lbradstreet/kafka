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
package org.apache.kafka.jmh.producer;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * The isolated batch lifecycle — allocate/reuse buffer, append to full, build — so each
 * hot-path optimization (buffer pooling, compression-ratio seeding) can be quantified on
 * its own. Existing record benchmarks build batches only in {@code @Setup}; here the build
 * is the measured operation. Run with {@code -prof gc}.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class BatchBuildBenchmark {

    private static final String TOPIC = "batch-build-benchmark";
    private static final int BATCH_SIZE = 16 * 1024;

    @Param({"NONE", "LZ4", "ZSTD", "GZIP"})
    public String compressionType;

    /** Fixed record count so every op does identical work regardless of codec/estimates. */
    private static final int RECORDS_PER_BATCH = 24;

    /** true = reuse one buffer across builds (pooling); false = fresh allocation per batch. */
    @Param({"false", "true"})
    public boolean reuseBuffer;

    private Compression compression;
    private CompressionType type;
    private ByteBuffer reusableBuffer;
    private byte[] key;
    private byte[] value;

    @Setup
    public void setup() {
        type = CompressionType.forName(compressionType.toLowerCase(java.util.Locale.ROOT));
        compression = Compression.of(type).build();
        reusableBuffer = ByteBuffer.allocate(BATCH_SIZE);
        key = "benchmark-key".getBytes(StandardCharsets.UTF_8);
        value = new byte[512];
        // Mixed-entropy payload: half text-like, half random, so codecs do real work.
        new Random(42).nextBytes(value);
        for (int i = 0; i < value.length / 2; i++)
            value[i] = (byte) ('a' + (i % 26));
    }

    @Benchmark
    public MemoryRecords buildBatch() {
        ByteBuffer buffer;
        if (reuseBuffer) {
            reusableBuffer.clear();
            buffer = reusableBuffer;
        } else {
            buffer = ByteBuffer.allocate(BATCH_SIZE);
        }
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, compression,
            TimestampType.CREATE_TIME, 0L, BATCH_SIZE);
        for (int i = 0; i < RECORDS_PER_BATCH; i++)
            builder.append(0L, key, value, Record.EMPTY_HEADERS);
        MemoryRecords records = builder.build();
        // The reused buffer may have been replaced by expandBuffer; keep the live one so the
        // "pooled" variant reflects steady-state reuse of a right-sized buffer.
        if (reuseBuffer && builder.buffer().capacity() >= BATCH_SIZE)
            reusableBuffer = builder.buffer();
        return records;
    }
}
