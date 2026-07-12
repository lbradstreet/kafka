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

import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.ProducerBatch;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.v2.ClientSettings;
import org.apache.kafka.clients.v2.producer.BackpressureSignal;
import org.apache.kafka.clients.v2.producer.BatchSealer;
import org.apache.kafka.clients.v2.producer.BatchV2;
import org.apache.kafka.clients.v2.producer.MemoryLimiter;
import org.apache.kafka.clients.v2.producer.PartitionAssignment;
import org.apache.kafka.clients.v2.producer.ProducerSettings;
import org.apache.kafka.clients.v2.producer.RecordAccumulatorV2;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Head-to-head cost of the producer accumulation hot path — append, drain, complete —
 * classic {@code RecordAccumulator}+{@code BufferPool} vs v2
 * {@code RecordAccumulatorV2}+{@code MemoryLimiter}.
 *
 * <p>Run with {@code -prof gc} and compare {@code gc.alloc.rate.norm} (bytes allocated per
 * appended record): the stable metric across environments.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class AccumulatorComparisonBenchmark {

    private static final String TOPIC = "accumulator-benchmark";
    private static final int PARTITION = 0;
    private static final int RECORDS_PER_INVOCATION = 10_000;
    private static final int BATCH_SIZE = 16 * 1024;
    private static final long TOTAL_MEMORY = 256L * 1024 * 1024;
    private static final PartitionAssignment FIXED = new PartitionAssignment.Fixed(PARTITION);

    @Param({"NONE", "LZ4", "ZSTD"})
    public String compressionType;

    @Param({"100", "1000"})
    public int valueSize;

    private final MockTime time = new MockTime();
    private Metrics metrics;
    private Node node;
    private Cluster cluster;
    private MetadataSnapshot metadataSnapshot;

    private RecordAccumulator classicAccumulator;
    private RecordAccumulatorV2 v2Accumulator;

    private byte[] key;
    private byte[] value;

    @Setup
    public void setup() {
        metrics = new Metrics();
        node = new Node(0, "localhost", 9092);
        TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
        MetadataResponse.PartitionMetadata partitionMetadata = new MetadataResponse.PartitionMetadata(
            Errors.NONE, tp, Optional.of(node.id()), Optional.empty(), null, null, null);
        metadataSnapshot = new MetadataSnapshot(null, Map.of(node.id(), node),
            List.of(partitionMetadata), Set.of(), Set.of(), Set.of(), null, Map.of());
        cluster = metadataSnapshot.cluster();

        Compression compression = Compression
            .of(CompressionType.forName(compressionType.toLowerCase(java.util.Locale.ROOT)))
            .build();

        classicAccumulator = new RecordAccumulator(
            new LogContext(),
            BATCH_SIZE + DefaultRecordBatch.RECORD_BATCH_OVERHEAD,
            compression,
            0,      // lingerMs: drainable immediately
            100L,
            1000L,
            Integer.MAX_VALUE, // deliveryTimeoutMs
            metrics,
            "producer-metrics",
            time,
            null,
            new BufferPool(TOTAL_MEMORY, BATCH_SIZE, metrics, time, "producer-metrics"));

        ClientSettings client = ClientSettings.newBuilder("localhost:9092").build();
        ProducerSettings settings = ProducerSettings.newBuilder(client)
            .batchSize(BATCH_SIZE)
            .compression(compression)
            .bufferMemory(TOTAL_MEMORY)
            .build();
        v2Accumulator = new RecordAccumulatorV2(settings, new MemoryLimiter(TOTAL_MEMORY),
            BatchSealer.NO_OP, BackpressureSignal.NEVER);

        key = "benchmark-key".getBytes(StandardCharsets.UTF_8);
        value = new byte[valueSize];
        new Random(42).nextBytes(value); // incompressible-ish payload, same for both paths
    }

    @TearDown
    public void tearDown() {
        metrics.close();
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS_PER_INVOCATION)
    public void classicAppendDrainComplete(Blackhole bh) throws InterruptedException {
        for (int i = 0; i < RECORDS_PER_INVOCATION; i++)
            classicAccumulator.append(TOPIC, PARTITION, 0L, key, value, Record.EMPTY_HEADERS,
                null, 0L, time.milliseconds(), cluster);
        while (true) {
            Map<Integer, List<ProducerBatch>> drained = classicAccumulator.drain(
                metadataSnapshot, Set.of(node), Integer.MAX_VALUE, time.milliseconds());
            List<ProducerBatch> batches = drained.get(node.id());
            if (batches == null || batches.isEmpty())
                break;
            for (ProducerBatch batch : batches) {
                bh.consume(batch.records().sizeInBytes());
                batch.complete(0L, -1L);
                classicAccumulator.deallocate(batch);
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(RECORDS_PER_INVOCATION)
    public void v2AppendDrainComplete(Blackhole bh) {
        for (int i = 0; i < RECORDS_PER_INVOCATION; i++)
            v2Accumulator.append(TOPIC, FIXED, 0L, key, value, Record.EMPTY_HEADERS,
                time.milliseconds());
        while (true) {
            Map<Node, List<BatchV2>> drained = v2Accumulator.drain(cluster, time.milliseconds());
            if (drained.isEmpty())
                break;
            for (List<BatchV2> batches : drained.values()) {
                for (BatchV2 batch : batches) {
                    bh.consume(batch.records().sizeInBytes());
                    batch.completeSuccessfully(0L, -1L);
                }
            }
        }
    }
}
