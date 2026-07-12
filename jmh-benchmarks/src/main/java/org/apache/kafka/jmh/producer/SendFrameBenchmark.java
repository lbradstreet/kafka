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
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.network.netty.ByteBufSinkChannel;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * The transport's request-to-wire conversion: serialize a produce request (SendBuilder)
 * and drain the {@code Send} into a Netty buffer via {@code ByteBufSinkChannel}. The record
 * payload must be wrapped by reference, not copied — with {@code -prof gc} the norm
 * allocation should stay near the small header scratch buffer regardless of payload size.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class SendFrameBenchmark {

    private static final int BATCH_SIZE = 64 * 1024;

    private ProduceRequest request;
    private RequestHeader header;

    @Setup
    public void setup() {
        byte[] key = "benchmark-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[1024];
        MemoryRecordsBuilder builder = MemoryRecords.builder(ByteBuffer.allocate(BATCH_SIZE),
            Compression.NONE, TimestampType.CREATE_TIME, 0L, BATCH_SIZE);
        while (builder.hasRoomFor(0L, key, value, Record.EMPTY_HEADERS))
            builder.append(0L, key, value, Record.EMPTY_HEADERS);
        MemoryRecords records = builder.build();

        ProduceRequestData.TopicProduceDataCollection topics =
            new ProduceRequestData.TopicProduceDataCollection();
        topics.add(new ProduceRequestData.TopicProduceData()
            .setName("send-frame-benchmark")
            .setPartitionData(java.util.List.of(new ProduceRequestData.PartitionProduceData()
                .setIndex(0)
                .setRecords(records))));
        ProduceRequest.Builder requestBuilder = ProduceRequest.builder(new ProduceRequestData()
            .setAcks((short) -1)
            .setTimeoutMs(30_000)
            .setTopicData(topics));
        short version = requestBuilder.latestAllowedVersion();
        request = requestBuilder.build(version);
        header = new RequestHeader(new RequestHeaderData()
            .setRequestApiKey(ApiKeys.PRODUCE.id)
            .setRequestApiVersion(version)
            .setClientId("bench")
            .setCorrelationId(1),
            ApiKeys.PRODUCE.requestHeaderVersion(version));
    }

    @Benchmark
    public int serializeAndFrame() throws IOException {
        Send send = request.toSend(header);
        ByteBuf frame = ByteBufSinkChannel.drain(send, PooledByteBufAllocator.DEFAULT);
        int bytes = frame.readableBytes();
        frame.release();
        return bytes;
    }
}
