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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.internal.Record;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BatchBufferSourceTest {

    @Test
    public void testAcquireRecyclesOnlyExactCapacity() {
        BatchBufferSource source = new BatchBufferSource(256, 4);
        ByteBuffer buffer = source.acquire();
        assertEquals(256, buffer.capacity());

        source.recycle(buffer);
        assertEquals(1, source.pooledBuffers());
        assertSame(buffer, source.acquire(), "recycled buffer must be reused");
        assertEquals(0, source.pooledBuffers());

        source.recycle(ByteBuffer.allocate(1024)); // expanded/backpressure-sized: rejected
        assertEquals(0, source.pooledBuffers());
    }

    @Test
    public void testPoolIsBounded() {
        BatchBufferSource source = new BatchBufferSource(64, 2);
        source.recycle(ByteBuffer.allocate(64));
        source.recycle(ByteBuffer.allocate(64));
        source.recycle(ByteBuffer.allocate(64));
        assertEquals(2, source.pooledBuffers(), "pool must not exceed its bound");
    }

    /** End-to-end through the accumulator: success recycles, failure does not. */
    @Test
    public void testAccumulatorReusesBufferAfterSuccessOnly() {
        Node node = new Node(1, "localhost", 9092);
        Cluster cluster = new Cluster("test", List.of(node),
            List.of(new PartitionInfo("t", 0, node, new Node[]{node}, new Node[]{node})),
            Set.of(), Set.of());
        ClientSettings client = ClientSettings.newBuilder("localhost:9092").build();
        ProducerSettings settings = ProducerSettings.newBuilder(client)
            .batchSize(512).linger(Duration.ZERO).build();
        RecordAccumulatorV2 accumulator = new RecordAccumulatorV2(settings,
            new MemoryLimiter(1 << 20), BatchSealer.NO_OP, BackpressureSignal.NEVER);
        PartitionAssignment fixed = new PartitionAssignment.Fixed(0);
        Header[] noHeaders = new Header[0];
        byte[] value = new byte[64];

        accumulator.append("t", fixed, 0L, null, value, noHeaders, 0L);
        BatchV2 first = accumulator.drain(cluster, 1L).get(node).get(0);
        ByteBuffer firstBuffer = first.backingBuffer();
        byte[] firstBytes = new byte[first.records().sizeInBytes()];
        first.records().buffer().duplicate().get(firstBytes);
        first.completeSuccessfully(0L, -1L);

        // Next batch must reuse the recycled buffer, and its contents must round-trip
        // correctly even though it overwrites the previous batch's storage.
        accumulator.append("t", fixed, 0L, null, value, noHeaders, 0L);
        BatchV2 second = accumulator.drain(cluster, 1L).get(node).get(0);
        assertSame(firstBuffer, second.backingBuffer(), "successful batch's buffer is reused");
        int count = 0;
        for (Record record : second.records().records()) {
            assertEquals(value.length, record.valueSize());
            count++;
        }
        assertEquals(1, count);
        second.completeExceptionally(new RuntimeException("boom"));

        // The failed batch's buffer must NOT come back.
        accumulator.append("t", fixed, 0L, null, value, noHeaders, 0L);
        BatchV2 third = accumulator.drain(cluster, 1L).get(node).get(0);
        assertNotSame(firstBuffer, third.backingBuffer(), "failed batch's buffer is dropped");
        assertTrue(third.records().sizeInBytes() > 0);
    }
}
