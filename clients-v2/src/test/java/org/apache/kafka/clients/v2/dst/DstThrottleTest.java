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
package org.apache.kafka.clients.v2.dst;

import org.apache.kafka.clients.v2.producer.AsyncProducer;
import org.apache.kafka.clients.v2.producer.DefaultAsyncProducer;
import org.apache.kafka.clients.v2.producer.ProducerRecordV2;
import org.apache.kafka.clients.v2.producer.ProducerSettings;
import org.apache.kafka.clients.v2.producer.RecordMetadataV2;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.network.netty.BrokerTimingModel;
import org.apache.kafka.network.netty.FaultInjector.FaultProfile;
import org.apache.kafka.network.netty.ThrottleModel;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Deterministic simulation of broker quota throttling (KIP-219): the broker stamps
 * {@code throttle_time_ms} into produce responses, and the v2 transport honors it by pausing
 * writes on the connection for the window (a throttled connection is treated as backpressured,
 * so batching adapts). Verifies delivery stays intact and the client responds to throttling.
 */
public class DstThrottleTest {

    private static final String TOPIC = "throttle-topic";

    /**
     * A broker that throttles every produce response by 40 ms: all records are still
     * delivered (no timeouts), the client observes the throttling, and — because a throttled
     * connection is backpressured — batches grow, yielding fewer, larger requests than an
     * unthrottled control.
     */
    @Test
    public void testClientHonorsThrottleAndKeepsDelivering() {
        Result throttled = run(ThrottleModel.constant(40), 16 * 512);
        Result unthrottled = run(ThrottleModel.NONE, 16 * 512);

        assertEquals(600, throttled.stored, "throttling must not drop records");
        assertEquals(600, unthrottled.stored);
        assertTrue(throttled.throttledResponses > 0, "the broker should have throttled produce");
        assertEquals(40, throttled.maxThrottleMs);
        // The client paused under throttle, so batches packed more records into fewer requests.
        assertTrue(throttled.produceRequests < unthrottled.produceRequests,
            "throttle-induced backpressure should mean fewer requests: throttled="
                + throttled.produceRequests + " unthrottled=" + unthrottled.produceRequests);
        assertTrue(throttled.maxRecordsPerRequest > unthrottled.maxRecordsPerRequest,
            "throttle-induced backpressure should pack larger batches: throttled="
                + throttled.maxRecordsPerRequest + " unthrottled=" + unthrottled.maxRecordsPerRequest);
    }

    /** Throttling that begins partway through is absorbed without loss or hang. */
    @Test
    public void testThrottleStartingMidStreamIsAbsorbed() {
        Result r = run(ThrottleModel.after(5, 30), 16 * 512);
        assertEquals(600, r.stored, "every record delivered despite mid-stream throttling");
        assertTrue(r.throttledResponses > 0);
        assertEquals(30, r.maxThrottleMs);
    }

    /** A throttled run is still a pure function of its seed. */
    @Test
    public void testThrottledRunIsDeterministic() {
        assertEquals(traceOf(), traceOf(), "same scenario + seed must yield an identical trace");
    }

    private List<String> traceOf() {
        try (DstHarness harness = new DstHarness(5, FaultProfile.NONE, 1,
            brokerId -> BrokerTimingModel.INSTANT, brokerId -> ThrottleModel.after(3, 25))) {
            drive(harness, 4096, 150, 3);
            return harness.traceEvents();
        }
    }

    private record Result(int produceRequests, int maxRecordsPerRequest, int throttledResponses,
                          int maxThrottleMs, int stored) { }

    private Result run(ThrottleModel throttle, int backpressureBatchSize) {
        try (DstHarness harness = new DstHarness(1, FaultProfile.NONE, 1,
            brokerId -> BrokerTimingModel.INSTANT, brokerId -> throttle)) {
            drive(harness, backpressureBatchSize, 600, 6);
            return new Result(harness.observer.produceRequests(),
                harness.observer.maxRecordsPerRequest(), harness.observer.throttledResponses(),
                harness.observer.maxThrottleMs(),
                harness.cluster.log(new TopicPartition(TOPIC, 0)).size());
        }
    }

    /** Steady production to one partition, faster than the (throttled) pipe drains. */
    private void drive(DstHarness harness, int backpressureBatchSize, int records, int perMs) {
        harness.cluster.createTopic(TOPIC, 1);
        ProducerSettings settings = ProducerSettings
            .newBuilder(harness.clientSettings().maxInFlight(5).build())
            .batchSize(512)
            .backpressureBatchSize(backpressureBatchSize)
            .maxRequestSize(1024 * 1024)
            .linger(Duration.ofMillis(1))
            .build();
        AsyncProducer<String, String> producer = new DefaultAsyncProducer<>(
            harness.runtime, settings, new StringSerializer(), new StringSerializer());

        List<CompletableFuture<RecordMetadataV2>> futures = new ArrayList<>();
        for (int i = 0; i < records; i++) {
            final int idx = i;
            harness.scheduler.schedule(() -> {
                futures.add(producer.send(new ProducerRecordV2<>(TOPIC, java.util.OptionalInt.of(0),
                    null, "v-" + idx, java.util.OptionalLong.empty(), List.of())));
            }, i / perMs, TimeUnit.MILLISECONDS);
        }
        harness.runUntil(() -> futures.size() == records
            && futures.stream().allMatch(CompletableFuture::isDone));
        harness.runUntil(producer.closeAsync()::isDone);
        futures.forEach(f -> assertTrue(!f.isCompletedExceptionally(), "no failures expected"));
    }
}
