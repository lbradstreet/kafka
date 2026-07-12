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
package org.apache.kafka.clients.v2;

import org.apache.kafka.clients.v2.admin.AsyncAdmin;
import org.apache.kafka.clients.v2.consumer.AsyncConsumer;
import org.apache.kafka.clients.v2.consumer.ConsumerRecordV2;
import org.apache.kafka.clients.v2.consumer.ConsumerSettings;
import org.apache.kafka.clients.v2.consumer.DefaultAsyncConsumer;
import org.apache.kafka.clients.v2.producer.AsyncProducer;
import org.apache.kafka.clients.v2.producer.DefaultAsyncProducer;
import org.apache.kafka.clients.v2.producer.ProducerRecordV2;
import org.apache.kafka.clients.v2.producer.ProducerSettings;
import org.apache.kafka.clients.v2.producer.RecordMetadataV2;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test of the v2 stack against a real broker.
 *
 * <p>Enabled only when {@code KAFKA_V2_IT_BOOTSTRAP} points at a running cluster, e.g.
 * {@code KAFKA_V2_IT_BOOTSTRAP=localhost:9092 ./gradlew :clients-v2:test --tests '*V2ClientEndToEnd*'}.
 */
@EnabledIfEnvironmentVariable(named = "KAFKA_V2_IT_BOOTSTRAP", matches = ".+")
public class V2ClientEndToEndTest {

    private static KafkaClientRuntime runtime;
    private static ClientSettings clientSettings;

    @BeforeAll
    public static void setUp() {
        runtime = KafkaClientRuntime.create();
        clientSettings = ClientSettings.newBuilder(System.getenv("KAFKA_V2_IT_BOOTSTRAP"))
            .clientId("v2-e2e")
            .requestTimeout(Duration.ofSeconds(20))
            .build();
    }

    @AfterAll
    public static void tearDown() {
        runtime.close();
    }

    @Test
    public void testAdminProduceConsumeRoundTrip() throws Exception {
        String topic = "v2-e2e-" + System.nanoTime();
        int records = 500;

        try (AsyncAdmin admin = new AsyncAdmin(runtime, clientSettings)) {
            var description = admin.describeCluster().get(30, TimeUnit.SECONDS);
            assertNotNull(description.clusterId());
            assertTrue(description.nodes().size() >= 1);
            admin.createTopic(topic, 3, (short) 1).get(30, TimeUnit.SECONDS);
        }

        // Produce keyed (Fixed assignment) and unkeyed (Deferred, late-bound) records,
        // compressed with lz4, and verify every future completes with a real offset.
        ProducerSettings producerSettings = ProducerSettings.newBuilder(clientSettings)
            .compression(Compression.lz4().build())
            .linger(Duration.ofMillis(5))
            .build();
        Map<String, String> sent = new HashMap<>();
        try (AsyncProducer<String, String> producer = new DefaultAsyncProducer<>(
            runtime, producerSettings, new StringSerializer(), new StringSerializer())) {
            List<CompletableFuture<RecordMetadataV2>> futures = new ArrayList<>();
            for (int i = 0; i < records; i++) {
                String key = i % 2 == 0 ? "key-" + i : null; // half keyed, half late-bound
                String value = "value-" + i;
                sent.put("value-" + i, key);
                futures.add(producer.send(new ProducerRecordV2<>(topic,
                    java.util.OptionalInt.empty(), key, value, java.util.OptionalLong.empty(),
                    List.of())));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .get(60, TimeUnit.SECONDS);
            Set<TopicPartition> partitionsUsed = new HashSet<>();
            for (CompletableFuture<RecordMetadataV2> future : futures) {
                RecordMetadataV2 metadata = future.get();
                assertTrue(metadata.offset() >= 0);
                partitionsUsed.add(metadata.topicPartition());
            }
            assertTrue(partitionsUsed.size() > 1,
                "late binding + keyed hashing should spread across partitions, used: " + partitionsUsed);
        }

        // Consume everything back and compare values.
        ConsumerSettings consumerSettings = ConsumerSettings.of(clientSettings);
        try (AsyncConsumer<String, String> consumer = new DefaultAsyncConsumer<>(
            runtime, consumerSettings, new StringDeserializer(), new StringDeserializer())) {
            List<TopicPartition> partitions = List.of(
                new TopicPartition(topic, 0), new TopicPartition(topic, 1), new TopicPartition(topic, 2));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions).get(30, TimeUnit.SECONDS);

            Map<String, String> consumed = new HashMap<>();
            long deadline = System.currentTimeMillis() + 60_000;
            while (consumed.size() < records && System.currentTimeMillis() < deadline) {
                List<ConsumerRecordV2<String, String>> polled = consumer.poll().get(30, TimeUnit.SECONDS);
                for (ConsumerRecordV2<String, String> record : polled)
                    consumed.put(record.value(), record.key());
            }
            assertEquals(records, consumed.size(), "must consume every produced record");
            assertEquals(sent, consumed, "keys and values must round-trip exactly");
        }
    }
}
