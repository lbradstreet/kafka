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

import org.apache.kafka.clients.v2.KafkaClientRuntime;
import org.apache.kafka.clients.v2.MetadataManager;
import org.apache.kafka.clients.v2.NetworkRequestDispatcher;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The drain thread: binds and seals ready batches (D14/D15 seams live in the accumulator),
 * groups them by partition leader, and sends produce requests through the dispatcher.
 *
 * <p>Retriable per-partition errors re-enqueue the sealed batch (it keeps its partition) after
 * the retry backoff, alongside a metadata refresh — mirroring classic Sender behavior minus
 * idempotence bookkeeping.
 */
final class SenderV2 implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(SenderV2.class);
    private static final long IDLE_SLEEP_MS = 5;

    private final KafkaClientRuntime runtime;
    private final ProducerSettings settings;
    private final NetworkRequestDispatcher dispatcher;
    private final MetadataManager metadata;
    private final RecordAccumulatorV2 accumulator;

    private volatile boolean running = true;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    SenderV2(KafkaClientRuntime runtime, ProducerSettings settings,
             NetworkRequestDispatcher dispatcher, MetadataManager metadata,
             RecordAccumulatorV2 accumulator) {
        this.runtime = runtime;
        this.settings = settings;
        this.dispatcher = dispatcher;
        this.metadata = metadata;
        this.accumulator = accumulator;
    }

    @Override
    public void run() {
        log.debug("v2 producer sender started");
        while (running)
            runOnce();
        // Orderly close: keep draining what was accepted before close, bounded in time.
        long deadline = runtime.time().milliseconds() + 2 * settings.client().requestTimeout().toMillis();
        while (!accumulator.isEmpty() && runtime.time().milliseconds() < deadline)
            runOnce();
        shutdownLatch.countDown();
        log.debug("v2 producer sender stopped");
    }

    void initiateClose() {
        accumulator.requestFlush();
        running = false;
    }

    boolean awaitShutdown(long timeoutMs) throws InterruptedException {
        return shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private void runOnce() {
        try {
            accumulator.clearFlushIfDrained();
            Set<String> topics = accumulator.topics();
            if (topics.isEmpty() || accumulator.isEmpty()) {
                sleepQuietly(IDLE_SLEEP_MS);
                return;
            }
            Cluster cluster = metadata.cluster(topics)
                .get(settings.client().requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
            Map<Node, List<BatchV2>> drained = accumulator.drain(cluster, runtime.time().milliseconds());
            if (drained.isEmpty()) {
                sleepQuietly(Math.min(IDLE_SLEEP_MS, Math.max(1, settings.linger().toMillis())));
                return;
            }
            for (Map.Entry<Node, List<BatchV2>> entry : drained.entrySet())
                sendProduceRequest(entry.getKey(), entry.getValue(), cluster);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        } catch (Exception e) {
            log.warn("v2 producer drain iteration failed; backing off", e);
            sleepQuietly(settings.client().retryBackoff().toMillis());
        }
    }

    private void sendProduceRequest(Node node, List<BatchV2> batches, Cluster cluster) {
        // A batch polled out of the accumulator is owned by this method: every exit path —
        // including unexpected exceptions — must complete or re-enqueue it, or its futures hang.
        try {
            doSendProduceRequest(node, batches, cluster);
        } catch (Throwable t) {
            log.warn("Failed to build/dispatch produce request to {}", node, t);
            for (BatchV2 batch : batches)
                retryOrFail(batch, t, t instanceof RetriableException);
        }
    }

    private void doSendProduceRequest(Node node, List<BatchV2> batches, Cluster cluster) {
        Map<TopicPartition, BatchV2> byPartition = new HashMap<>();
        Map<Uuid, String> idToName = new HashMap<>();
        ProduceRequestData.TopicProduceDataCollection topicData =
            new ProduceRequestData.TopicProduceDataCollection();

        for (BatchV2 batch : batches) {
            TopicPartition tp = batch.partition();
            Uuid topicId = cluster.topicId(tp.topic());
            idToName.put(topicId, tp.topic());
            ProduceRequestData.TopicProduceData topic = topicData.find(tp.topic(), topicId);
            if (topic == null) {
                topic = new ProduceRequestData.TopicProduceData()
                    .setName(tp.topic())
                    .setTopicId(topicId);
                topicData.add(topic);
            }
            topic.partitionData().add(new ProduceRequestData.PartitionProduceData()
                .setIndex(tp.partition())
                .setRecords(batch.records()));
            byPartition.put(tp, batch);
        }

        ProduceRequest.Builder request = ProduceRequest.builder(new ProduceRequestData()
            .setAcks(settings.acks())
            .setTimeoutMs((int) settings.client().requestTimeout().toMillis())
            .setTopicData(topicData));

        dispatcher.send(node, request).whenComplete((response, error) -> {
            try {
                if (error != null) {
                    Throwable cause = error instanceof CompletionException && error.getCause() != null
                        ? error.getCause() : error;
                    for (BatchV2 batch : byPartition.values())
                        retryOrFail(batch, cause, cause instanceof RetriableException
                            || cause instanceof org.apache.kafka.common.errors.TimeoutException);
                } else {
                    handleProduceResponse((ProduceResponse) response, byPartition, idToName);
                }
            } catch (Throwable t) {
                // whenComplete would swallow this; fail the remaining batches instead of hanging.
                log.error("Produce response handling failed for node {}", node, t);
                for (BatchV2 batch : byPartition.values())
                    batch.completeExceptionally(t);
            }
        });
    }

    private void handleProduceResponse(ProduceResponse response,
                                       Map<TopicPartition, BatchV2> byPartition,
                                       Map<Uuid, String> idToName) {
        for (var topicResponse : response.data().responses()) {
            String topic = topicResponse.name() == null || topicResponse.name().isEmpty()
                ? idToName.get(topicResponse.topicId())
                : topicResponse.name();
            for (var partitionResponse : topicResponse.partitionResponses()) {
                TopicPartition tp = new TopicPartition(topic, partitionResponse.index());
                BatchV2 batch = byPartition.remove(tp);
                if (batch == null) {
                    log.warn("Produce response for unknown partition {}", tp);
                    continue;
                }
                Errors errors = Errors.forCode(partitionResponse.errorCode());
                if (errors == Errors.NONE)
                    batch.completeSuccessfully(partitionResponse.baseOffset(),
                        partitionResponse.logAppendTimeMs());
                else
                    retryOrFail(batch, errors.exception(), errors.exception() instanceof RetriableException);
            }
        }
        for (BatchV2 batch : byPartition.values())
            batch.completeExceptionally(new IllegalStateException(
                "Produce response did not cover partition " + batch.partition()));
    }

    private void retryOrFail(BatchV2 batch, Throwable cause, boolean retriable) {
        if (retriable && batch.attempts() < settings.retries()) {
            batch.incrementAttempts();
            log.debug("Retrying batch for {} (attempt {}) after {}", batch.partition(),
                batch.attempts(), cause.toString());
            metadata.refresh();
            runtime.scheduler().schedule(() -> accumulator.reenqueue(batch),
                settings.client().retryBackoff().toMillis(), TimeUnit.MILLISECONDS);
        } else {
            batch.completeExceptionally(cause);
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
