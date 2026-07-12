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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous cluster metadata: fetches on demand, caches until {@code metadata.max.age.ms},
 * and retries retriable per-topic errors (new topics propagating, leader elections) with
 * backoff until the request timeout elapses.
 */
public final class MetadataManager {

    private static final Logger log = LoggerFactory.getLogger(MetadataManager.class);

    private final KafkaClientRuntime runtime;
    private final NetworkRequestDispatcher dispatcher;
    private final ClientSettings settings;

    private final Set<String> interestingTopics = ConcurrentHashMap.newKeySet();
    private volatile Cached cached;

    private record Cached(Cluster cluster, Set<String> topics, long fetchedAtMs) { }

    public MetadataManager(KafkaClientRuntime runtime, NetworkRequestDispatcher dispatcher,
                           ClientSettings settings) {
        this.runtime = runtime;
        this.dispatcher = dispatcher;
        this.settings = settings;
    }

    /** Cluster metadata covering the given topics, from cache when fresh. */
    public CompletableFuture<Cluster> cluster(Set<String> topics) {
        interestingTopics.addAll(topics);
        Cached snapshot = cached;
        long nowMs = runtime.time().milliseconds();
        if (snapshot != null
            && nowMs - snapshot.fetchedAtMs() < settings.metadataMaxAge().toMillis()
            && snapshot.topics().containsAll(topics))
            return CompletableFuture.completedFuture(snapshot.cluster());
        return refresh();
    }

    /** Force a metadata refresh for every topic seen so far. */
    public CompletableFuture<Cluster> refresh() {
        long deadlineMs = runtime.time().milliseconds() + settings.requestTimeout().toMillis();
        CompletableFuture<Cluster> result = new CompletableFuture<>();
        attemptRefresh(Set.copyOf(interestingTopics), deadlineMs, result);
        return result;
    }

    private void attemptRefresh(Set<String> topics, long deadlineMs, CompletableFuture<Cluster> result) {
        MetadataRequest.Builder request = new MetadataRequest.Builder(new ArrayList<>(topics), false);
        dispatcher.sendToAnyBroker(request).whenComplete((response, error) -> {
            if (error != null) {
                retryOrFail(topics, deadlineMs, result, unwrap(error));
                return;
            }
            MetadataResponse metadata = (MetadataResponse) response;
            Throwable topicError = firstFatalTopicError(metadata);
            if (topicError != null) {
                result.completeExceptionally(topicError);
                return;
            }
            if (hasRetriableTopicError(metadata)) {
                retryOrFail(topics, deadlineMs, result,
                    new TimeoutException("Metadata for " + topics + " not yet available"));
                return;
            }
            Cluster cluster = metadata.buildCluster();
            cached = new Cached(cluster, new HashSet<>(topics), runtime.time().milliseconds());
            result.complete(cluster);
        });
    }

    private void retryOrFail(Set<String> topics, long deadlineMs,
                             CompletableFuture<Cluster> result, Throwable lastError) {
        long backoffMs = settings.retryBackoff().toMillis();
        if (runtime.time().milliseconds() + backoffMs >= deadlineMs) {
            result.completeExceptionally(lastError);
            return;
        }
        if (!(lastError instanceof RetriableException) && !(lastError instanceof java.io.IOException)
            && !(lastError instanceof KafkaException)) {
            result.completeExceptionally(lastError);
            return;
        }
        log.debug("Retrying metadata fetch for {} after {} ms", topics, backoffMs, lastError);
        runtime.scheduler().schedule(() -> attemptRefresh(topics, deadlineMs, result),
            backoffMs, TimeUnit.MILLISECONDS);
    }

    private static Throwable firstFatalTopicError(MetadataResponse metadata) {
        for (Map.Entry<String, Errors> entry : metadata.errors().entrySet()) {
            Errors errors = entry.getValue();
            if (errors != Errors.NONE && !(errors.exception() instanceof RetriableException))
                return errors.exception("Metadata error for topic " + entry.getKey());
        }
        return null;
    }

    private static boolean hasRetriableTopicError(MetadataResponse metadata) {
        return metadata.errors().values().stream().anyMatch(e -> e != Errors.NONE);
    }

    private static Throwable unwrap(Throwable t) {
        return t instanceof java.util.concurrent.CompletionException && t.getCause() != null
            ? t.getCause() : t;
    }
}
