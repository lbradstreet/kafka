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
package org.apache.kafka.clients.v2.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The v2 consumer, pull-first (design decision D5): {@link #poll()} returns a future for the
 * next batch of records rather than blocking. This phase supports explicit assignment only;
 * group membership arrives with the KIP-848 manager port (DESIGN.md phase 6).
 */
public interface AsyncConsumer<K, V> extends AutoCloseable {

    /** Replace the assignment. Positions default to the earliest offset until seeked. */
    void assign(Collection<TopicPartition> partitions);

    /** Set the fetch position explicitly. */
    void seek(TopicPartition partition, long offset);

    /** Resolve positions to the log start offset. */
    CompletableFuture<Void> seekToBeginning(Collection<TopicPartition> partitions);

    /** Resolve positions to the log end offset. */
    CompletableFuture<Void> seekToEnd(Collection<TopicPartition> partitions);

    /**
     * Fetch the next records from all assigned partitions. Completes with an empty list when
     * the fetch returns no data within the configured wait.
     */
    CompletableFuture<List<ConsumerRecordV2<K, V>>> poll();

    /** @return the next offset that will be fetched for the partition, if known */
    Long position(TopicPartition partition);

    @Override
    void close();
}
