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

import java.util.concurrent.CompletableFuture;

/**
 * The v2 producer: {@code CompletableFuture}-first, never blocks the calling thread.
 *
 * <p>Transactions and idempotence are not implemented in this phase (design decision D15);
 * {@link #beginTransaction()} exists to reserve the API shape and always throws.
 */
public interface AsyncProducer<K, V> extends AutoCloseable {

    /**
     * Queue a record for sending. The returned future completes once the record is
     * acknowledged per the configured {@code acks}, or fails with the terminal error.
     * This method never blocks; if the producer's memory budget is exhausted the future
     * fails with {@code TimeoutException}.
     */
    CompletableFuture<RecordMetadataV2> send(ProducerRecordV2<K, V> record);

    /**
     * Makes all buffered records immediately eligible for sending and returns a future that
     * completes when every record accepted before this call has an outcome.
     */
    CompletableFuture<Void> flush();

    /**
     * Reserved (D15): transactional producing is not yet implemented in v2.
     *
     * @throws UnsupportedOperationException always, for now
     */
    default Transaction beginTransaction() {
        throw new UnsupportedOperationException(
            "Transactions are not implemented in the v2 producer yet (see DESIGN.md, D15)");
    }

    /** Flush, then release all resources. */
    @Override
    void close();

    /** Reserved transactional scope (D15). */
    interface Transaction {
        CompletableFuture<Void> commit();

        CompletableFuture<Void> abort();
    }
}
