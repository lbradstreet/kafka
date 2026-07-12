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
import org.apache.kafka.common.compress.Compression;

import java.time.Duration;
import java.util.Objects;

/**
 * Producer settings. Idempotence and transactions are intentionally absent in this phase
 * (design decision D15); the drain pipeline carries the seams they will use.
 *
 * @param client          shared connection-level settings
 * @param batchSize       target uncompressed bytes per batch when the destination can accept
 *                        another request right away (mirrors {@code batch.size})
 * @param backpressureBatchSize batch-size ceiling used while the destination's in-flight
 *                        window is saturated: since the batch cannot be sent yet anyway, it
 *                        keeps accepting records up to this size, so the eventual request
 *                        carries more data ({@code backpressure.batch.size})
 * @param linger          how long a non-full batch may wait for company (mirrors {@code linger.ms})
 * @param compression     compression codec and level (KIP-390 levels supported via
 *                        {@link Compression} builders)
 * @param maxRequestSize  cap on a single produce request (mirrors {@code max.request.size})
 * @param bufferMemory    total memory budget for unsent batches (mirrors {@code buffer.memory})
 * @param acks            acknowledgment level: 0, 1 or -1 (all)
 * @param retries         per-batch retry attempts for retriable broker errors
 */
public record ProducerSettings(
    ClientSettings client,
    int batchSize,
    int backpressureBatchSize,
    Duration linger,
    Compression compression,
    int maxRequestSize,
    long bufferMemory,
    short acks,
    int retries
) {

    public ProducerSettings {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(compression, "compression");
        if (batchSize <= 0)
            throw new IllegalArgumentException("batchSize must be positive");
        if (backpressureBatchSize < batchSize)
            throw new IllegalArgumentException("backpressureBatchSize (" + backpressureBatchSize
                + ") must be >= batchSize (" + batchSize + ")");
        if (backpressureBatchSize > maxRequestSize)
            throw new IllegalArgumentException("backpressureBatchSize (" + backpressureBatchSize
                + ") must fit in maxRequestSize (" + maxRequestSize + ")");
        if (acks != 1 && acks != -1)
            throw new IllegalArgumentException("acks must be 1 or -1 (acks=0 needs fire-and-forget "
                + "support in the transport's correlation layer, which is not implemented yet)");
    }

    public static Builder newBuilder(ClientSettings client) {
        return new Builder(client);
    }

    public static final class Builder {
        private final ClientSettings client;
        private int batchSize = 16 * 1024;
        private int backpressureBatchSize = 0; // 0 → derived default in build()
        private Duration linger = Duration.ZERO;
        private Compression compression = Compression.NONE;
        private int maxRequestSize = 1024 * 1024;
        private long bufferMemory = 32L * 1024 * 1024;
        private short acks = -1;
        private int retries = 5;

        private Builder(ClientSettings client) {
            this.client = client;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /** Set equal to {@code batchSize} to disable backpressure-adaptive batching. */
        public Builder backpressureBatchSize(int backpressureBatchSize) {
            this.backpressureBatchSize = backpressureBatchSize;
            return this;
        }

        public Builder linger(Duration linger) {
            this.linger = linger;
            return this;
        }

        public Builder compression(Compression compression) {
            this.compression = compression;
            return this;
        }

        public Builder maxRequestSize(int maxRequestSize) {
            this.maxRequestSize = maxRequestSize;
            return this;
        }

        public Builder bufferMemory(long bufferMemory) {
            this.bufferMemory = bufferMemory;
            return this;
        }

        public Builder acks(short acks) {
            this.acks = acks;
            return this;
        }

        public Builder retries(int retries) {
            this.retries = retries;
            return this;
        }

        public ProducerSettings build() {
            int effectiveBackpressureBatchSize = backpressureBatchSize != 0
                ? backpressureBatchSize
                : (int) Math.min(4L * batchSize, maxRequestSize);
            return new ProducerSettings(client, batchSize, effectiveBackpressureBatchSize, linger,
                compression, maxRequestSize, bufferMemory, acks, retries);
        }
    }
}
