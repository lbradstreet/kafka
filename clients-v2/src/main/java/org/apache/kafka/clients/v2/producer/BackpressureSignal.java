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

import org.apache.kafka.common.TopicPartition;

/**
 * Tells the accumulator whether a destination is currently unable to accept another request
 * (its connection's in-flight window is full). While saturated, open batches keep accepting
 * records up to {@code backpressure.batch.size} instead of sealing at {@code batch.size} —
 * the request cannot be sent yet anyway, so fewer, larger batches beat a queue of small ones.
 *
 * <p>Answers are advisory and race-tolerant: a stale answer only shifts where a batch stops
 * growing, never correctness. Unknown destinations (no metadata, no connection yet) must
 * report {@code false}.
 */
interface BackpressureSignal {

    BackpressureSignal NEVER = new BackpressureSignal() {
        @Override
        public boolean isSaturated(TopicPartition partition) {
            return false;
        }

        @Override
        public boolean isTopicSaturated(String topic) {
            return false;
        }
    };

    /** @return true if the partition's leader cannot accept another request right now */
    boolean isSaturated(TopicPartition partition);

    /**
     * For unbound (Deferred, D14) batches: true only if <em>every</em> available partition
     * of the topic has a saturated leader — while any leader has room, drain-time binding
     * could route there, so the batch should seal at the normal size.
     */
    boolean isTopicSaturated(String topic);
}
