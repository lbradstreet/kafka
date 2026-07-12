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
 * The reserved idempotence/transactions seam (design decision D15).
 *
 * <p>Sealing happens exactly once per batch, at drain time, after the batch is bound to its
 * final partition. This is where the classic producer stamps producerId/epoch/sequence
 * ({@code RecordAccumulator.drainBatchesForOneNode} &rarr; {@code setProducerState}); when
 * idempotence is implemented for v2, that logic lands here. After sealing, a batch's partition
 * and contents are immutable — in particular, late-binding rebinds (D14) are illegal.
 *
 * <p>The default implementation does nothing.
 */
public interface BatchSealer {

    BatchSealer NO_OP = (batch, partition) -> { };

    void seal(BatchV2 batch, TopicPartition partition);
}
