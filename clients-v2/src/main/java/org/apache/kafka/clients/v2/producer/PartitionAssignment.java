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

/**
 * How a record relates to a partition (design decision D14).
 *
 * <p>{@link Fixed} pins the record to a partition at send time — keyed records and explicit
 * partitions. {@link Deferred} leaves the record unbound: it accumulates in a per-topic queue
 * and is bound to a concrete partition only when its batch is drained, based on leader
 * availability and queue depth. Until a batch is sealed for send it may be re-bound (e.g. when
 * a leader disappears); after sealing its partition is immutable — the seam where idempotent
 * sequences will later be assigned (D15).
 *
 * <p>Deferred records trade per-key ordering for availability and load balance; keyed records
 * default to {@code Fixed}.
 */
public sealed interface PartitionAssignment {

    Deferred DEFERRED = new Deferred();

    record Fixed(int partition) implements PartitionAssignment {
        public Fixed {
            if (partition < 0)
                throw new IllegalArgumentException("partition must be non-negative: " + partition);
        }
    }

    record Deferred() implements PartitionAssignment { }
}
