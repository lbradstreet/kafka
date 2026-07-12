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

import org.apache.kafka.common.utils.Utils;

/**
 * v2 partitioner: returns an assignment rather than a partition number, so a record can be
 * either pinned ({@code Fixed}) or left for drain-time binding ({@code Deferred}, D14).
 */
public interface PartitionerV2 {

    /**
     * @param topic         the topic
     * @param keyBytes      serialized key, or null
     * @param numPartitions current partition count for the topic
     */
    PartitionAssignment assign(String topic, byte[] keyBytes, int numPartitions);

    /**
     * Default behavior: keyed records hash to a fixed partition exactly like the classic
     * producer (murmur2, positive, modulo); unkeyed records are deferred to drain-time binding.
     */
    static PartitionerV2 defaultPartitioner() {
        return (topic, keyBytes, numPartitions) -> {
            if (keyBytes == null)
                return PartitionAssignment.DEFERRED;
            return new PartitionAssignment.Fixed(Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions);
        };
    }
}
