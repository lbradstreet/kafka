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

import org.apache.kafka.common.header.Header;

import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * A record to send.
 *
 * @param topic     destination topic
 * @param partition explicit partition, or empty to let the partitioner decide
 * @param key       key, or null
 * @param value     value, or null (tombstone)
 * @param timestamp record timestamp, or empty for send time
 * @param headers   record headers, possibly empty
 */
public record ProducerRecordV2<K, V>(
    String topic,
    OptionalInt partition,
    K key,
    V value,
    OptionalLong timestamp,
    List<Header> headers
) {

    public ProducerRecordV2 {
        Objects.requireNonNull(topic, "topic");
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(timestamp, "timestamp");
        headers = List.copyOf(headers);
    }

    public static <K, V> ProducerRecordV2<K, V> of(String topic, K key, V value) {
        return new ProducerRecordV2<>(topic, OptionalInt.empty(), key, value, OptionalLong.empty(), List.of());
    }

    public static <K, V> ProducerRecordV2<K, V> of(String topic, V value) {
        return new ProducerRecordV2<>(topic, OptionalInt.empty(), null, value, OptionalLong.empty(), List.of());
    }
}
