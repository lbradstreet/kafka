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

import org.apache.kafka.clients.v2.ClientSettings;

import java.time.Duration;
import java.util.Objects;

/**
 * Consumer settings for the assignment-based v2 consumer. Group membership (KIP-848 protocol)
 * is a later phase; see DESIGN.md.
 *
 * @param client                 shared connection-level settings
 * @param maxPartitionFetchBytes per-partition fetch cap (mirrors {@code max.partition.fetch.bytes})
 * @param fetchMaxWait           broker-side wait for {@code fetchMinBytes}
 * @param fetchMinBytes          minimum bytes before the broker responds
 */
public record ConsumerSettings(
    ClientSettings client,
    int maxPartitionFetchBytes,
    Duration fetchMaxWait,
    int fetchMinBytes
) {

    public ConsumerSettings {
        Objects.requireNonNull(client, "client");
    }

    public static ConsumerSettings of(ClientSettings client) {
        return new ConsumerSettings(client, 1024 * 1024, Duration.ofMillis(500), 1);
    }
}
