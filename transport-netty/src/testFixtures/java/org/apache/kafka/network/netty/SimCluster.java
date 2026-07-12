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
package org.apache.kafka.network.netty;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The simulated cluster's shared state: nodes, topics, partition leadership, and the
 * per-partition logs (storage outlives leadership moves, like real replicated logs).
 *
 * <p>Topic ids derive deterministically from the topic name — no global randomness (D13).
 */
public final class SimCluster {

    /** A record in a simulated log; its offset is its index in the partition's log list. */
    public record StoredRecord(byte[] key, byte[] value) { }

    private static final class Topic {
        final Uuid id;
        final Map<Integer, Integer> leaderByPartition = new LinkedHashMap<>();

        Topic(Uuid id) {
            this.id = id;
        }
    }

    private final List<Node> nodes = new ArrayList<>();
    private final Map<String, Topic> topics = new LinkedHashMap<>();
    private final Map<TopicPartition, List<StoredRecord>> logs = new HashMap<>();

    public SimCluster(int brokerCount) {
        for (int id = 1; id <= brokerCount; id++)
            nodes.add(new Node(id, hostOf(id), 9092));
    }

    public static String hostOf(int brokerId) {
        return "sim-broker-" + brokerId;
    }

    public int brokerIdFor(String host) {
        if (!host.startsWith("sim-broker-"))
            throw new IllegalArgumentException("Not a sim broker host: " + host);
        return Integer.parseInt(host.substring("sim-broker-".length()));
    }

    public List<Node> nodes() {
        return List.copyOf(nodes);
    }

    public String bootstrap() {
        Node first = nodes.get(0);
        return first.host() + ":" + first.port();
    }

    /** Leaders are assigned round-robin over the nodes. */
    public synchronized void createTopic(String name, int partitions) {
        Topic topic = new Topic(deterministicTopicId(name));
        for (int p = 0; p < partitions; p++)
            topic.leaderByPartition.put(p, nodes.get(p % nodes.size()).id());
        topics.put(name, topic);
    }

    public synchronized void moveLeader(TopicPartition tp, int newLeaderBrokerId) {
        Topic topic = topics.get(tp.topic());
        if (topic == null || !topic.leaderByPartition.containsKey(tp.partition()))
            throw new IllegalArgumentException("Unknown partition " + tp);
        topic.leaderByPartition.put(tp.partition(), newLeaderBrokerId);
    }

    public synchronized boolean topicExists(String name) {
        return topics.containsKey(name);
    }

    public synchronized List<String> topicNames() {
        return List.copyOf(topics.keySet());
    }

    public synchronized Uuid topicId(String name) {
        Topic topic = topics.get(name);
        return topic == null ? Uuid.ZERO_UUID : topic.id;
    }

    public synchronized String topicName(Uuid id) {
        for (Map.Entry<String, Topic> entry : topics.entrySet())
            if (entry.getValue().id.equals(id))
                return entry.getKey();
        return null;
    }

    public synchronized int partitionCount(String name) {
        Topic topic = topics.get(name);
        return topic == null ? 0 : topic.leaderByPartition.size();
    }

    /** @return leader broker id, or -1 if the partition is unknown */
    public synchronized int leader(TopicPartition tp) {
        Topic topic = topics.get(tp.topic());
        if (topic == null)
            return -1;
        Integer leader = topic.leaderByPartition.get(tp.partition());
        return leader == null ? -1 : leader;
    }

    /** Append validated records; returns the base offset. */
    public synchronized long append(TopicPartition tp, List<StoredRecord> records) {
        List<StoredRecord> log = logs.computeIfAbsent(tp, p -> new ArrayList<>());
        long baseOffset = log.size();
        log.addAll(records);
        return baseOffset;
    }

    public synchronized long logEndOffset(TopicPartition tp) {
        List<StoredRecord> log = logs.get(tp);
        return log == null ? 0 : log.size();
    }

    public synchronized List<StoredRecord> log(TopicPartition tp) {
        List<StoredRecord> log = logs.get(tp);
        return log == null ? List.of() : List.copyOf(log);
    }

    private static Uuid deterministicTopicId(String name) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        long msb = 0x517b_0000_0000_0000L;
        long lsb = 1;
        for (byte b : bytes) {
            msb = msb * 31 + b;
            lsb = lsb * 131 + b;
        }
        // Uuid.ZERO_UUID and reserved ids all have tiny values; ensure we stay clear.
        Uuid id = new Uuid(msb | 0x4000, lsb | 0x8000);
        if (id.equals(Uuid.ZERO_UUID))
            throw new IllegalStateException("Degenerate topic id for " + name);
        return id;
    }
}
