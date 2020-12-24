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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

/**
 * An internal class that implements a cache used for sticky partitioning behavior. The cache tracks the current sticky
 * partition for any given topic. This class should not be used externally. 
 */
public class StickyPartitionCache {
    private final ConcurrentMap<String, Integer> indexCache;
    private final Sender sender;

    public StickyPartitionCache(Sender sender) {
        this.indexCache = new ConcurrentHashMap<>();
        this.sender = sender;
    }

    public int partition(String topic, Cluster cluster) {
        Integer part = indexCache.get(topic);
        if (part == null) {
            return nextPartition(topic, cluster, -1);
        }
        return part;
    }

    public int nextPartition(String topic, Cluster cluster, int prevPartition) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        Integer oldPart = indexCache.get(topic);
        Integer newPart = oldPart;
        // Check that the current sticky partition for the topic is either not set or that the partition that 
        // triggered the new batch matches the sticky partition that needs to be changed.
        if (oldPart == null || oldPart == prevPartition) {
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            if (availablePartitions.size() < 1) {
                Integer random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                newPart = random % partitions.size();
            } else if (availablePartitions.size() == 1) {
                newPart = availablePartitions.get(0).partition();
            } else {
                // super inefficient approach
                Optional<PartitionInfo> choice = availablePartitions.stream().min(new Comparator<PartitionInfo>() {
                    @Override
                    public int compare(PartitionInfo tp1, PartitionInfo tp2) {
                        long o1NumInFlight = sender.numInFlightBatches(new TopicPartition(tp1.topic(), tp1.partition()));
                        long o2NumInFlight = sender.numInFlightBatches(new TopicPartition(tp2.topic(), tp2.partition()));
                        //System.out.println("num in flight " + tp1 + " " + o1NumInFlight + " tp 2" + " " + tp2 + " " + o2NumInFlight);
                        return Double.compare(o1NumInFlight + ThreadLocalRandom.current().nextDouble(1), o2NumInFlight + ThreadLocalRandom.current().nextDouble(1));
                    }
                });

                /*
                while (newPart == null || newPart.equals(oldPart)) {
                    int random = Utils.toPositive(ThreadLocalRandom.current().nextInt());
                    newPart = availablePartitions.get(random % availablePartitions.size()).partition();
                }
                 */
                //System.out.println("choice " + choice);
                newPart = choice.get().partition();
                //System.out.println("new partition " + newPart);
            }
            // Only change the sticky partition if it is null or prevPartition matches the current sticky partition.
            if (oldPart == null) {
                indexCache.putIfAbsent(topic, newPart);
            } else {
                indexCache.replace(topic, prevPartition, newPart);
            }
            return indexCache.get(topic);
        }
        return indexCache.get(topic);
    }

}