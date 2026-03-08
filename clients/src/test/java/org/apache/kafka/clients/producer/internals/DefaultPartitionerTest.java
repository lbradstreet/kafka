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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class DefaultPartitionerTest {
    private byte[] keyBytes = "key".getBytes();
    private DefaultPartitioner partitioner = new DefaultPartitioner();
    private Node node0 = new Node(0, "localhost", 99);
    private Node node1 = new Node(1, "localhost", 100);
    private Node node2 = new Node(2, "localhost", 101);
    private Node[] nodes = new Node[] {node0, node1, node2};
    private String topic = "test";
    // Intentionally make the partition list not in partition order to test the edge cases.
    private List<PartitionInfo> partitions = asList(new PartitionInfo(topic, 1, null, nodes, nodes),
                                                    new PartitionInfo(topic, 2, node1, nodes, nodes),
                                                    new PartitionInfo(topic, 0, node0, nodes, nodes));
    private Cluster cluster = new Cluster("clusterId", asList(node0, node1, node2), partitions,
            Collections.<String>emptySet(), Collections.<String>emptySet());

    @Test
    public void testKeyPartitionIsStable() {
        int partition = partitioner.partition("test",  null, keyBytes, null, null, cluster);
        assertEquals("Same key should yield same partition", partition, partitioner.partition("test", null, keyBytes, null, null, cluster));
    }

    @Test
    public void testStickyPartitionWithoutOnNewBatch() {
        // Without calling onNewBatch, the partitioner should always return the same partition
        int firstPartition = partitioner.partition(topic, null, null, null, null, cluster);
        for (int i = 0; i < 100; i++) {
            int partition = partitioner.partition(topic, null, null, null, null, cluster);
            assertEquals("Sticky partitioner should return same partition without onNewBatch",
                    firstPartition, partition);
        }
    }

    @Test
    public void testPartitionSwitchesOnNewBatch() {
        // When onNewBatch is called, the partitioner should switch to a different partition
        int firstPartition = partitioner.partition(topic, null, null, null, null, cluster);
        partitioner.onNewBatch(topic, cluster, firstPartition);
        int secondPartition = partitioner.partition(topic, null, null, null, null, cluster);
        assertNotEquals("Partition should change after onNewBatch", firstPartition, secondPartition);
    }

    @Test
    public void testStickyWithUnavailablePartitions() {
        // With unavailable partitions, sticky should pick an available one and stick to it
        int partition = partitioner.partition(topic, null, null, null, null, cluster);
        assertTrue("We should never choose a leader-less node", partition == 0 || partition == 2);

        // Should stick to same partition without onNewBatch
        for (int i = 0; i < 10; i++) {
            assertEquals(partition, partitioner.partition(topic, null, null, null, null, cluster));
        }

        // After onNewBatch, should switch to the other available partition
        partitioner.onNewBatch(topic, cluster, partition);
        int nextPartition = partitioner.partition(topic, null, null, null, null, cluster);
        assertTrue("Should pick an available partition", nextPartition == 0 || nextPartition == 2);
        assertNotEquals("Should switch partitions", partition, nextPartition);
    }

    @Test
    public void testOnNewBatchCyclesThroughAllPartitions() {
        // Over multiple onNewBatch calls, we should eventually visit all available partitions
        final String topicA = "topicA";
        List<PartitionInfo> allPartitions = asList(new PartitionInfo(topicA, 0, node0, nodes, nodes),
                new PartitionInfo(topicA, 1, node1, nodes, nodes),
                new PartitionInfo(topicA, 2, node2, nodes, nodes));
        Cluster testCluster = new Cluster("clusterId", asList(node0, node1, node2), allPartitions,
                Collections.<String>emptySet(), Collections.<String>emptySet());

        Set<Integer> seenPartitions = new HashSet<>();
        for (int i = 0; i < 30; i++) {
            int partition = partitioner.partition(topicA, null, null, null, null, testCluster);
            seenPartitions.add(partition);
            partitioner.onNewBatch(topicA, testCluster, partition);
        }
        assertEquals("Should have visited all 3 partitions", 3, seenPartitions.size());
    }

    @Test
    public void testStickyPartitionPerTopic() {
        // Each topic should have independent sticky state
        final String topicA = "topicA";
        final String topicB = "topicB";

        List<PartitionInfo> allPartitions = asList(new PartitionInfo(topicA, 0, node0, nodes, nodes),
                new PartitionInfo(topicA, 1, node1, nodes, nodes),
                new PartitionInfo(topicA, 2, node2, nodes, nodes),
                new PartitionInfo(topicB, 0, node0, nodes, nodes));
        Cluster testCluster = new Cluster("clusterId", asList(node0, node1, node2), allPartitions,
                Collections.<String>emptySet(), Collections.<String>emptySet());

        int partA = partitioner.partition(topicA, null, null, null, null, testCluster);
        int partB = partitioner.partition(topicB, null, null, null, null, testCluster);

        // Switching topic A should not affect topic B
        partitioner.onNewBatch(topicA, testCluster, partA);
        assertEquals("Topic B should still be sticky", partB,
                partitioner.partition(topicB, null, null, null, null, testCluster));
    }

    @Test
    public void testKeyedPartitionNotAffectedByOnNewBatch() {
        // Keyed records should always return the same partition regardless of onNewBatch calls
        int partition = partitioner.partition(topic, null, keyBytes, null, null, cluster);
        partitioner.onNewBatch(topic, cluster, partition);
        assertEquals("Keyed partition should be stable",
                partition, partitioner.partition(topic, null, keyBytes, null, null, cluster));
    }
}
