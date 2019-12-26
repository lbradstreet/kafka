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

package org.apache.kafka.jmh.metadata;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.streams.state.internals.MeteredTimestampedKeyValueStore;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MetadataRequestBenchmark {
    @Param(value = {"200", "1000"})
    private int topicCount;

    @Param(value = {"10", "50", "100"})
    private int partitionsPerTopic;

    private int nBrokers = 20;
    private int replicationFactor = 3;

    String clusterId = "clusterid";
    int controllerId = 33;
    List<Node> brokers = new ArrayList<>();
    List<MetadataResponse.TopicMetadata> topicMetadatas = new ArrayList<>();


    @Setup(Level.Trial)
    public void setUp() {
        for (int b = 0; b < nBrokers; b++) {
            brokers.add(new Node(b, "host", 999));
        }

        for (int t = 0; t < topicCount; t++) {
            List<MetadataResponse.PartitionMetadata> partitionMetadatas = new ArrayList<>();
            for (int p = 0; p < partitionsPerTopic; p++) {
                int startBroker = new Random().nextInt(nBrokers - replicationFactor); // is this right?
                List<Node> replicas = new ArrayList<>();
                for (int r = startBroker; r < startBroker + replicationFactor; r++) {
                    replicas.add(brokers.get(r));
                }

                MetadataResponse.PartitionMetadata partitionMetadata =
                        new MetadataResponse.PartitionMetadata(Errors.NONE, p, brokers.get(startBroker),
                                Optional.of(33), replicas, replicas, new ArrayList<>());
                partitionMetadatas.add(partitionMetadata);
            }
            MetadataResponse.TopicMetadata topicMetadata =
                    new MetadataResponse.TopicMetadata(Errors.NONE, "topic-"+t,
                            false, partitionMetadatas);
            topicMetadatas.add(topicMetadata);
        }
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void prepareResponse() {
        MetadataResponse.prepareResponse(1000, brokers, clusterId, controllerId, topicMetadatas,
                1 << AclOperation.ALL.code());
    }
}
