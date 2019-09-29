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

package org.apache.kafka.jmh.fetcher;

import kafka.api.ApiVersion$;
import kafka.cluster.BrokerEndPoint;
import kafka.cluster.DelayedOperations;
import kafka.cluster.Partition;
import kafka.cluster.PartitionStateStore;
import kafka.log.CleanerConfig;
import kafka.log.Defaults;
import kafka.log.LogAppendInfo;
import kafka.log.LogConfig;
import kafka.log.LogManager;
import kafka.server.BrokerState;
import kafka.server.BrokerTopicStats;
import kafka.server.FailedPartitions;
import kafka.server.KafkaConfig;
import kafka.server.LogDirFailureChannel;
import kafka.server.MetadataCache;
import kafka.server.OffsetAndEpoch;
import kafka.server.OffsetTruncationState;
import kafka.server.ReplicaFetcherThread;
import kafka.server.ReplicaQuota;
import kafka.server.checkpoints.OffsetCheckpoints;
import kafka.utils.KafkaScheduler;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.RecordsSend;
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.LeaderAndIsrRequest;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochRequest;
import org.apache.kafka.common.utils.Time;
import org.mockito.Mockito;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.compat.java8.OptionConverters;
import scala.collection.Map;
import scala.collection.Seq;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)

public class ReplicaFetcherThreadBenchmark {
    @Param({"500", "1000", "5000", "10000"})
    private static String partitionCount;

    @State(Scope.Thread)
    public static class BenchState {
        private ReplicaFetcherBenchThread fetcher;
        private LogManager logManager;
        private File logDir = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());
        private List<File> logDirs = Collections.singletonList(logDir);
        private KafkaScheduler scheduler = new KafkaScheduler(1, "scheduler", true);
        private BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
        private LogDirFailureChannel logDirFailureChannel = Mockito.mock(LogDirFailureChannel.class);

        public BenchState() {
            logDir.mkdir();
            scheduler.startup();
            Properties props = new Properties();
            props.put("zookeeper.connect", "127.0.0.1:9999");
            KafkaConfig config = new KafkaConfig(props);

            LogConfig logConfig = createLogConfig();
            logManager = new LogManager(JavaConverters.asScalaIteratorConverter(logDirs.iterator()).asScala().toSeq(),
                    JavaConverters.asScalaIteratorConverter(new ArrayList<File>().iterator()).asScala().toSeq(),
                    new scala.collection.mutable.HashMap<>(),
                    logConfig,
                    new CleanerConfig(0, 0, 0, 0, 0, 0.0, 0, false, "MD5"),
                    1,
                    1000L,
                    10000L,
                    10000L,
                    1000L,
                    60000,
                    scheduler,
                    new BrokerState(),
                    brokerTopicStats,
                    logDirFailureChannel,
                    Time.SYSTEM);

            scala.collection.mutable.Map<TopicPartition, OffsetAndEpoch> partitions =
                    new scala.collection.mutable.HashMap<>();
            LinkedHashMap<TopicPartition, FetchResponse.PartitionData<BaseRecords>> initialFetched = new LinkedHashMap<>();
            HashMap<TopicPartition, Partition> partitionValues = new HashMap<>();
            for (int i = 0; i < Integer.parseInt(partitionCount); i++) {
                TopicPartition tp = new TopicPartition("mytopic", i);

                // one leader, plus two followers
                List<Integer> replicas = new ArrayList<>();
                replicas.add(0);
                replicas.add(1);
                replicas.add(2);
                LeaderAndIsrRequest.PartitionState partitionState = new LeaderAndIsrRequest.PartitionState(
                        0, 0, 0, replicas, 1, replicas, true);
                PartitionStateStore partitionStateStore = Mockito.mock(PartitionStateStore.class);
                Mockito.when(partitionStateStore.fetchTopicConfig()).thenReturn(new Properties());
                OffsetCheckpoints offsetCheckpoints = Mockito.mock(OffsetCheckpoints.class);
                Mockito.when(offsetCheckpoints.fetch(logDir.getAbsolutePath(), tp)).thenReturn(Option.apply(0L));
                Partition partition = new Partition(tp, 100,
                        ApiVersion$.MODULE$.latestVersion(), 0, Time.SYSTEM,
                        partitionStateStore, new DelayedOperationsMock(tp), Mockito.mock(MetadataCache.class),
                        logManager);

                partition.makeFollower(0, partitionState, 0, offsetCheckpoints);
                while (partition.log().isEmpty()) {
                    // spin until log is setup
                }

                partitions.put(tp, new OffsetAndEpoch(0, 0));
                partitionValues.put(tp, partition);
                initialFetched.put(tp, new FetchResponse.PartitionData<>(Errors.NONE, 0, 0, 0,
                        new LinkedList<>(),
                        new BaseRecords() {
                            @Override
                            public int sizeInBytes() {
                                return 0;
                            }

                            @Override
                            public RecordsSend toSend(String destination) {
                                return null;
                            }
                        }));
            }
            fetcher = new ReplicaFetcherBenchThread(config, partitionValues);
            fetcher.addPartitions(partitions);
            // force a pass to move partitions to fetching state
            // we do this in the constructor so that we do not measure this time as part of the
            // steady state work
            fetcher.doWork();
            fetcher.fetchSessionHandler().handleResponse(new FetchResponse<>(Errors.NONE,
                    initialFetched, 0, 999));
        }
    }

    // avoid mocked DelayedOperations to avoid mocked class affecting benchmark results
    private static class DelayedOperationsMock extends DelayedOperations {
        DelayedOperationsMock(TopicPartition topicPartition) {
            super(topicPartition, null, null, null);
        }

        @Override
        public int numDelayedDelete() {
            return 0;
        }
    }

    private static LogConfig createLogConfig() {
        Properties logProps = new Properties();
        logProps.put(LogConfig.SegmentMsProp(), Defaults.SegmentMs());
        logProps.put(LogConfig.SegmentBytesProp(), Defaults.SegmentSize());
        logProps.put(LogConfig.RetentionMsProp(), Defaults.RetentionMs());
        logProps.put(LogConfig.RetentionBytesProp(), Defaults.RetentionSize());
        logProps.put(LogConfig.SegmentJitterMsProp(), Defaults.SegmentJitterMs());
        logProps.put(LogConfig.CleanupPolicyProp(), Defaults.CleanupPolicy());
        logProps.put(LogConfig.MaxMessageBytesProp(), Defaults.MaxMessageSize());
        logProps.put(LogConfig.IndexIntervalBytesProp(), Defaults.IndexInterval());
        logProps.put(LogConfig.SegmentIndexBytesProp(), Defaults.MaxIndexSize());
        logProps.put(LogConfig.MessageFormatVersionProp(), Defaults.MessageFormatVersion());
        logProps.put(LogConfig.FileDeleteDelayMsProp(), Defaults.FileDeleteDelayMs());
        return LogConfig.apply(logProps, new scala.collection.immutable.HashSet<>());
    }


    @Benchmark
    public long testFetcher(BenchState state) {
        state.fetcher.doWork();
        return state.fetcher.fetcherStats().requestRate().count();
    }
}

class ReplicaFetcherBenchThread extends ReplicaFetcherThread {
    private HashMap<TopicPartition, Partition> partitions;
    private long startOffset = 0;

    ReplicaFetcherBenchThread(KafkaConfig config, HashMap<TopicPartition, Partition> partitions) {
        super("name",
                3,
                new BrokerEndPoint(3, "host", 3000),
                config,
                new FailedPartitions(),
                null,
                new Metrics(),
                Time.SYSTEM,
                new ReplicaQuota() {
                    @Override
                    public boolean isQuotaExceeded() {
                        return false;
                    }
                    @Override
                    public void record(long value) { }
                    @Override
                    public boolean isThrottled(TopicPartition topicPartition) {
                        return false;
                    }
                },
                Option.empty());
        this.partitions = partitions;
    }

    @Override
    public Option<Object> latestEpoch(TopicPartition topicPartition) {
        return Option.apply(0);
    }

    @Override
    public long logStartOffset(TopicPartition topicPartition) {
        // return 0L;
        // return startOffset++;
        return partitions.get(topicPartition).localLogOrException().logStartOffset();
    }

    @Override
    public long logEndOffset(TopicPartition topicPartition) {
        return 0;
    }

    @Override
    public void truncate(TopicPartition tp, OffsetTruncationState offsetTruncationState) {
        // pretend to truncate to move to Fetching state
    }

    @Override
    public Option<OffsetAndEpoch> endOffsetForEpoch(TopicPartition topicPartition, int epoch) {
        return OptionConverters.toScala(Optional.of(new OffsetAndEpoch(0, 0)));
    }

    @Override
    public Option<LogAppendInfo> processPartitionData(TopicPartition topicPartition, long fetchOffset, FetchResponse.PartitionData partitionData) {
        return Option.empty();
    }

    @Override
    public long fetchEarliestOffsetFromLeader(TopicPartition topicPartition, int currentLeaderEpoch) {
        return 0;
    }

    @Override
    public Map<TopicPartition, EpochEndOffset> fetchEpochEndOffsets(Map<TopicPartition, OffsetsForLeaderEpochRequest.PartitionData> partitions) {
        scala.collection.mutable.Map<TopicPartition, EpochEndOffset> endOffsets = new scala.collection.mutable.HashMap<>();
        Iterator<TopicPartition> iterator = partitions.keys().iterator();
        while (iterator.hasNext()) {
            endOffsets.put(iterator.next(), new EpochEndOffset(0, 100));
        }
        return endOffsets;
    }

    @Override
    public Seq<Tuple2<TopicPartition, FetchResponse.PartitionData<Records>>> fetchFromLeader(FetchRequest.Builder fetchRequest) {
        return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<TopicPartition, FetchResponse.PartitionData<Records>>>().iterator()).asScala().toSeq();
    }
}
