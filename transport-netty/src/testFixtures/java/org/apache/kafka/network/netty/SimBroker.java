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
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A protocol-accurate in-memory broker node for the DST harness.
 *
 * <p>Requests arrive as real wire frames produced by the real client pipeline; they are
 * parsed with the production request classes and answered with production response
 * serialization, FIFO per connection like a real broker. Only the APIs the producer path
 * needs are implemented — anything else fails loudly so scope creep is visible.
 */
public final class SimBroker {

    /** A serialized response frame plus how long the broker took to produce it. */
    public record Response(byte[] frame, long processingDelayMs) { }

    private final int id;
    private final SimCluster cluster;
    private final SimTrace trace;
    private final BrokerTimingModel timingModel;
    private final ThrottleModel throttleModel;
    private final ProduceObserver observer;
    private int produceCount = 0;

    public SimBroker(int id, SimCluster cluster, SimTrace trace) {
        this(id, cluster, trace, BrokerTimingModel.INSTANT, ThrottleModel.NONE, ProduceObserver.NONE);
    }

    public SimBroker(int id, SimCluster cluster, SimTrace trace, BrokerTimingModel timingModel,
                     ProduceObserver observer) {
        this(id, cluster, trace, timingModel, ThrottleModel.NONE, observer);
    }

    public SimBroker(int id, SimCluster cluster, SimTrace trace, BrokerTimingModel timingModel,
                     ThrottleModel throttleModel, ProduceObserver observer) {
        this.id = id;
        this.cluster = cluster;
        this.trace = trace;
        this.timingModel = timingModel;
        this.throttleModel = throttleModel;
        this.observer = observer;
    }

    public int id() {
        return id;
    }

    /**
     * @param requestFrame a full request frame including the 4-byte length prefix
     * @return the full response frame including the 4-byte length prefix, plus the broker's
     *         processing delay (see {@link BrokerTimingModel})
     */
    public Response handle(byte[] requestFrame) {
        ByteBuffer buffer = ByteBuffer.wrap(requestFrame);
        int declaredSize = buffer.getInt();
        if (declaredSize != buffer.remaining())
            throw new IllegalStateException("Frame length prefix " + declaredSize
                + " does not match payload size " + buffer.remaining());
        RequestHeader header = RequestHeader.parse(buffer);
        AbstractRequest request = AbstractRequest.parseRequest(header.apiKey(), header.apiVersion(),
            new ByteBufferAccessor(buffer)).request;

        AbstractResponse response = switch (header.apiKey()) {
            case API_VERSIONS -> handleApiVersions();
            case METADATA -> handleMetadata((MetadataRequest) request);
            case PRODUCE -> handleProduce((org.apache.kafka.common.requests.ProduceRequest) request);
            default -> throw new UnsupportedOperationException(
                "SimBroker does not implement " + header.apiKey()
                    + " — extend it deliberately rather than silently");
        };

        if (header.apiKey() == ApiKeys.PRODUCE)
            produceCount++;
        long processingDelayMs = timingModel.processingDelayMs(header.apiKey(), produceCount);

        ByteBuffer payload = RequestTestUtils.serializeResponseWithHeader(response,
            header.apiVersion(), header.correlationId());
        byte[] framed = new byte[4 + payload.remaining()];
        ByteBuffer out = ByteBuffer.wrap(framed);
        out.putInt(payload.remaining());
        out.put(payload);
        return new Response(framed, processingDelayMs);
    }

    private AbstractResponse handleApiVersions() {
        trace.add("broker-" + id + " api-versions");
        return TestUtils.defaultApiVersionsResponse(ApiMessageType.ListenerType.BROKER);
    }

    private AbstractResponse handleMetadata(MetadataRequest request) {
        MetadataResponseData data = new MetadataResponseData()
            .setClusterId("sim-cluster")
            .setControllerId(1)
            .setThrottleTimeMs(0);
        for (Node node : cluster.nodes())
            data.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(node.id())
                .setHost(node.host())
                .setPort(node.port()));

        List<String> requested = request.isAllTopics() || request.topics() == null
            ? cluster.topicNames()
            : request.topics();
        for (String topic : requested) {
            MetadataResponseData.MetadataResponseTopic topicResponse =
                new MetadataResponseData.MetadataResponseTopic().setName(topic);
            if (!cluster.topicExists(topic)) {
                topicResponse.setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
            } else {
                topicResponse.setTopicId(cluster.topicId(topic)).setErrorCode(Errors.NONE.code());
                for (int p = 0; p < cluster.partitionCount(topic); p++) {
                    int leader = cluster.leader(new TopicPartition(topic, p));
                    topicResponse.partitions().add(new MetadataResponseData.MetadataResponsePartition()
                        .setPartitionIndex(p)
                        .setErrorCode(Errors.NONE.code())
                        .setLeaderId(leader)
                        .setLeaderEpoch(0)
                        .setReplicaNodes(List.of(leader))
                        .setIsrNodes(List.of(leader)));
                }
            }
            data.topics().add(topicResponse);
        }
        trace.add("broker-" + id + " metadata topics=" + requested);
        return new org.apache.kafka.common.requests.MetadataResponse(data,
            ApiKeys.METADATA.latestVersion());
    }

    private AbstractResponse handleProduce(org.apache.kafka.common.requests.ProduceRequest request) {
        ProduceResponseData responseData = new ProduceResponseData();
        int requestRecords = 0;
        int requestBytes = 0;
        for (ProduceRequestData.TopicProduceData topicData : request.data().topicData()) {
            String topic = topicData.name() == null || topicData.name().isEmpty()
                ? cluster.topicName(topicData.topicId())
                : topicData.name();
            ProduceResponseData.TopicProduceResponse topicResponse =
                new ProduceResponseData.TopicProduceResponse()
                    .setName(topicData.name())
                    .setTopicId(topicData.topicId());
            for (ProduceRequestData.PartitionProduceData partitionData : topicData.partitionData()) {
                TopicPartition tp = new TopicPartition(topic, partitionData.index());
                ProduceResponseData.PartitionProduceResponse partitionResponse =
                    new ProduceResponseData.PartitionProduceResponse()
                        .setIndex(partitionData.index())
                        .setLogAppendTimeMs(-1);
                if (cluster.leader(tp) != id) {
                    trace.add("broker-" + id + " produce NOT_LEADER " + tp);
                    partitionResponse.setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code()).setBaseOffset(-1);
                } else {
                    MemoryRecords records = (MemoryRecords) partitionData.records();
                    requestBytes += records.sizeInBytes();
                    List<SimCluster.StoredRecord> stored = validateAndExtract(records);
                    requestRecords += stored.size();
                    long baseOffset = cluster.append(tp, stored);
                    trace.add("broker-" + id + " produce " + tp + " base=" + baseOffset
                        + " count=" + stored.size());
                    partitionResponse.setErrorCode(Errors.NONE.code()).setBaseOffset(baseOffset);
                }
                topicResponse.partitionResponses().add(partitionResponse);
            }
            responseData.responses().add(topicResponse);
        }
        int throttleMs = throttleModel.throttleMs(produceCount + 1); // +1: counted after handle()
        if (throttleMs > 0) {
            responseData.setThrottleTimeMs(throttleMs);
            trace.add("broker-" + id + " throttle " + throttleMs + "ms");
        }
        observer.onProduceRequest(id, requestRecords, requestBytes, throttleMs);
        return new org.apache.kafka.common.requests.ProduceResponse(responseData);
    }

    /** Validate batch integrity (CRC) exactly like a real broker, then copy records out. */
    private List<SimCluster.StoredRecord> validateAndExtract(MemoryRecords records) {
        List<SimCluster.StoredRecord> stored = new ArrayList<>();
        for (RecordBatch batch : records.batches()) {
            batch.ensureValid();
            for (Record record : batch) {
                stored.add(new SimCluster.StoredRecord(
                    Utils.toNullableArray(record.key()),
                    Utils.toNullableArray(record.value())));
            }
        }
        return stored;
    }
}
