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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * The simulated wire: delivers frames between client channels and {@link SimBroker}s as
 * scheduled virtual-time events, applying {@link FaultInjector} decisions at each hop.
 *
 * <p>Delivery is order-preserving per connection and direction (extra fault delay never
 * reorders frames on one connection) — the FIFO property real TCP provides and the
 * transport's correlation layer depends on (D8). Faults are therefore drops, delays and
 * disconnects, never reorderings or duplications.
 */
public final class SimNetwork {

    public static final long BASE_LATENCY_MS = 2;

    /** One live client connection attached to a broker. */
    public static final class Endpoint {
        final String connectionId;
        final SimChannel channel;
        final SimBroker broker;
        long nextClientToBrokerMs;
        long nextBrokerToClientMs;
        boolean open = true;

        Endpoint(String connectionId, SimChannel channel, SimBroker broker) {
            this.connectionId = connectionId;
            this.channel = channel;
            this.broker = broker;
        }
    }

    private final SimScheduler scheduler;
    private final FaultInjector faults;
    private final SimTrace trace;
    private final ProduceObserver observer;
    private final Map<Integer, SimBroker> brokers = new HashMap<>();

    public SimNetwork(SimScheduler scheduler, FaultInjector faults, SimTrace trace) {
        this(scheduler, faults, trace, ProduceObserver.NONE);
    }

    public SimNetwork(SimScheduler scheduler, FaultInjector faults, SimTrace trace,
                      ProduceObserver observer) {
        this.scheduler = scheduler;
        this.faults = faults;
        this.trace = trace;
        this.observer = observer;
    }

    public void addBroker(SimBroker broker) {
        brokers.put(broker.id(), broker);
    }

    public SimBroker broker(int id) {
        return brokers.get(id);
    }

    public Endpoint register(String connectionId, SimChannel channel, SimBroker broker) {
        Endpoint endpoint = new Endpoint(connectionId, channel, broker);
        channel.closeFuture().addListener(f -> endpoint.open = false);
        channel.outboundSink(frame -> clientFrame(endpoint, frame));
        return endpoint;
    }

    /** Client → broker hop; called synchronously from the channel flush. */
    private void clientFrame(Endpoint endpoint, ByteBuf frame) {
        byte[] bytes = new byte[frame.readableBytes()];
        frame.readBytes(bytes);
        frame.release();
        if (!endpoint.open)
            return;
        // The frame is now on the wire (in flight) until its response is delivered.
        observer.onFrameSent(endpoint.connectionId);
        if (faults.dropRequest(endpoint.connectionId)) {
            observer.onResponseDelivered(endpoint.connectionId); // never answered; clear in-flight
            return; // the client's request timeout handles it
        }
        if (faults.disconnect(endpoint.connectionId)) {
            observer.onResponseDelivered(endpoint.connectionId);
            scheduler.execute(() -> closeEndpoint(endpoint));
            return;
        }
        long deliverAt = orderPreservingDeliveryTime(endpoint.nextClientToBrokerMs);
        endpoint.nextClientToBrokerMs = deliverAt;
        scheduleAt(deliverAt, () -> deliverToBroker(endpoint, bytes));
    }

    private void deliverToBroker(Endpoint endpoint, byte[] requestFrame) {
        if (!endpoint.open) {
            observer.onResponseDelivered(endpoint.connectionId);
            return;
        }
        SimBroker.Response response = endpoint.broker.handle(requestFrame);
        if (faults.dropResponse(endpoint.connectionId)) {
            observer.onResponseDelivered(endpoint.connectionId);
            return;
        }
        // Broker processing time delays the response but never reorders it: the max() with
        // the previous departure keeps responses FIFO on the connection (head-of-line).
        long deliverAt = orderPreservingDeliveryTime(
            endpoint.nextBrokerToClientMs, response.processingDelayMs());
        endpoint.nextBrokerToClientMs = deliverAt;
        scheduleAt(deliverAt, () -> deliverToClient(endpoint, response.frame()));
    }

    private void deliverToClient(Endpoint endpoint, byte[] responseFrame) {
        observer.onResponseDelivered(endpoint.connectionId);
        if (!endpoint.open || !endpoint.channel.isActive())
            return;
        endpoint.channel.writeInbound(Unpooled.wrappedBuffer(responseFrame));
        endpoint.channel.runPendingTasks();
    }

    private void closeEndpoint(Endpoint endpoint) {
        if (!endpoint.open)
            return;
        endpoint.open = false;
        trace.add("network close conn=" + endpoint.connectionId);
        endpoint.channel.close();
        endpoint.channel.runPendingTasks();
    }

    private long orderPreservingDeliveryTime(long previousDeliveryMs) {
        return orderPreservingDeliveryTime(previousDeliveryMs, 0L);
    }

    private long orderPreservingDeliveryTime(long previousDeliveryMs, long extraProcessingMs) {
        long candidate = scheduler.time().milliseconds()
            + BASE_LATENCY_MS + faults.extraDelayMs() + extraProcessingMs;
        return Math.max(candidate, previousDeliveryMs);
    }

    private void scheduleAt(long timeMs, Runnable task) {
        scheduler.schedule(task, Math.max(0, timeMs - scheduler.time().milliseconds()),
            TimeUnit.MILLISECONDS);
    }
}
