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
package org.apache.kafka.clients.v2.dst;

import org.apache.kafka.clients.v2.ClientSettings;
import org.apache.kafka.clients.v2.KafkaClientRuntime;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.network.netty.DirectExecutorService;
import org.apache.kafka.network.netty.FaultInjector;
import org.apache.kafka.network.netty.SimBroker;
import org.apache.kafka.network.netty.SimCluster;
import org.apache.kafka.network.netty.SimNetwork;
import org.apache.kafka.network.netty.SimScheduler;
import org.apache.kafka.network.netty.SimTrace;
import org.apache.kafka.network.netty.SimTransport;

import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * The deterministic simulation harness (design decision D13): wires the sim runtime —
 * virtual clock, single task queue, in-memory network with seeded fault injection, and
 * protocol-accurate {@link SimBroker}s — into a {@link KafkaClientRuntime}, so real v2
 * clients run unmodified with every source of time, scheduling and I/O simulated.
 *
 * <p>A run is a pure function of (scenario, seed): the same seed must yield an identical
 * {@link SimTrace}. Failures reproduce by re-running the seed.
 */
public final class DstHarness implements AutoCloseable {

    /** A generous cap on virtual time; hitting it means the scenario hung. */
    public static final long MAX_VIRTUAL_TIME_MS = Duration.ofMinutes(30).toMillis();

    public final MockTime time = new MockTime(0, 0L, 0L);
    public final SimTrace trace = new SimTrace(time);
    public final SimScheduler scheduler = new SimScheduler(time);
    public final SimCluster cluster;
    public final FaultInjector faults;
    public final SimNetwork network;
    public final SimTransport transport;
    public final KafkaClientRuntime runtime;

    public DstHarness(long seed, FaultInjector.FaultProfile profile, int brokerCount) {
        this.cluster = new SimCluster(brokerCount);
        this.faults = new FaultInjector(seed, profile, trace);
        this.network = new SimNetwork(scheduler, faults, trace);
        cluster.nodes().forEach(node -> network.addBroker(new SimBroker(node.id(), cluster, trace)));
        this.transport = new SimTransport(network, cluster, scheduler, trace);
        this.runtime = KafkaClientRuntime.newBuilder()
            .transport(transport)
            .scheduler(scheduler)
            .callbackExecutor(new DirectExecutorService())
            .time(time)
            .build();
    }

    public ClientSettings.Builder clientSettings() {
        return ClientSettings.newBuilder(cluster.bootstrap())
            .clientId("dst")
            .requestTimeout(Duration.ofSeconds(10))
            .retryBackoff(Duration.ofMillis(50));
    }

    /** Drive the simulation until the condition holds; throws if it never can (a hang). */
    public void runUntil(BooleanSupplier condition) {
        scheduler.runUntil(condition, MAX_VIRTUAL_TIME_MS);
    }

    public List<String> traceEvents() {
        return trace.events();
    }

    @Override
    public void close() {
        runtime.close();
    }
}
