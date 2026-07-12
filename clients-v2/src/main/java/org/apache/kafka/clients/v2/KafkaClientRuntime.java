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
package org.apache.kafka.clients.v2;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.network.netty.ClientTransport;
import org.apache.kafka.network.netty.NettyClientTransport;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared infrastructure for v2 clients: the transport (event loops), a scheduler for delays
 * and retries, the executor on which user-visible futures complete, and the {@link Time}
 * source.
 *
 * <p>One runtime can back any number of producers, consumers and admin clients, sharing the
 * event-loop threads (D10). Every source of time, scheduling and I/O used by v2 clients flows
 * through this object, which is the seam the deterministic simulation harness replaces (D13).
 */
public final class KafkaClientRuntime implements AutoCloseable {

    private final ClientTransport transport;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService callbackExecutor;
    private final Time time;
    private final boolean ownsTransport;

    private KafkaClientRuntime(ClientTransport transport,
                               ScheduledExecutorService scheduler,
                               ExecutorService callbackExecutor,
                               Time time,
                               boolean ownsTransport) {
        this.transport = transport;
        this.scheduler = scheduler;
        this.callbackExecutor = callbackExecutor;
        this.time = time;
        this.ownsTransport = ownsTransport;
    }

    /** Production runtime: Netty transport, daemon threads, system clock. */
    public static KafkaClientRuntime create() {
        return newBuilder().build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public ClientTransport transport() {
        return transport;
    }

    public ScheduledExecutorService scheduler() {
        return scheduler;
    }

    public ExecutorService callbackExecutor() {
        return callbackExecutor;
    }

    public Time time() {
        return time;
    }

    @Override
    public void close() {
        scheduler.shutdown();
        callbackExecutor.shutdown();
        if (ownsTransport)
            transport.close();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            callbackExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static final class Builder {
        private ClientTransport transport;
        private ScheduledExecutorService scheduler;
        private ExecutorService callbackExecutor;
        private Time time = Time.SYSTEM;

        /** Override the transport — the simulation harness's entry point (D13). */
        public Builder transport(ClientTransport transport) {
            this.transport = transport;
            return this;
        }

        public Builder scheduler(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Builder callbackExecutor(ExecutorService executor) {
            this.callbackExecutor = executor;
            return this;
        }

        public Builder time(Time time) {
            this.time = time;
            return this;
        }

        public KafkaClientRuntime build() {
            boolean ownsTransport = transport == null;
            ClientTransport effectiveTransport = ownsTransport ? new NettyClientTransport() : transport;
            ScheduledExecutorService effectiveScheduler = scheduler != null ? scheduler
                : Executors.newSingleThreadScheduledExecutor(daemonThreads("kafka-v2-scheduler"));
            ExecutorService effectiveCallbackExecutor = callbackExecutor != null ? callbackExecutor
                : Executors.newSingleThreadExecutor(daemonThreads("kafka-v2-callbacks"));
            return new KafkaClientRuntime(effectiveTransport, effectiveScheduler,
                effectiveCallbackExecutor, time, ownsTransport);
        }

        private static ThreadFactory daemonThreads(String prefix) {
            AtomicInteger counter = new AtomicInteger();
            return runnable -> {
                Thread thread = new Thread(runnable, prefix + "-" + counter.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            };
        }
    }
}
