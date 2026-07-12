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

import org.apache.kafka.common.utils.MockTime;

import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * Deterministic virtual-time scheduler: the single task queue of the DST harness (D13).
 *
 * <p>Tasks execute in {@code (dueTime, submissionOrder)} order on the caller's thread;
 * {@link #runUntil} advances the {@link MockTime} clock to each task's due time. Everything
 * in a simulation — drain ticks, request timeouts, network deliveries, retries — flows
 * through this queue, so a run is a pure function of (scenario, seed).
 *
 * <p>Single-threaded by design: all simulation work happens inline on the driving thread.
 */
public final class SimScheduler extends AbstractExecutorService implements ScheduledExecutorService {

    private final MockTime time;
    private final PriorityQueue<SimTask> tasks = new PriorityQueue<>();
    private long sequence = 0;
    private boolean shutdown = false;

    public SimScheduler(MockTime time) {
        this.time = time;
    }

    public MockTime time() {
        return time;
    }

    /**
     * Run tasks (advancing virtual time) until the condition holds, the queue empties, or
     * the virtual-time cap is exceeded (which throws — a simulated hang).
     */
    public void runUntil(BooleanSupplier condition, long maxVirtualTimeMs) {
        while (!condition.getAsBoolean()) {
            SimTask task = tasks.poll();
            if (task == null)
                throw new IllegalStateException(
                    "Simulation is idle at t=" + time.milliseconds() + " but the condition never held");
            if (task.cancelled)
                continue;
            if (task.dueMs > time.milliseconds()) {
                if (task.dueMs > maxVirtualTimeMs)
                    throw new IllegalStateException("Virtual-time cap " + maxVirtualTimeMs
                        + "ms exceeded at t=" + time.milliseconds() + " — simulated hang");
                time.sleep(task.dueMs - time.milliseconds());
            }
            task.run();
        }
    }

    public boolean hasPending() {
        return tasks.stream().anyMatch(t -> !t.cancelled);
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        if (shutdown)
            throw new java.util.concurrent.RejectedExecutionException("SimScheduler is shut down");
        SimTask task = new SimTask(time.milliseconds() + Math.max(0, unit.toMillis(delay)),
            sequence++, command);
        tasks.add(task);
        return task;
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException("Callable scheduling is not used by the v2 stack");
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        throw new UnsupportedOperationException("Fixed-rate scheduling is not used by the v2 stack");
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        throw new UnsupportedOperationException("Fixed-delay scheduling is not used by the v2 stack");
    }

    @Override
    public void execute(Runnable command) {
        schedule(command, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown = true;
        tasks.clear();
        return List.of();
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown && tasks.isEmpty();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
    }

    private final class SimTask implements ScheduledFuture<Object>, Runnable, Comparable<Delayed> {
        final long dueMs;
        final long seq;
        final Runnable command;
        boolean cancelled = false;
        boolean done = false;

        SimTask(long dueMs, long seq, Runnable command) {
            this.dueMs = dueMs;
            this.seq = seq;
            this.command = command;
        }

        @Override
        public void run() {
            done = true;
            command.run();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(dueMs - time.milliseconds(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            if (other instanceof SimTask task) {
                int byTime = Long.compare(dueMs, task.dueMs);
                return byTime != 0 ? byTime : Long.compare(seq, task.seq);
            }
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (done)
                return false;
            cancelled = true;
            return true;
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isDone() {
            return done || cancelled;
        }

        @Override
        public Object get() {
            throw new UnsupportedOperationException("SimTask results are not observable");
        }

        @Override
        public Object get(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException("SimTask results are not observable");
        }
    }
}
