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
package org.apache.kafka.clients.v2.producer;

import org.apache.kafka.common.errors.TimeoutException;

/**
 * The producer's memory <em>budget</em>, separated from buffer <em>reuse</em> (design decision
 * D9): this class only accounts bytes; allocation happens elsewhere.
 *
 * <p>{@link #acquireNow} never blocks — {@code send()} in the v2 producer is non-blocking by
 * contract, so budget exhaustion surfaces as a failed future rather than a stalled caller
 * thread. (An awaitable, FIFO-fair asynchronous acquire is the planned extension for callers
 * that prefer backpressure to failure.)
 */
public final class MemoryLimiter {

    private final long capacity;
    private long available;

    public MemoryLimiter(long capacity) {
        this.capacity = capacity;
        this.available = capacity;
    }

    /** @throws TimeoutException if the budget cannot cover the request right now */
    synchronized void acquireNow(int bytes) {
        if (bytes > capacity)
            throw new IllegalArgumentException("Allocation of " + bytes
                + " bytes exceeds the total buffer memory " + capacity);
        if (bytes > available)
            throw new TimeoutException("Producer buffer memory exhausted: requested " + bytes
                + " bytes, available " + available + " of " + capacity);
        available -= bytes;
    }

    /**
     * Non-throwing variant used to fund incremental batch growth (backpressure sealing):
     * a refusal just stops the batch from growing, it is not an error.
     *
     * @return true if the bytes were acquired
     */
    synchronized boolean tryAcquire(int bytes) {
        if (bytes > available)
            return false;
        available -= bytes;
        return true;
    }

    synchronized void release(int bytes) {
        available = Math.min(capacity, available + bytes);
    }

    synchronized long available() {
        return available;
    }
}
