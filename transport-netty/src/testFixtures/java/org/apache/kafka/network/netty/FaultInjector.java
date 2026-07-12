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

import java.util.Random;

/**
 * Buggify-style fault injection, driven by a single seeded {@link Random} so every fault
 * sequence reproduces from the scenario seed (D13). Faults are injected only at the
 * simulated-network layer — never above it — so the transport's FIFO correlation contract
 * is genuinely exercised, not silently violated.
 */
public final class FaultInjector {

    /**
     * @param dropRequestProbability  chance a client→broker frame vanishes (client times out)
     * @param dropResponseProbability chance a broker→client frame vanishes
     * @param disconnectProbability   chance a frame event instead kills the connection
     * @param maxExtraDelayMs         uniform extra delivery latency, order-preserving per connection
     */
    public record FaultProfile(double dropRequestProbability,
                               double dropResponseProbability,
                               double disconnectProbability,
                               long maxExtraDelayMs) {

        public static final FaultProfile NONE = new FaultProfile(0, 0, 0, 0);

        public boolean quiet() {
            return dropRequestProbability == 0 && dropResponseProbability == 0
                && disconnectProbability == 0 && maxExtraDelayMs == 0;
        }
    }

    private final Random random;
    private final FaultProfile profile;
    private final SimTrace trace;
    private int faultsInjected = 0;

    public FaultInjector(long seed, FaultProfile profile, SimTrace trace) {
        this.random = new Random(seed);
        this.profile = profile;
        this.trace = trace;
    }

    public boolean dropRequest(String connectionId) {
        return fire(profile.dropRequestProbability(), "drop-request conn=" + connectionId);
    }

    public boolean dropResponse(String connectionId) {
        return fire(profile.dropResponseProbability(), "drop-response conn=" + connectionId);
    }

    public boolean disconnect(String connectionId) {
        return fire(profile.disconnectProbability(), "disconnect conn=" + connectionId);
    }

    public long extraDelayMs() {
        if (profile.maxExtraDelayMs() == 0)
            return 0;
        return random.nextLong(profile.maxExtraDelayMs() + 1);
    }

    public int faultsInjected() {
        return faultsInjected;
    }

    private boolean fire(double probability, String event) {
        if (probability > 0 && random.nextDouble() < probability) {
            faultsInjected++;
            trace.add("fault " + event);
            return true;
        }
        return false;
    }
}
