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

/**
 * A broker's quota-throttle decision: how long (in ms) to report a produce request as
 * throttled via {@code throttle_time_ms} (KIP-219). The broker responds promptly with this
 * value stamped in the response; a well-behaved client then pauses sending on the connection
 * for the window. This is distinct from {@link BrokerTimingModel} processing latency (which
 * delays the response itself without a reported field).
 */
@FunctionalInterface
public interface ThrottleModel {

    /** No throttling — the default. */
    ThrottleModel NONE = produceCount -> 0;

    /**
     * @param produceCount how many produce requests this broker has handled (1-based, current)
     * @return throttle time in milliseconds to report on this response
     */
    int throttleMs(int produceCount);

    /** Constant throttle on every produce request. */
    static ThrottleModel constant(int throttleMs) {
        return n -> throttleMs;
    }

    /** No throttle until {@code threshold} produce requests handled, then a fixed throttle. */
    static ThrottleModel after(int threshold, int throttleMs) {
        return n -> n > threshold ? throttleMs : 0;
    }
}
