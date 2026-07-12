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

/**
 * A Netty-based client transport for the Kafka wire protocol.
 *
 * <p>This module replaces the hand-rolled NIO reactor in {@code org.apache.kafka.common.network}
 * for the v2 client stack while keeping the wire protocol byte-identical: the 4-byte length
 * framing, ApiVersions negotiation, request header versioning and response correlation semantics
 * all match the classic {@code NetworkClient}.
 *
 * <p>Design notes (see {@code docs/netty-redesign/DESIGN.md}):
 * <ul>
 *   <li>{@link org.apache.kafka.network.netty.ClientTransport} is the async SPI; the production
 *       implementation is {@link org.apache.kafka.network.netty.NettyClientTransport}.</li>
 *   <li>All time, scheduling and randomness flow through injectable abstractions so the whole
 *       stack can run under a deterministic simulation harness (design decision D13).</li>
 *   <li>Responses are matched to requests positionally (FIFO per connection) and the correlation
 *       id is verified, exactly like the classic client (design decision D8).</li>
 *   <li>User-facing futures never complete on event-loop threads (design decision D10).</li>
 * </ul>
 */
package org.apache.kafka.network.netty;
