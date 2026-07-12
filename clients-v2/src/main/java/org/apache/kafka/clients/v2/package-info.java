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
 * The v2 Kafka client stack: a modern, {@code CompletableFuture}-first Java API built on the
 * Netty transport in {@code org.apache.kafka.network.netty}.
 *
 * <p>Wire-protocol behavior is identical to the classic clients; the API surface is not. See
 * {@code docs/netty-redesign/DESIGN.md} for the architecture, decision log (D1&ndash;D15) and
 * phase plan. Idempotence and transactions are intentionally not implemented yet (D15) — the
 * seams they will occupy ({@code BatchSealer}, {@code SEALED} batch lifecycle) exist as no-ops.
 *
 * <p>The classic client implementation under {@code org.apache.kafka.clients.**} must not be
 * imported here (enforced by checkstyle import-control); only the shared
 * {@code org.apache.kafka.common.**} layers are reused.
 */
package org.apache.kafka.clients.v2;
