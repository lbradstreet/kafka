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

import java.util.Map;

/**
 * Channel security for a transport connection.
 *
 * <p>TLS reuses Kafka's own {@code SslFactory}/{@code SslEngineFactory} machinery (including
 * {@code ssl.engine.factory.class}, KIP-519) to build the {@code SSLEngine}; Netty's
 * {@code SslHandler} only drives the engine. SASL is not yet implemented in this transport
 * (see DESIGN.md phase plan).
 */
public sealed interface SecuritySpec {

    Plaintext PLAINTEXT = new Plaintext();

    record Plaintext() implements SecuritySpec { }

    /**
     * @param sslConfigs the {@code ssl.*} client configs understood by
     *                   {@code org.apache.kafka.common.config.SslConfigs}
     */
    record Tls(Map<String, Object> sslConfigs) implements SecuritySpec { }
}
