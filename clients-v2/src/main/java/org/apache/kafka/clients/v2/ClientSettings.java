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

import org.apache.kafka.network.netty.SecuritySpec;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Connection-level settings shared by every v2 client (producer, consumer, admin).
 *
 * @param bootstrapServers initial broker addresses
 * @param clientId         client id placed in request headers
 * @param security         channel security (plaintext or TLS)
 * @param requestTimeout   per-request timeout (mirrors {@code request.timeout.ms})
 * @param connectTimeout   TCP connect timeout
 * @param metadataMaxAge   how long cached metadata stays fresh (mirrors {@code metadata.max.age.ms})
 * @param retryBackoff     delay between retries of retriable operations
 * @param maxInFlight      per-connection in-flight request window
 */
public record ClientSettings(
    List<InetSocketAddress> bootstrapServers,
    String clientId,
    SecuritySpec security,
    Duration requestTimeout,
    Duration connectTimeout,
    Duration metadataMaxAge,
    Duration retryBackoff,
    int maxInFlight
) {

    public ClientSettings {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        if (bootstrapServers.isEmpty())
            throw new IllegalArgumentException("bootstrapServers must not be empty");
        bootstrapServers = List.copyOf(bootstrapServers);
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(security, "security");
    }

    public static Builder newBuilder(String bootstrap) {
        return new Builder(parseBootstrap(bootstrap));
    }

    private static List<InetSocketAddress> parseBootstrap(String bootstrap) {
        return List.of(bootstrap.split(",")).stream()
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(hostPort -> {
                int colon = hostPort.lastIndexOf(':');
                if (colon < 0)
                    throw new IllegalArgumentException("Bootstrap server must be host:port, got: " + hostPort);
                return InetSocketAddress.createUnresolved(
                    hostPort.substring(0, colon), Integer.parseInt(hostPort.substring(colon + 1)));
            })
            .toList();
    }

    public static final class Builder {
        private final List<InetSocketAddress> bootstrapServers;
        private String clientId = "kafka-clients-v2";
        private SecuritySpec security = SecuritySpec.PLAINTEXT;
        private Duration requestTimeout = Duration.ofSeconds(30);
        private Duration connectTimeout = Duration.ofSeconds(30);
        private Duration metadataMaxAge = Duration.ofMinutes(5);
        private Duration retryBackoff = Duration.ofMillis(100);
        private int maxInFlight = 5;

        private Builder(List<InetSocketAddress> bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder security(SecuritySpec security) {
            this.security = security;
            return this;
        }

        public Builder requestTimeout(Duration timeout) {
            this.requestTimeout = timeout;
            return this;
        }

        public Builder connectTimeout(Duration timeout) {
            this.connectTimeout = timeout;
            return this;
        }

        public Builder metadataMaxAge(Duration maxAge) {
            this.metadataMaxAge = maxAge;
            return this;
        }

        public Builder retryBackoff(Duration backoff) {
            this.retryBackoff = backoff;
            return this;
        }

        public Builder maxInFlight(int maxInFlight) {
            this.maxInFlight = maxInFlight;
            return this;
        }

        public ClientSettings build() {
            return new ClientSettings(bootstrapServers, clientId, security, requestTimeout,
                connectTimeout, metadataMaxAge, retryBackoff, maxInFlight);
        }
    }
}
