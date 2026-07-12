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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Per-connection settings for {@link ClientTransport#connect}.
 *
 * @param clientId          the client id placed in request headers
 * @param security          channel security (plaintext or TLS)
 * @param requestTimeout    how long a request may remain unanswered before the connection is
 *                          considered broken (mirrors {@code request.timeout.ms})
 * @param connectTimeout    TCP connect (and TLS handshake) timeout
 * @param maxInFlight       maximum requests awaiting a response per connection; additional
 *                          requests queue locally and are written as the window opens
 *                          (mirrors {@code max.in.flight.requests.per.connection})
 * @param maxReceiveBytes   maximum accepted response frame size
 * @param sendBufferBytes   socket send buffer, or {@link #USE_DEFAULT_BUFFER_SIZE}
 * @param receiveBufferBytes socket receive buffer, or {@link #USE_DEFAULT_BUFFER_SIZE}
 * @param callbackExecutor  executor on which user-visible futures complete; never the event loop
 *                          (design decision D10)
 */
public record ConnectionSpec(
    String clientId,
    SecuritySpec security,
    Duration requestTimeout,
    Duration connectTimeout,
    int maxInFlight,
    int maxReceiveBytes,
    int sendBufferBytes,
    int receiveBufferBytes,
    Executor callbackExecutor
) {

    public static final int USE_DEFAULT_BUFFER_SIZE = -1;

    public ConnectionSpec {
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(security, "security");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        Objects.requireNonNull(connectTimeout, "connectTimeout");
        Objects.requireNonNull(callbackExecutor, "callbackExecutor");
        if (maxInFlight <= 0)
            throw new IllegalArgumentException("maxInFlight must be positive: " + maxInFlight);
        if (maxReceiveBytes <= 0)
            throw new IllegalArgumentException("maxReceiveBytes must be positive: " + maxReceiveBytes);
    }

    public static Builder newBuilder(String clientId) {
        return new Builder(clientId);
    }

    public static final class Builder {
        private final String clientId;
        private SecuritySpec security = SecuritySpec.PLAINTEXT;
        private Duration requestTimeout = Duration.ofSeconds(30);
        private Duration connectTimeout = Duration.ofSeconds(30);
        private int maxInFlight = 5;
        private int maxReceiveBytes = 100 * 1024 * 1024;
        private int sendBufferBytes = USE_DEFAULT_BUFFER_SIZE;
        private int receiveBufferBytes = USE_DEFAULT_BUFFER_SIZE;
        private Executor callbackExecutor = Runnable::run;

        private Builder(String clientId) {
            this.clientId = clientId;
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

        public Builder maxInFlight(int maxInFlight) {
            this.maxInFlight = maxInFlight;
            return this;
        }

        public Builder maxReceiveBytes(int maxReceiveBytes) {
            this.maxReceiveBytes = maxReceiveBytes;
            return this;
        }

        public Builder sendBufferBytes(int bytes) {
            this.sendBufferBytes = bytes;
            return this;
        }

        public Builder receiveBufferBytes(int bytes) {
            this.receiveBufferBytes = bytes;
            return this;
        }

        public Builder callbackExecutor(Executor executor) {
            this.callbackExecutor = executor;
            return this;
        }

        public ConnectionSpec build() {
            return new ConnectionSpec(clientId, security, requestTimeout, connectTimeout,
                maxInFlight, maxReceiveBytes, sendBufferBytes, receiveBufferBytes, callbackExecutor);
        }
    }
}
