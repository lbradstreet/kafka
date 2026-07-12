# Trunk architecture verification memo

Verified against apache/kafka trunk @ `ac0d7e245b` (2026-07-10). This memo is the
factual baseline for [DESIGN.md](DESIGN.md); every design phase cites these findings
rather than assumptions carried over from older releases. File paths are repo-relative.

## 1. Common network layer (`org.apache.kafka.common.network`, `:clients`)

- **`Selectable`** (`clients/src/main/java/org/apache/kafka/common/network/Selectable.java`)
  is the adapter contract for the Netty `Selectable` shim. Exact surface:
  `connect(id, addr, sndBuf, rcvBuf)`, `wakeup()`, `close()`, `close(id)`,
  **`send(NetworkSend)`** (takes `NetworkSend`, not `Send`), `poll(timeout)`,
  `List<NetworkSend> completedSends()`, **`Collection<NetworkReceive> completedReceives()`**
  (backed by a `LinkedHashMap<String, NetworkReceive>`; at most one receive per channel
  per poll), `Map<String, ChannelState> disconnected()`, `List<String> connected()`,
  `mute/unmute/muteAll/unmuteAll`, `isChannelReady(id)`, `USE_DEFAULT_BUFFER_SIZE = -1`.
- **`stagedReceives` is gone.** `Selector` enforces one-receive-per-channel-per-poll
  structurally via the `completedReceives` map (`Selector.java:111`, comment ~line 434).
- **Mute state machine** (`KafkaChannel.java:84-114`):
  `ChannelMuteState = NOT_MUTED | MUTED | MUTED_AND_RESPONSE_PENDING | MUTED_AND_THROTTLED |
  MUTED_AND_THROTTLED_AND_RESPONSE_PENDING`; events
  `REQUEST_RECEIVED | RESPONSE_SENT | THROTTLE_STARTED | THROTTLE_ENDED`.
  `SocketServer` fires `REQUEST_RECEIVED` on request read (`SocketServer.scala:1042`),
  `RESPONSE_SENT` on send completion (`:950`, `:1073`), `THROTTLE_STARTED`/`THROTTLE_ENDED`
  (`:960`, `:964`). The Netty adapter must reproduce these transitions exactly.
- **`Send.writeTo(TransferableChannel)`** (`Send.java`); `TransferableChannel extends
  GatheringByteChannel` adds `hasPendingWrites()` and
  `transferFrom(FileChannel, position, count)` (sendfile via `FileChannel.transferTo`).
  `NetworkSend` is now a thin decorator `(destinationId, Send)`; `ByteBufferSend` has a
  `sizePrefixed(ByteBuffer)` factory.
- **`SendBuilder`** moved to `org.apache.kafka.common.protocol.SendBuilder`
  (implements `Writable`). `AbstractRequest.toSend(header)` →
  `SendBuilder.buildRequestSend(header, data())` (`AbstractRequest.java:110`). It sizes via
  `MessageSizeAccumulator` + `ObjectSerializationCache`, allocates one scratch buffer of
  `sizeExcludingZeroCopy() + 4`, writes the 4-byte frame + header + body, and **retains
  zero-copy fields (`writeByteBuffer`, `writeRecords`) by reference**. Result is one `Send`
  or a `MultiRecordsSend` (`common/record/internal/MultiRecordsSend.java`); file-backed
  records become `RecordsSend`/`DefaultRecordsSend`, and `FileRecords.writeTo`
  (`common/record/internal/FileRecords.java:291-302`) does the sendfile call.
- **Records classes moved**: `org.apache.kafka.common.record.internal.*` now holds
  `RecordBatch`, `MemoryRecordsBuilder`, `CompressionType`, `CompressionRatioEstimator`,
  `FileRecords`, `RecordsSend`, `MultiRecordsSend`, etc. `BufferSupplier` moved to
  `org.apache.kafka.common.utils.internals.BufferSupplier`.

## 2. `NetworkClient` (`clients/src/main/java/org/apache/kafka/clients/NetworkClient.java`)

- `CHECKING_API_VERSIONS` still exists. `handleConnections()` (`:1106`) queues
  `ApiVersionsRequest` when `discoverBrokerVersions`; `handleInitiateApiVersionRequests`
  (`:1122`) transitions the connection state; `handleApiVersionsResponse` (`:1037`)
  completes readiness. KIP-1242: cluster/node id may be attached (`:1138-1145`).
- Request version via `NodeApiVersions.latestUsableVersion` in `doSend` (`:578-596`);
  **header version via `ApiKeys.requestHeaderVersion(apiVersion)`**
  (`RequestHeader.java:44,138`) — this selects flexible (tagged-field) header v2.
- **Correlation is still FIFO positional**: `handleCompletedReceives` (`:1008`) pairs each
  receive with `inFlightRequests.completeNext(source)`; mismatch throws
  `CorrelationIdMismatchException` (handled `:843-853`) — an exception, not an assert —
  with a special case for the SASL **reserved correlation-id range**
  (`SaslClientAuthenticator.isReserved`).
- `InFlightRequests` still `Map<String, ArrayDeque<InFlightRequest>>` + atomic count.

## 3. SASL (`clients/src/main/java/org/apache/kafka/common/security/authenticator/SaslClientAuthenticator.java`)

- `SaslState`: `SEND_APIVERSIONS_REQUEST, RECEIVE_APIVERSIONS_RESPONSE,
  SEND_HANDSHAKE_REQUEST, RECEIVE_HANDSHAKE_RESPONSE, INITIAL, INTERMEDIATE,
  CLIENT_COMPLETE, COMPLETE, FAILED` **plus KIP-368 reauth states**
  `REAUTH_PROCESS_ORIG_APIVERSIONS_RESPONSE, REAUTH_SEND_HANDSHAKE_REQUEST,
  REAUTH_RECEIVE_HANDSHAKE_OR_OTHER_RESPONSE, REAUTH_INITIAL`.
- Reauth is initiated by `KafkaChannel.maybeBeginClientReauthentication` (`:597-615`) only
  when `NOT_MUTED && !midWrite && now >= clientSessionReauthenticationTimeNanos`;
  `swapAuthenticatorsAndBeginReauthentication` (`:673-678`) installs a fresh authenticator.
- Responses to pre-reauth in-flight requests arriving mid-reauth are buffered in
  `ReauthInfo.pendingAuthenticatedReceives` and drained via
  `pollResponseReceivedDuringReauthentication()`. Reauth traffic uses reserved
  correlation ids (`MIN/MAX_RESERVED_CORRELATION_ID`).
- The authenticator sends its own `ApiVersionsRequest` **hardcoded at v0** (`:247-248`);
  `NetworkClient` sends a second, version-negotiated `ApiVersionsRequest` after the channel
  is ready. Both must be reproduced.

## 4. TLS (KIP-519)

- `SslEngineFactory` is public API at
  `clients/src/main/java/org/apache/kafka/common/security/auth/SslEngineFactory.java`
  (`createClientSslEngine(host, port, endpointIdentification)`, `createServerSslEngine`,
  `shouldBeRebuilt`, `reconfigurableConfigs`, `keystore`, `truststore`).
- `SslFactory.instantiateSslEngineFactory` (`ssl/SslFactory.java:135-141`) reads
  `ssl.engine.factory.class`, defaults to `DefaultSslEngineFactory`, supports dynamic
  reconfiguration. **Consequence for Netty**: construct `SslHandler(sslEngineFactory.create*SslEngine(...))`
  around the Kafka-created engine; never let Netty build its own `SslContext`.

## 5. Memory pools

- The client-facing `Selector` constructor **hardcodes `MemoryPool.NONE`**
  (`Selector.java:213`); the pooled constructor (`:157`) is broker-only, with
  `lowMemThreshold = 0.1 * pool.size()`.
- Broker installs `SimpleMemoryPool(queuedMaxBytes, socketRequestMaxBytes, ...)` only when
  `queued.max.bytes > 0` (`SocketServer.scala:100`), threaded into `KafkaChannel`.

## 6. Broker (`:core` Scala + `:server` Java)

- `SocketServer.scala` (1714 lines) is still the acceptor/processor engine.
  **Control-plane acceptors are gone** — only `DataPlaneAcceptor` (`:363`); controller and
  broker listeners both use `createDataPlaneAcceptorAndProcessors` (`:150-152`).
- `Processor` (`:800`) owns a common-network `Selector` (`createSelector` `:867`).
- **KAFKA-20451**: request/response DTOs moved to the Java `:server` module, package
  `org.apache.kafka.network` (`BaseRequest`, `Request`, `Response`, `SendResponse`,
  `Session`, `SocketServerConfigs`, metrics). `RequestChannel.scala` (202 lines) is now a
  thin Scala queue/dispatch wrapper. A small helper `SocketServer.java` also lives there.
- Internal clients still build `Selector` + `NetworkClient` directly:
  `server/src/main/java/org/apache/kafka/server/NodeToControllerChannelManagerImpl.java`
  (`:104`, `:115`) and `core/src/main/scala/kafka/server/BrokerBlockingSender.scala`
  (`:71`, `:82`). A transport switch must be threaded into `Processor.createSelector` and
  these two builders.
- New config keys belong in a Java `*Configs` class in `:server` (alongside
  `SocketServerConfigs`), surfaced through `AbstractKafkaConfig`
  (`server/src/main/java/org/apache/kafka/server/config/AbstractKafkaConfig.java`).

## 7. Producer internals (`:clients`)

- `RecordAccumulator` append/drain shape unchanged;
  **sequences are assigned at drain time** in `drainBatchesForOneNode`
  (`RecordAccumulator.java:917-933`): `maybeUpdateProducerIdAndEpoch` →
  `batch.setProducerState(pidEpoch, txnMgr.sequenceNumber(tp), isTxn)` →
  `incrementSequenceNumber` → `addInFlightBatch`. This is exactly the `BatchSealer` seam
  in the v2 design (D15).
- `BuiltInPartitioner` (KIP-794) is per-topic, not a `Partitioner` impl; size-based sticky
  switching (`stickyBatchSize`), uniform or queue-weighted choice
  (`partitioner.adaptive.partitioning.enable`, forced off with a custom partitioner),
  rack-aware filtering (KIP-881).
- `BufferPool` unchanged (free list of `batch.size` buffers + FIFO condition waiters).
- `ByteBufferOutputStream` still reallocs at 1.1x and copies (`expandBuffer`,
  `ByteBufferOutputStream.java:120`); `CompressionRatioEstimator` still drives sizing.
- `KafkaProducer.send` still returns `java.util.concurrent.Future<RecordMetadata>`
  (`KafkaProducer.java:975`, `:1094`); **no CompletableFuture producer API exists in trunk**.

## 8. Consumer internals (`:clients`)

- `AsyncKafkaConsumer` + background `ConsumerNetworkThread` (KIP-848 split) confirmed.
  `runOnce()` drains application events, polls each manager, hands `PollResult`s to
  `NetworkClientDelegate`.
- **An interface literally named `RequestManager` exists**
  (`consumer/internals/RequestManager.java`) — the v2 dispatcher is therefore named
  `NetworkRequestDispatcher`. Managers: `CoordinatorRequestManager`,
  `CommitRequestManager`, `OffsetsRequestManager`, `TopicMetadataRequestManager`,
  `FetchRequestManager`, heartbeat/membership managers, share/streams variants.
- Managers perform **no I/O** in `poll()` — they return
  `NetworkClientDelegate.PollResult` / `UnsentRequest`. The state machines are reusable
  over a different transport if those envelope types are abstracted; some managers also
  hold a direct `NetworkClientDelegate` reference (`FetchRequestManager`,
  `OffsetsRequestManager`).
- Fetch: `FetchBuffer`/`FetchCollector`/`CompletedFetch`; decompression is lazy at
  iteration (`CompletedFetch.nextFetchedRecord` → `streamingIterator(bufferSupplier)`).

## 9. Compression (`org.apache.kafka.common.compress`)

- `Compression` interface + per-codec builders with KIP-390 `level(int)` —
  `NoCompression, GzipCompression, SnappyCompression, Lz4Compression, ZstdCompression`.
  Level bounds live on `record.internal.CompressionType`.
- Zstd already avoids finalizers (`Zstd*StreamNoFinalizer`) and bridges zstd-jni's
  `BufferPool` onto Kafka's `BufferSupplier` for decompression; output side uses
  `RecyclingBufferPool` + 16 KB buffering. **Not yet present**: compression-context
  (`ZstdCompressCtx`) reuse, gzip `Deflater` reuse, chunked (non-realloc) output sinks —
  these remain the improvement surface for Phase 8.

## 10. Admin

- `AdminApiDriver` / `AdminApiHandler` / `AdminApiLookupStrategy`
  (`clients/src/main/java/org/apache/kafka/clients/admin/internals/`) — lookup→fulfillment
  pipeline confirmed, reusable over a new transport.
- `KafkaFuture.toCompletionStage()` exists (`KafkaFuture.java:112`).

## 11. Build system

- `build.gradle`: `minClientJavaVersion = 11`, `minNonClientJavaVersion = 17`;
  per-module `options.release` (no toolchain blocks). New modules simply stay off the
  `modulesNeedingJava11` list to get **Java 17**.
- Scala `2.13.18` only.
- **Netty appears nowhere** in any gradle file — it is a net-new dependency (Connect uses
  Jetty, unrelated).
- Message generation: per-module `processMessages` (JavaExec of
  `org.apache.kafka.message.MessageGenerator` from `:generator`) with JSON specs in
  `src/main/resources/common/message`; `:clients` generates
  `org.apache.kafka.common.message` into `clients/src/generated` (absent until built).
  `metadata`, `raft`, `storage`, and the coordinators generate their own messages.
- Checkstyle import-control: per-module `checkstyle/import-control-<module>.xml` +
  `configProperties = checkstyleConfigProperties("import-control-<module>.xml")` in the
  module's block in `build.gradle` — the mechanism for the v2 dependency rules.

## Corrections applied to the design (vs. pre-verification draft)

1. All references to `org.apache.kafka.common.record.*` low-level types updated to
   `record.internal.*`; `BufferSupplier` to `utils.internals`.
2. The `Selectable` adapter must implement `send(NetworkSend)` and expose
   `Collection<NetworkReceive>`; mute adapter must implement the 5-state machine.
3. The Netty encoder integrates with `SendBuilder` output (a `Send` possibly containing
   zero-copy `MultiRecordsSend` components), not raw `ByteBuffer[]`.
4. Correlation mismatch handling models `CorrelationIdMismatchException` + the SASL
   reserved-id range, not a plain assert.
5. Reauth interleaving must buffer authenticated receives during reauth
   (`pendingAuthenticatedReceives` semantics).
6. `network.transport.impl` config lands in `:server`'s `SocketServerConfigs` (or sibling),
   and the switch also threads into `NodeToControllerChannelManagerImpl` and
   `BrokerBlockingSender`.
7. v2 request layer named `NetworkRequestDispatcher` (trunk already has `RequestManager`).
8. Consumer reuse strategy: abstract the `NetworkClientDelegate.PollResult`/`UnsentRequest`
   envelope behind a v2 interface so KIP-848 manager state machines can be ported with
   minimal changes.
