<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Kafka on Netty: modern client stack redesign (clients-v2)

Status: **in progress — first working implementation landed** · Baseline: trunk @
`ac0d7e245b` (2026-07-10) · Facts verified in [trunk-verification.md](trunk-verification.md)
(cited below as **[TV §n]**).

## 0. Implementation status

Landed on this branch (verified end-to-end against a real single-node KRaft broker built
from this tree — admin describeCluster/createTopic, 500 records produced with LZ4 across
keyed/late-bound assignments, consumed back and compared exactly):

- `:transport-netty` — `ClientTransport`/`KafkaConnection`/`NettyClientTransport`,
  length-field framing, FIFO correlation + correlation-id verification (D8), ApiVersions
  negotiation with v0 downgrade, in-flight window with eager write-ahead (goal 5), request
  timeouts, TLS wired through `SslFactory`-created engines (KIP-519, untested), zero-copy
  `Send`→`ByteBuf` conversion. `EmbeddedChannel` unit tests.
- `:clients-v2` — `KafkaClientRuntime` (D10/D13 seams), `NetworkRequestDispatcher`,
  `MetadataManager`; producer with `RecordAccumulatorV2` (one batch per partition per
  request), late binding (D14), no-op `BatchSealer` (D15), `MemoryLimiter` (D9, fail-fast
  variant); assignment-based consumer (fetch v12, sessionless); minimal admin.
- **DST harness (Phase 2b, D13)** — landed. The sim runtime ships as `:transport-netty`
  test fixtures (same package as the transport so the *real* `NettyKafkaConnection` +
  frame decoder run unmodified over `EmbeddedChannel`s): `SimScheduler` (virtual-time
  single task queue driving `MockTime`), `SimTransport`/`SimNetwork` (order-preserving
  per-connection delivery; seeded `FaultInjector` for drops, delays, disconnects),
  protocol-accurate `SimBroker` (ApiVersions/Metadata/Produce with real `MemoryRecords`
  CRC validation; deterministic topic ids; controllable leadership moves) and a
  `SimTrace`. `DstHarness` (clients-v2 tests) wires it through the existing
  `KafkaClientRuntime` builder seams. Determinism required two production changes:
  `SenderV2` became a self-rescheduling async tick on the runtime scheduler (no drain
  thread, no blocking metadata waits; `closeAsync()` added), and request timeouts gained
  a scheduler seam in `ConnectionSpec`. Tests: 20-seed quiet-network exactly-once sweep,
  same-seed trace-equality under faults, 20-seed faulty-network liveness +
  no-invented-acks sweep, and a D14 rebind-under-total-leader-failover scenario — all in
  seconds of wall clock.

**Scope note:** this effort is producer-focused. Consumer groups, fetch sessions and other
consumer-side depth are **deferred indefinitely** — the assign-based consumer exists only
as verification tooling (and the DST harness verifies against the SimBroker log directly,
needing no consumer at all).

Not yet implemented (per phase plan below): SASL handshake handler, `Selectable` adapter +
broker flag, pooled receive payloads (responses are copied to heap before parse —
classic-client parity), chunked compression sink, acks=0 fire-and-forget, metrics.

## 1. Goals

1. A **modern Java 17 client API** (`kafka-clients-v2`): records, sealed interfaces,
   `CompletableFuture`-first, typed configs. Breaking source changes from classic clients
   are acceptable; classic clients keep working unchanged during the transition.
2. A **Netty-based transport** replacing the hand-rolled NIO layer in
   `org.apache.kafka.common.network` for v2 clients, with the broker able to run on it
   behind a flag via an adapter.
3. **Improved buffer pooling**: pooled allocation end-to-end (`PooledByteBufAllocator`),
   eliminating per-receive heap allocation, exact-batch.size-only recycling, and
   realloc-and-copy growth.
4. **Improved compression**: pooled codec workspaces, chunked output (no copy-on-grow),
   per-codec levels, decompression into pooled buffers.
5. **Improved pipelining**: serialize/write request N+1 while N flushes; configurable
   in-flight windows; per-connection FIFO correlation preserved.
6. **Deterministic simulation testing (DST)**: the whole v2/transport stack runs on an
   FDB-style deterministic runtime — virtual time, seeded randomness, simulated network,
   buggify fault injection, seed-reproducible failures.
7. **Flexible partitioning**: an opt-in mode where records are not fixed to a partition at
   append; binding happens at drain time and can be revised before a batch is sealed.

**Non-goals (this effort):**
- Any wire-protocol change. Framing, ApiVersions negotiation, SASL handshake and
  reauthentication sequences, record batch format v2 remain **byte-identical**.
- Broker request-handling redesign (`SocketServer` threading, `RequestChannel`, quotas,
  ordering semantics stay as-is; only the I/O engine becomes pluggable).
- **Idempotence and transactions in the v2 producer** (explicit deferral — see D15). The
  design reserves every seam they need; classic clients keep full support.
- Removing the existing NIO transport; it remains the broker default throughout.

## 2. Decisions

### D1 — Two-layer transport strategy
A new async `ClientTransport` SPI (Netty-native) serves clients-v2. A separate
`NettySelectableAdapter implements Selectable` serves legacy consumers — the broker's
`Processor`, and the internal clients that construct `Selector` + `NetworkClient` directly
(`NodeToControllerChannelManagerImpl`, `BrokerBlockingSender`) [TV §6].
*Why:* `Selectable`'s poll contract is reactor-shaped; forcing v2 through it forfeits
Netty's pipelining and threading wins, while rewriting the broker against a new SPI is out
of scope.

### D2 — No physical split of `:clients` initially
`:clients-v2` depends on `:clients` and reuses `org.apache.kafka.common.**` verbatim:
generated messages, `requests`, `protocol` (`SendBuilder`, `ApiMessage`,
`ObjectSerializationCache`), `record.internal`, `compress`, `security`, `utils` [TV §1, §9].
Checkstyle import-control (per-module xml, the existing mechanism [TV §11]) bans
`org.apache.kafka.clients.**` inside v2 except an explicitly whitelisted
`org.apache.kafka.clients.v2.compat` package. Physical `:clients-core` extraction is
deferred (Phase 11).

### D3 — New Gradle modules, Java 17
`:transport-netty` (artifact `kafka-transport-netty`, package `org.apache.kafka.network.netty`)
and `:clients-v2` (artifact `kafka-clients-v2`, package root `org.apache.kafka.clients.v2`).
Both target Java 17 — simply not listed in `modulesNeedingJava11` [TV §11]. Netty is a
net-new dependency [TV §11] and stays **off** the classic `kafka-clients` classpath.
`:core`/`:server` gain an optional dependency on `:transport-netty` for the broker flag.

### D4 — v2 request layer is new
`NetworkRequestDispatcher` (named to avoid trunk's existing
`consumer.internals.RequestManager` interface [TV §8]) replaces `NetworkClient`'s upper
half for v2: connection pool, backoff/state machine, least-loaded routing, retries. A thin
`compat/NetworkClientAdapter implements KafkaClient` exists only as a test/migration
vehicle.

### D5 — Consumer is pull-based async primary
`CompletableFuture<ConsumerBatch> poll(PollOptions)` is the primitive; a
`Flow.Publisher` adapter and a handler-style `subscribe` convenience layer on top. The
KIP-848 manager state machines are ported: they already perform no I/O in `poll()` and
return request envelopes [TV §8] — v2 abstracts the
`NetworkClientDelegate.PollResult`/`UnsentRequest` envelope types behind a small
`RequestEnvelope` interface so the managers' logic transfers with minimal change.

### D6 — ByteBuf/ByteBuffer boundary at the record-batch level
Transport, framing, and headers use pooled `ByteBuf`. Record-format code
(`record.internal.MemoryRecords*`) keeps operating on `ByteBuffer` via zero-copy
`ByteBuf.nioBuffer()` on single-component buffers. No rewrite of the record format
implementation — it is the wire-format crown jewels. Composite buffers are used only on
the write path, where `SendBuilder`-style multi-buffer sends already exist [TV §1].

### D7 — Refcounting policy
Pooled `ByteBuf`s never escape to user code. Fetch payloads are refcounted internally;
records are copied out at deserialization, then the payload is released (release ledger +
Netty leak detector `paranoid` in CI). An opt-in zero-copy consumption mode with
`AutoCloseable` batches is a stretch goal.

### D8 — Keep FIFO positional correlation
Responses are matched to the oldest in-flight request per connection, with
`CorrelationIdMismatchException` on id mismatch and the SASL reserved correlation-id range
honored, exactly as `NetworkClient` does today [TV §2]. Map-lookup matching would mask
broker reordering bugs. Pipelining wins come from the write side, not the match side.

### D9 — Producer memory: budget ≠ reuse
Classic `BufferPool` conflates a memory *budget* with buffer *reuse* [TV §7]. v2 splits
them: `MemoryLimiter` (fair FIFO waiters; preserves `buffer.memory`/`max.block.ms`
semantics, async-first acquisition) for budget; `PooledByteBufAllocator` for reuse —
which, unlike `BufferPool`, also pools non-batch.size allocations.

### D10 — Threading
One shared `EventLoopGroup` (epoll where available) per `KafkaClientRuntime`, shareable
across producer/consumer/admin. User futures/callbacks **never complete on event-loop
threads by default**; they complete on a configurable executor (expert direct-executor
opt-in). BlockHound in tests.

### D11 — Broker Netty path behind a flag
`network.transport.impl = nio | netty` (default `nio`), defined in a Java `*Configs` class
in `:server` alongside `SocketServerConfigs` [TV §6], consumed by `Processor.createSelector`
and the two internal-client builders. NIO paths are never deleted in this effort; the
default flips only after differential testing proves parity.

### D12 — Compression: extend trunk's `Compression`
Trunk already has per-codec levels (KIP-390) and zstd finalizer-free streams with a
`BufferSupplier` bridge for decompression [TV §9]. The remaining improvement surface:
- `CompressionWorkspacePool`: zstd compression-context reuse, gzip `Deflater`/`Inflater`
  reuse via `reset()`, pooled lz4 block buffers, snappy buffer reuse.
- A chunked output sink (`ChunkedBufOutputStream extends ByteBufferOutputStream`) so
  compression-estimate misses append a new pooled chunk instead of the 1.1x
  realloc-and-copy in `expandBuffer` [TV §7].
- Dictionary support: config hooks only (`ZstdDictionaryProvider` SPI, wired to no-op) —
  a wire-visible dictionary id requires a future KIP; do not invent wire format.

### D13 — Deterministic simulation is a first-class design constraint
All v2/transport code obtains time, scheduling, randomness, and I/O **only** through
abstractions owned by `KafkaClientRuntime`:

| Abstraction | Production | Simulation |
|---|---|---|
| `Time` (reuse `common.utils.Time`) | `SystemTime` | virtual clock |
| `TaskScheduler` (all delays/timeouts; no direct `eventLoop.schedule`) | event-loop timers | priority queue keyed by `(virtualTime, seq)`, seeded tiebreaks |
| `RandomSource` | seeded-from-entropy | seeded-from-scenario |
| `ClientTransport` | `NettyClientTransport` | `SimTransport` over `EmbeddedChannel`s |
| future-completion executor | user executor | the sim scheduler |

The sim profile wires the **real pipeline handlers** (framing, SASL, ApiVersions,
correlation) into Netty `EmbeddedChannel`s connected by an in-memory `SimNetwork`, so the
code under test is the production code. A `SimBroker` speaks real protocol frames via the
generated messages. A buggify-style `FaultInjector` (seeded drops, delays, reorders —
injected at the SimNetwork level only, so the D8 FIFO invariant is genuinely exercised —
disconnects mid-handshake/mid-reauth, leadership moves, throttles) drives exploration;
every failure reproduces from its seed (`-Dkafka.dst.seed=`).
Determinism rules, lint-enforced (checkstyle regex/ArchUnit) in both new modules: no
`System.currentTimeMillis/nanoTime`, no `new Random()`/`ThreadLocalRandom`, no
unordered-map iteration feeding decisions, no direct thread creation.

### D14 — Flexible/late partition binding (v2 producer)
The v2 partitioner returns a sealed type:

```java
sealed interface PartitionAssignment {
  record Fixed(int partition) implements PartitionAssignment {}
  record Deferred(OptionalInt preferenceHint) implements PartitionAssignment {}
}
```

- `Fixed`: keyed records, explicit-partition sends, and legacy-compatible behavior.
- `Deferred`: records accumulate in **unbound per-topic batch queues**; a batch is bound
  to a concrete partition only at **drain time**, chosen by leader availability, node
  readiness/channel writability, and queue depth (subsumes KIP-794 adaptive logic, whose
  drain-time load statistics [TV §7] this generalizes).
- **Rebinding** (opt-in): a bound-but-unsent batch (leader offline, node unwritable,
  retriable pre-send failure) may be re-enqueued unbound — legal only **before the batch
  passes the `BatchSealer` seam** (D15). Once sealed, a batch keeps its partition forever.
  This boundary is exactly where trunk assigns idempotent sequences at drain
  (`drainBatchesForOneNode` [TV §7]), so the rule stays valid when idempotence lands.
- Keyed records are always `Fixed` unless the user's partitioner explicitly defers them.
- Config: `partitioning.mode = fixed | late-binding`. Documentation must state that
  deferred records carry no per-key ordering guarantee.

### D15 — Idempotence & transactions: deferred, seams reserved
This phase implements **neither** idempotence nor transactions in v2 (classic clients
retain full support; nothing is removed). Reserved seams, mapped onto where classic code
does the equivalent work:

1. **`BatchSealer`** — an explicit no-op stage in the `SenderV2` drain pipeline; the future
   home of producerId/epoch/sequence stamping. Classic equivalent:
   `RecordAccumulator.drainBatchesForOneNode` → `setProducerState` +
   `incrementSequenceNumber` [TV §7]. Batch lifecycle gains a `SEALED` state after which
   partition and content are immutable — also the D14 rebind boundary.
2. **`ProducerIdentity`** — optional (currently always-null) parameter of
   `ProduceRequest` construction, standing in for pid/epoch.
3. **Accumulator bookkeeping** — per-partition in-flight structures shaped to support a
   future 5-deep sequenced window and partition muting (fields already needed for
   `max.in.flight=1` ordering).
4. **API sketch** — `Transaction` exists as a sealed scoped type in the v2 API but throws
   `UnsupportedOperationException`, documented `@Evolving`.
5. **DST** — the "no duplicate sequence per partition" invariant checker ships dormant,
   enabled when the feature lands.

## 3. Architecture

### 3.1 `:transport-netty` (package `org.apache.kafka.network.netty`)

```
ClientTransport (SPI)
  CompletableFuture<KafkaConnection> connect(Node, ConnectionSpec)
KafkaConnection
  CompletableFuture<AbstractResponse> send(AbstractRequest.Builder<?>, RequestOptions)
  NodeApiVersions apiVersions(); boolean isReady(); CompletableFuture<Void> close(reason)
```

Client pipeline (in order):
1. **`SslHandler`** — wraps the `SSLEngine` produced by Kafka's own `SslEngineFactory`
   / `SslFactory` (`ssl.engine.factory.class`, KIP-519) [TV §4]. Netty never builds its
   own `SslContext`; dynamic keystore reconfiguration keeps flowing through `SslFactory`.
2. **`KafkaFrameDecoder`** — `LengthFieldBasedFrameDecoder(maxReceiveSize, 0, 4, 0, 4)`
   (Kafka length prefix excludes itself), retaining pooled payload `ByteBuf`s;
   `InvalidReceiveException` parity for oversize frames.
3. **`KafkaFrameEncoder`** — consumes the `Send` produced by
   `SendBuilder.buildRequestSend` [TV §1]: heap components wrapped as `ByteBuf` without
   copy; zero-copy components (`MultiRecordsSend`, file-backed `RecordsSend`) translated
   to `FileRegion` (plaintext) or chunked reads (TLS — sendfile is already impossible
   under TLS today).
4. **`SaslClientHandshakeHandler`** — self-removing; byte-for-byte replication of
   `SaslClientAuthenticator` [TV §3]: ApiVersionsRequest (hardcoded v0, as today) →
   SaslHandshake → token exchange (SaslAuthenticate-wrapped for handshake v1+, raw frames
   for v0), **including KIP-368 reauthentication**: scheduled via `TaskScheduler` from the
   session lifetime, gated on quiet-channel conditions, buffering authenticated responses
   that arrive mid-reauth (`pendingAuthenticatedReceives` semantics), reserved
   correlation-id range honored. Reuses `SaslClientAuthenticator`'s mechanism/JAAS/login
   plumbing; only the I/O shuttling is new.
5. **`ApiVersionsGate`** — replicates `NetworkClient`'s `CHECKING_API_VERSIONS` [TV §2]:
   post-auth ApiVersionsRequest (version-negotiated, with the v0 downgrade retry and
   KIP-1242 fields), stores `NodeApiVersions`, completes the `connect()` future.
6. **`CorrelationHandler`** — assigns correlation ids; picks request version via
   `latestUsableVersion` and header version via `ApiKeys.requestHeaderVersion`; FIFO
   in-flight deque with positional matching + `CorrelationIdMismatchException` (D8);
   request timeouts via `TaskScheduler` (not poll-driven); enforces the in-flight window
   while **eagerly writing queued requests as slots open** — with Netty's write queue,
   serialization and flush of request N+1 overlap the flush of N (the pipelining win).

`NettyTransportMetrics` reproduces the `selector-metrics` sensor names
(`outgoing-byte-rate`, `request-rate`, per-node sensors; `io-wait-time` analog =
event-loop idle) with documented gaps.

### 3.2 Legacy adapter + broker path (`org.apache.kafka.network.netty.compat`)

- **`NettySelectableAdapter implements Selectable`** — exact trunk surface [TV §1]:
  `send(NetworkSend)`, `Collection<NetworkReceive> completedReceives()` with the
  one-receive-per-channel-per-poll invariant, `Map<String, ChannelState> disconnected()`,
  and the full five-state mute machine (`REQUEST_RECEIVED`/`RESPONSE_SENT`/
  `THROTTLE_STARTED`/`THROTTLE_ENDED` transitions) mapped onto `autoRead` toggling plus
  decoded-frame buffering. Event loops push completions into MPSC queues; `poll(timeout)`
  drains them; `wakeup()` unparks the poller.
- **`NettyServerTransport`** — `ServerBootstrap` per listener (all listeners are
  data-plane [TV §6]); server pipeline mirrors §3.1 with `SaslServerHandshakeHandler`
  (replicating `SaslServerAuthenticator` incl. reauth and `KafkaPrincipalBuilder`);
  honors the `queued.max.bytes` `MemoryPool` budget by gating `autoRead` [TV §5];
  **`DefaultFileRegion` for the sendfile fetch path** (`FileRecords.writeTo` parity
  [TV §1]) — a mandatory deliverable, not an optimization.
- Broker changes are minimal: `Processor.createSelector` goes behind a factory selected by
  `network.transport.impl`; the same switch is threaded into
  `NodeToControllerChannelManagerImpl` and `BrokerBlockingSender` (both consume
  `Selectable`, so this is construction plumbing only) [TV §6]. `RequestChannel` and
  handler threading untouched.

### 3.3 `:clients-v2` (package `org.apache.kafka.clients.v2`)

- **`KafkaClientRuntime`** (AutoCloseable) — owns the D13 abstraction bundle (`Time`,
  `TaskScheduler`, `RandomSource`, transport factory), the `EventLoopGroup`, callback
  executor, and `Metrics`. Shareable across clients; each client keeps its own
  `MetadataManager` and connection set initially (socket sharing across clients deferred:
  metadata epochs and SASL identities make it risky; event-loop sharing captures most of
  the thread-count win).
- **Config**: `record`-based, typed (`Duration`, `DataSize`), sealed
  `SecurityConfig = Plaintext | Tls | SaslPlaintext | SaslTls`; builders; a `compat`
  translation to `AbstractConfig` maps where classic internals are reused.
- **`MetadataManager`** — async refresh over `KafkaConnection`, leader-epoch tracking
  (ported from `org.apache.kafka.clients.Metadata` via the whitelisted compat package).
- **`NetworkRequestDispatcher`** (D4) — node→connection pool, connection state machine,
  least-loaded selection, retries; exposes the `RequestEnvelope` abstraction that ported
  KIP-848 managers consume (D5, [TV §8]).
- **Producer** (`v2.producer`): `AsyncProducer.send(ProducerRecord<K,V>) →
  CompletableFuture<RecordMetadata>`; `flush()`/`close()` return futures.
  `RecordAccumulatorV2` appends through `ChunkedBufOutputStream` (D12) so
  `record.internal.MemoryRecordsBuilder` is reused unmodified; `MemoryLimiter` (D9);
  `SenderV2` on a dedicated drain thread — drain pipeline:
  `bind (D14) → fill → [BatchSealer: no-op seam, D15] → request build → dispatch`,
  with per-node writability backpressure and `max.request.size` caps. Late binding per
  D14. No idempotence/transactions (D15).
- **Consumer** (`v2.consumer`): pull-async `poll` → `ConsumerBatch` (records + commit
  helpers); ported manager state machines; `FetchBuffer` holds refcounted payloads;
  decompression via allocator-backed `BufferSupplier`; ByteBuffer-first `Deserializer`
  API; fetch-session (`FetchSessionHandler`) and rebalance parity.
- **Admin** (`v2.admin`): `CompletableFuture`-native operations (no
  `KafkaFuture`/`*Result` wrappers), porting the `AdminApiDriver`/`AdminApiHandler`
  lookup→fulfillment framework [TV §10]; `KafkaFuture.toCompletionStage()` bridges for
  migration.

### 3.4 Simulation runtime (D13)

`:transport-netty` test-fixtures (or a small `:dst` module): deterministic
`TaskScheduler`, `SimNetwork`/`SimTransport`, scriptable `SimBroker` (metadata, produce,
fetch, group coordination — grown per phase), `FaultInjector`, and a `DstHarness`
(`run(seed, scenario) → trace`). CI runs N random seeds per PR plus nightly sweeps;
trace-based invariant checkers: no committed-offset regression, no future completed twice,
buffer-release ledger balanced, connection FIFO respected (duplicate-sequence checker
dormant per D15). A double-run-same-seed trace-equality job guards against determinism
leaks.

## 4. Phases

Each phase is independently mergeable and keeps the build green.

| # | Phase | Key deliverables |
|---|---|---|
| 0 | Trunk sync + verification | Done: branch on trunk `ac0d7e245b`; [trunk-verification.md](trunk-verification.md) |
| 1 | Build wiring | Netty deps pinned in `gradle/dependencies.gradle`; `:transport-netty`, `:clients-v2` registered (Java 17); import-control files |
| 2 | Netty client transport core | §3.1 complete; golden-bytes differential harness (classic vs Netty traffic byte-compared for identical request sequences: plaintext/TLS/every SASL mechanism incl. reauth); integration tests against unmodified broker; handlers run under `EmbeddedChannel` |
| 2b | Simulation runtime | §3.4: `DstHarness`, `SimBroker`, `FaultInjector`, determinism lint, seed-replay CI |
| 3 | `Selectable` adapter + broker opt-in | §3.2; `network.transport.impl` config in `:server`; full `:core` integration suite under the flag (non-gating CI job initially); compatibility matrix old-client↔netty-broker, netty-client↔unmodified-broker |
| 4 | v2 API surface + runtime | §3.3 skeleton: runtime, configs, metadata, dispatcher; describeCluster smoke over real broker **and** under `DstHarness` with a fixed seed |
| 5 | v2 Producer | Accumulator/limiter/sender per §3.3; late binding (D14); `BatchSealer` no-op seam (D15); golden test: byte-identical `MemoryRecords` vs classic producer (non-idempotent config); DST rebind-under-failover scenarios |
| 6 | v2 Consumer | Ported managers; refcounted fetch path; leak-gated soak; compressed/uncompressed parity vs classic consumer (same offsets, same bytes) |
| 7 | v2 Admin | Topic/config/group/offset operations first; long tail incremental |
| 8 | Compression & pooling deep work | `CompressionWorkspacePool` (upstreamable to classic clients too); `CompressionSpec` sealed config; adaptive decompress sizing; allocator metrics |
| 9 | Pipelining polish | Write-ahead knob bounded by writability watermarks; fair per-node drain; fault-injection proof that reordered responses kill the connection (D8) |
| 10 | Testing/benchmarks/hardening | Compatibility-matrix CI; JMH gates (≥30% producer allocation-rate reduction, no p99 regression); 24h soak, paranoid leak detection; chaos mid-handshake/mid-reauth/mid-rebalance; nightly DST seed sweeps with invariant checkers |
| 11 | Deferred | Physical `:clients-core` split; Netty default for internal clients; idempotence + transactions on the D15 seams; deprecation & upstream KIP packaging |

## 5. Risk register

| Risk | Mitigation |
|---|---|
| SASL handshake/reauth byte divergence (GSSAPI raw v0, KIP-368 interleaving, reserved correlation ids) | Golden-bytes harness covers every mechanism incl. reauth; reuse authenticator internals, replace only I/O shuttling |
| Broker mute/quota semantics mismatch in the `Selectable` adapter | Exact five-state machine documented [TV §1] and implemented against it; full core suite under flag; default stays `nio` until parity proven |
| Pooled-buffer leaks via lazy decompression / user-retained values | D7 copy-by-default; release ledger; paranoid leak detector; soak gate |
| sendfile (`FileRegion`) fetch parity on the Netty broker path | Explicit Phase-3 deliverable + throughput benchmark gate |
| Determinism leaks in sim mode (Netty internals, dependency `nanoTime`/`Random`, map iteration) | D13 lint; `EmbeddedChannel`-only I/O in sim; double-run-same-seed trace equality in CI |
| Late binding breaks ordering (keyed reorder) or future idempotence (duplicate sequences after rebind) | D14/D15: rebind only pre-seal; keyed records `Fixed` by default; DST failover scenarios; dormant duplicate-sequence checker |
| Deferred idempotence/txn design rots | D15 seams are named code artifacts (`BatchSealer`, `ProducerIdentity`, `SEALED`), mapped to classic `TransactionManager` responsibilities [TV §7] |
| Ported KIP-848 managers drift from upstream | `RequestEnvelope` abstraction keeps ports thin; diff-audit against upstream on trunk merges |
| Event-loop deadlocks from user callbacks | D10 executor completion; BlockHound in tests |
| Two transports to maintain | Differential harness makes parity cheap to verify; classic NIO untouched |

## 6. Key upstream files (contract sources)

- `clients/src/main/java/org/apache/kafka/common/network/{Selectable,Selector,KafkaChannel,Send,NetworkSend,ByteBufferSend,NetworkReceive,TransferableChannel}.java`
- `clients/src/main/java/org/apache/kafka/common/protocol/{SendBuilder,ApiKeys,ApiMessage,ObjectSerializationCache}.java`
- `clients/src/main/java/org/apache/kafka/clients/{NetworkClient,InFlightRequests,ClusterConnectionStates}.java`
- `clients/src/main/java/org/apache/kafka/common/security/authenticator/{SaslClientAuthenticator,SaslServerAuthenticator}.java`
- `clients/src/main/java/org/apache/kafka/common/security/{auth/SslEngineFactory,ssl/SslFactory}.java`
- `clients/src/main/java/org/apache/kafka/common/record/internal/{MemoryRecordsBuilder,FileRecords,RecordsSend,MultiRecordsSend,CompressionType}.java`
- `clients/src/main/java/org/apache/kafka/common/compress/*.java`
- `clients/src/main/java/org/apache/kafka/clients/producer/internals/{RecordAccumulator,BuiltInPartitioner,BufferPool,Sender}.java`
- `clients/src/main/java/org/apache/kafka/clients/consumer/internals/{AsyncKafkaConsumer,ConsumerNetworkThread,RequestManager,RequestManagers,NetworkClientDelegate,FetchBuffer,FetchCollector,CompletedFetch}.java`
- `clients/src/main/java/org/apache/kafka/clients/admin/internals/{AdminApiDriver,AdminApiHandler,AdminApiLookupStrategy}.java`
- `core/src/main/scala/kafka/network/{SocketServer,RequestChannel}.scala`, `server/src/main/java/org/apache/kafka/network/*.java`
- `server/src/main/java/org/apache/kafka/server/NodeToControllerChannelManagerImpl.java`, `core/src/main/scala/kafka/server/BrokerBlockingSender.scala`
- `build.gradle`, `settings.gradle`, `gradle/dependencies.gradle`, `checkstyle/import-control*.xml`
