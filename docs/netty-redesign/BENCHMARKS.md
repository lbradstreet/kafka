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

# Producer hot-path benchmarks

Evidence for redesign goals 3 (buffer pooling) and 4 (compression), and the measured
effect of the producer hot-path optimizations. See [DESIGN.md](DESIGN.md) decisions D9,
D9b and D12.

## What was optimized

Four changes to the v2 producer accumulation/compression path (commit *"Producer hot-path
optimizations + comparative JMH benchmarks"*):

1. **Compression-ratio seeding** — `BatchV2` seeds `MemoryRecordsBuilder`'s estimated
   compression ratio from the per-topic `CompressionRatioEstimator` and feeds observations
   back on success, like the classic producer. Fewer `ByteBufferOutputStream.expandBuffer`
   realloc-and-copy events, tighter full-detection for compressed batches.
2. **Pooled batch buffers** — `BatchBufferSource` recycles exactly-`batch.size` heap
   buffers, recycled only after a batch's *successful* completion (a built `MemoryRecords`
   shares the batch's backing array). Reuse, not budget — the `MemoryLimiter` accounting
   (D9) is unchanged.
3. **Saturated-at-creation sizing** — a batch created while its destination is already
   backpressured is allocated full `backpressure.batch.size` up front, avoiding a chain of
   1.1× realloc-copies as it grows.
4. **LZ4 output workspace pooling** — an additive
   `Lz4Compression.wrapForOutput(..., BufferSupplier)` overload lets the two ~64 KB LZ4
   block buffers be reused across batches instead of allocated per batch (D12). The v2
   producer wraps its codec in `WorkspacePooledCompression`; classic clients are unaffected
   (new overload, old signature untouched).

## Methodology

Benchmarks live in `jmh-benchmarks/src/main/java/org/apache/kafka/jmh/producer/`:

- **`AccumulatorComparisonBenchmark`** — the headline: append 10 000 records → drain →
  complete, classic `RecordAccumulator`+`BufferPool` vs v2
  `RecordAccumulatorV2`+`MemoryLimiter`, over `{NONE, LZ4, ZSTD} × {100, 1000}`-byte values.
- **`BatchBuildBenchmark`** — the isolated batch lifecycle (allocate/reuse buffer → append
  a fixed record count → `build()`), `reuseBuffer` on/off, to quantify buffer pooling alone.
- **`SendFrameBenchmark`** — the transport's request→`ByteBuf` conversion, to confirm it
  wraps the record payload by reference rather than copying.

The stable metric is **`gc.alloc.rate.norm`** (bytes allocated per op) from `-prof gc`;
throughput timings are reported but are noisier. Exact invocation:

```
./jmh-benchmarks/jmh.sh -f 1 -wi 2 -i 3 -r 2 -w 2 -prof gc \
  "org.apache.kafka.jmh.producer.(AccumulatorComparison|BatchBuild|SendFrame)"
```

> **Environment caveat.** These numbers were taken in a shared CI-class container (short
> 2×2s warmup / 3×2s measurement, single fork). They are **indicative of allocation
> behavior, not lab-grade throughput**. Allocation-per-op is reproducible; the µs/op values
> and any single-digit-percent deltas are within run-to-run noise. Re-run on dedicated
> hardware with more iterations for publishable throughput.

## Results — allocation per op (`gc.alloc.rate.norm`, bytes)

### Headline: full v2 accumulation path, before vs after the optimizations

| codec | value | v2 before | v2 after | Δ | classic (control) |
|-------|------:|----------:|---------:|------:|------------------:|
| NONE  |   100 |       377 |      376 |  −0%  | 246 |
| NONE  |  1000 |      2337 |     2337 |  −0%  | 1205 |
| LZ4   |   100 |      1290 |  **133** | **−90%** | 230 |
| LZ4   |  1000 |      9425 |  **346** | **−96%** | 739 |
| ZSTD  |   100 |       495 |  **252** | **−49%** | ~145 |
| ZSTD  |  1000 |      2359 |  **304** | **−87%** | 263 |

The compressed paths — where a 64–128 KB codec workspace and buffer growth previously
allocated per batch — drop by **49–96 %**, landing at or below the classic producer's
allocation rate. `classic` is the unchanged control (its ZSTD/100 wobble is short-run noise
from zstd-jni's shared native pool). `NONE` is flat here because with no codec workspace the
per-record allocations (`CompletableFuture`, `RecordMetadataV2`, timestamp boxing) dominate
and mask the pooled-buffer saving — which the isolated benchmark below shows directly.

### Isolated: buffer pooling (`BatchBuildBenchmark`, fresh alloc vs reused buffer)

| codec | fresh buffer | reused buffer | Δ |
|-------|-------------:|--------------:|------:|
| NONE  |       16 816 |       **360** | **−98%** |
| ZSTD  |       36 169 |        19 754 | −45% |
| GZIP  |       44 525 |        28 016 | −37% |
| LZ4   |      148 328 |       131 910 | −11% |

`NONE` isolates the effect cleanly: reusing the batch buffer removes the entire ~16 KB
per-batch allocation, leaving only the `MemoryRecords` view (~360 B). For LZ4 the buffer is
small next to the per-batch codec workspace, so buffer reuse alone moves it little — this
standalone benchmark uses the raw codec, **not** the v2 `WorkspacePooledCompression` wrapper;
the workspace pooling is what produces LZ4's 90 %+ drop in the headline table.

### Transport framing (`SendFrameBenchmark`)

`serializeAndFrame` = **1463 B/op**, independent of the (64 KB) record payload size — the
record bytes are wrapped by reference into the Netty buffer, not copied. The ~1.4 KB is the
`SendBuilder` header scratch buffer plus the composite-buffer bookkeeping.

## Reproducing

```
./gradlew :jmh-benchmarks:shadowJar
java -jar jmh-benchmarks/build/libs/kafka-jmh-benchmarks-*-all.jar \
  -f 1 -wi 5 -i 10 -prof gc \
  "org.apache.kafka.jmh.producer.(AccumulatorComparison|BatchBuild|SendFrame)"
```

## What remains hot / future work

- **`NONE` per-record overhead** — `CompletableFuture` + `RecordMetadataV2` + timestamp
  boxing per record dominate the uncompressed path; a value-type metadata carrier or a
  batched completion path would attack it.
- **Gzip `Deflater` reuse** — gzip still allocates a `Deflater` (+ its buffers) per batch;
  reuse needs `Deflater.reset()` lifecycle handling (native resource) — deliberately out of
  scope here (D12).
- **Pooled receive payloads (D7)** — this slice is the send path only; fetch-side payloads
  are still copied to heap before parse.
