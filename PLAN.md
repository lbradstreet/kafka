# Plan: Fix Kafka Producer Memory Usage with Small Records (KAFKA-13832 / KAFKA-15582)

## Problem

When producing small records to many partitions, the Kafka producer's actual memory
consumption far exceeds what's useful because:

1. **`RecordAccumulator.append()` (line 208)** always allocates `Math.max(batchSize, recordEstimate)` per new partition batch
2. With 1000 partitions and default `batch.size=16384`, this means 16MB minimum allocation even if each record is 100 bytes
3. With larger `batch.size` (e.g. 1MB), this becomes 1GB for 1000 partitions
4. The `buffer.memory` limit (default 32MB) is quickly exhausted, causing threads to block

## Approach: Adaptive Batch Sizing Under Memory Pressure

Introduce memory-pressure-aware batch allocation in `RecordAccumulator`. When available
memory is running low relative to the number of active partitions, allocate smaller initial
batch buffers instead of always using the full `batchSize`.

### Key Design Decisions

- **Scaling strategy**: When memory pressure is detected, scale down batch allocation size
  proportionally. Use `min(batchSize, availableMemory / numActivePartitions)` with a floor
  at the estimated record size.
- **Threshold**: Memory pressure kicks in when available memory drops below
  `batchSize * partitionCount` — i.e., there isn't enough memory to give every active
  partition a full-sized batch.
- **Pooling trade-off**: Smaller-than-`batchSize` allocations won't be pooled by
  `BufferPool` (they go through `nonPooledAvailableMemory` on deallocation). This is
  acceptable because: (a) the alternative is blocking/OOM, (b) under memory pressure,
  reducing allocation size is strictly better, and (c) if memory pressure eases, subsequent
  batches will return to full `batchSize` and benefit from pooling again.
- **No new configuration**: This is adaptive behavior based on existing configs. Users
  don't need to tune anything new.

## Changes

### 1. `RecordAccumulator.java` — Adaptive batch size calculation

**In `append()` method (around line 208):**

Replace:
```java
int size = Math.max(this.batchSize,
    AbstractRecords.estimateSizeInBytesUpperBound(maxUsableMagic, compression, key, value, headers));
```

With logic that:
1. Computes `estimatedRecordSize` as the upper bound for this single record
2. Checks available memory via `free.availableMemory()`
3. Counts active partitions via `batches.size()` (number of partitions with deques)
4. If `availableMemory < batchSize * activePartitions`, calculates a reduced batch size:
   `effectiveSize = max(estimatedRecordSize, availableMemory / activePartitions)`
   capped at `batchSize`
5. Otherwise uses full `batchSize` (no behavior change when memory is plentiful)

Add a private helper method:
```java
private int effectiveBatchSize(int estimatedRecordSize) {
    long availableMemory = free.availableMemory();
    int activePartitions = Math.max(1, batches.size());
    if (availableMemory < (long) batchSize * activePartitions) {
        int reduced = (int) Math.max(estimatedRecordSize, availableMemory / activePartitions);
        return Math.min(batchSize, reduced);
    }
    return batchSize;
}
```

### 2. `BufferPool.java` — No changes required

`availableMemory()` already exists and provides the needed information. The method acquires
a lock, but it's lightweight and called once per new batch creation (not per record append).

### 3. `RecordAccumulatorTest.java` — New test cases

Add tests that verify:
- **Normal behavior preserved**: When memory is plentiful, full `batchSize` is still allocated
- **Adaptive sizing under pressure**: When available memory is low relative to partition
  count, smaller buffers are allocated
- **Records still succeed**: Small records to many partitions don't cause
  `TimeoutException` when they previously would have with rigid `batchSize` allocation
- **Recovery**: When memory pressure eases (batches sent), allocation returns to full
  `batchSize`

### 4. `BufferPoolTest.java` — No changes expected

BufferPool behavior is unchanged; it already handles non-poolable-size allocations correctly.

## Risk Assessment

- **Low risk**: When memory is plentiful, behavior is identical to today (the `if` branch
  returns `batchSize` directly)
- **Moderate complexity**: The adaptive path introduces a new calculation, but it's
  straightforward and bounded
- **Trade-off**: Under memory pressure, smaller batches mean potentially more requests to
  brokers (less batching). But this is strictly better than the status quo where the
  producer either blocks or OOMs
- **Thread safety**: `batches.size()` is a read on a `CopyOnWriteMap` (safe);
  `free.availableMemory()` acquires its own lock (safe)
