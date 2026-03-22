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
package org.apache.kafka.tools;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Simulates the Kafka producer pipeline with and without expanded batching
 * under backpressure over a high-latency link.
 *
 * Models: record arrival, batch accumulation, inflight slot management,
 * RTT-based response completion, and the nodeInflightFull flag.
 *
 * Sweeps across arrival rates to show behavior when expanded batches
 * are partially filled vs fully filled.
 */
public class BackpressureBatchingSim {

    // --- Configuration ---
    static final int BATCH_SIZE = 16_384;           // 16 KB
    static final int BACKPRESSURE_MULTIPLIER = 4;
    static final int MAX_INFLIGHT = 5;
    static final int RTT_MS = 100;                  // 100 ms round-trip
    static final int RECORD_SIZE = 100;             // bytes per record
    static final int SIM_DURATION_MS = 10_000;      // simulate 10 seconds
    static final int SENDER_INTERVAL_MS = 1;        // sender thread polls every 1 ms
    static final int LINGER_MS = 5;                 // linger time before sending partial batch

    // Records per 16KB batch: 16384/100 = ~163
    // Max throughput at 16KB batches: 5 * 163 / 0.1s = 8150 rec/s = ~0.78 MB/s
    // Max throughput at 64KB batches: 5 * 655 / 0.1s = 32750 rec/s = ~3.12 MB/s

    // Sweep from well below baseline capacity to well above expanded capacity
    static final int[] ARRIVAL_RATES = {
        2_000,    // well below pipeline capacity — no backpressure
        5_000,    // approaching baseline capacity
        8_000,    // at baseline capacity — pipeline starts saturating
        10_000,   // just above baseline cap — light backpressure
        15_000,   // moderate backpressure — expanded batches partially filled
        20_000,   // heavier backpressure — expanded batches ~60% filled
        30_000,   // near expanded capacity — batches mostly filled
        50_000,   // above expanded capacity — batches fully filled
        80_000,   // heavily overloaded
    };

    public static void main(String[] args) {
        System.out.println("=== Kafka Producer Backpressure Batching Simulation ===");
        System.out.println("=== Rate Sweep: Partial vs Full Expanded Batches    ===");
        System.out.println();
        printConfig();
        System.out.println();

        // --- Section 1: Rate sweep comparison table ---
        printRateSweep();

        // --- Section 2: Detailed comparison at a rate that doesn't fill expanded batches ---
        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("DETAILED VIEW: 10,000 records/sec (just above baseline cap, partial fill)");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println();
        SimResult base10k = runSimulation(false, 10_000);
        SimResult exp10k = runSimulation(true, 10_000);
        printDetailedComparison(base10k, exp10k);

        System.out.println();
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println("DETAILED VIEW: 50,000 records/sec (well above cap, full fill)");
        System.out.println("═══════════════════════════════════════════════════════════════════════════");
        System.out.println();
        SimResult base50k = runSimulation(false, 50_000);
        SimResult exp50k = runSimulation(true, 50_000);
        printDetailedComparison(base50k, exp50k);
    }

    static void printConfig() {
        System.out.println("Configuration:");
        System.out.printf("  batch.size           = %,d bytes (%d KB)%n", BATCH_SIZE, BATCH_SIZE / 1024);
        System.out.printf("  max.in.flight        = %d%n", MAX_INFLIGHT);
        System.out.printf("  RTT                  = %d ms%n", RTT_MS);
        System.out.printf("  record size          = %d bytes%n", RECORD_SIZE);
        System.out.printf("  linger.ms            = %d ms%n", LINGER_MS);
        System.out.printf("  simulation duration  = %,d ms%n", SIM_DURATION_MS);
        System.out.printf("  backpressure mult    = %dx (expanded batch = %d KB)%n",
                BACKPRESSURE_MULTIPLIER, BATCH_SIZE * BACKPRESSURE_MULTIPLIER / 1024);
        System.out.printf("  baseline pipe cap    = ~%,d rec/s (%.1f MB/s)%n",
                MAX_INFLIGHT * (BATCH_SIZE / RECORD_SIZE) * (1000 / RTT_MS),
                (double) MAX_INFLIGHT * BATCH_SIZE / RTT_MS * 1000 / 1_048_576.0);
        System.out.printf("  expanded pipe cap    = ~%,d rec/s (%.1f MB/s)%n",
                MAX_INFLIGHT * (BATCH_SIZE * BACKPRESSURE_MULTIPLIER / RECORD_SIZE) * (1000 / RTT_MS),
                (double) MAX_INFLIGHT * BATCH_SIZE * BACKPRESSURE_MULTIPLIER / RTT_MS * 1000 / 1_048_576.0);
    }

    static void printRateSweep() {
        System.out.println("╔══════════╦═══════════╦═══════════╦═════════╦══════════╦══════════╦══════════╦══════════╦═════════════╗");
        System.out.println("║ Arrival  ║ Baseline  ║ Expanded  ║ Thru-   ║ Expanded ║ Avg Fill ║ Wasted   ║ Inflight ║ Avg Record  ║");
        System.out.println("║ Rate     ║ Thruput   ║ Thruput   ║ put     ║ Batches  ║ Ratio    ║ Memory   ║ Full     ║ Latency     ║");
        System.out.println("║ (rec/s)  ║ (MB/s)    ║ (MB/s)    ║ Gain    ║ (% total)║ (exp'd)  ║ (KB/req) ║ (% time) ║ Base → Exp  ║");
        System.out.println("╠══════════╬═══════════╬═══════════╬═════════╬══════════╬══════════╬══════════╬══════════╬═════════════╣");

        for (int rate : ARRIVAL_RATES) {
            SimResult baseline = runSimulation(false, rate);
            SimResult expanded = runSimulation(true, rate);

            double baseThroughput = baseline.totalBytesSent / (SIM_DURATION_MS / 1000.0) / 1_048_576.0;
            double expThroughput = expanded.totalBytesSent / (SIM_DURATION_MS / 1000.0) / 1_048_576.0;
            double gain = baseThroughput > 0 ? expThroughput / baseThroughput : 0;

            int totalExpandedBatches = expanded.expandedBatches;
            int totalBatches = expanded.normalBatches + expanded.expandedBatches;
            double expandedPct = totalBatches > 0 ? 100.0 * totalExpandedBatches / totalBatches : 0;

            double avgFillRatio = expanded.expandedBatchBytesUsed > 0
                    ? (double) expanded.expandedBatchBytesUsed / expanded.expandedBatchBytesAllocated
                    : 0;

            double wastedKBPerReq = expanded.totalRequestsSent > 0
                    ? (double) (expanded.expandedBatchBytesAllocated - expanded.expandedBatchBytesUsed) / expanded.totalRequestsSent / 1024.0
                    : 0;

            double inflightFullPct = 100.0 * baseline.timeInflightFull / SIM_DURATION_MS;

            double baseAvgLatency = baseline.totalRecordsSent > 0
                    ? (double) baseline.totalRecordLatencyMs / baseline.totalRecordsSent
                    : 0;
            double expAvgLatency = expanded.totalRecordsSent > 0
                    ? (double) expanded.totalRecordLatencyMs / expanded.totalRecordsSent
                    : 0;

            System.out.printf("║ %,7d  ║ %7.2f   ║ %7.2f   ║ %5.2fx  ║ %6.1f%%  ║ %6.1f%%  ║ %6.1f   ║ %6.1f%%  ║ %4.0f → %4.0fms ║%n",
                    rate, baseThroughput, expThroughput, gain,
                    expandedPct, avgFillRatio * 100, wastedKBPerReq,
                    inflightFullPct, baseAvgLatency, expAvgLatency);
        }

        System.out.println("╚══════════╩═══════════╩═══════════╩═════════╩══════════╩══════════╩══════════╩══════════╩═════════════╝");

        System.out.println();
        System.out.println("Key observations:");
        System.out.println("  - 'Avg Fill Ratio' = bytes used / bytes allocated for expanded batches.");
        System.out.println("    When < 100%, expanded batches are sent partially filled (linger.ms or slot opened).");
        System.out.println("  - 'Wasted Memory' = unused allocated bytes in expanded batches, amortized per request.");
        System.out.println("    These use the BufferPool's non-poolable path (not returned to the free list).");
        System.out.println("  - 'Avg Record Latency' = average time from record arrival to batch send.");
        System.out.println("    Higher at low rates because partial batches wait for linger.ms to expire.");
        System.out.println("  - At rates below baseline capacity, inflight never fills → no expanded batches created.");
        System.out.println("  - At rates just above baseline capacity, expanded batches are partially filled");
        System.out.println("    but still carry more data per request than baseline → net throughput gain.");
    }

    static SimResult runSimulation(boolean expandedBatching, int arrivalRate) {
        PriorityQueue<Long> inflightCompletions = new PriorityQueue<>();
        Deque<Batch> pendingBatches = new ArrayDeque<>();
        Batch currentBatch = null;

        boolean nodeInflightFull = false;

        long totalBytesSent = 0;
        long totalRecordsSent = 0;
        int totalRequestsSent = 0;
        int normalBatches = 0;
        int expandedBatchCount = 0;
        long timeInflightFull = 0;
        int maxPendingBatches = 0;

        // Latency tracking
        long totalRecordLatencyMs = 0;

        // Expanded batch fill tracking
        long expandedBatchBytesAllocated = 0;
        long expandedBatchBytesUsed = 0;

        double recordDebt = 0.0;
        double recordsPerMs = arrivalRate / 1000.0;

        for (long now = 0; now < SIM_DURATION_MS; now++) {
            // --- 1. Complete any inflight requests whose RTT has elapsed ---
            while (!inflightCompletions.isEmpty() && inflightCompletions.peek() <= now) {
                inflightCompletions.poll();
            }

            // --- 2. Accumulate arriving records into batches ---
            recordDebt += recordsPerMs;
            while (recordDebt >= 1.0) {
                if (currentBatch == null) {
                    int batchCapacity;
                    if (expandedBatching && nodeInflightFull) {
                        batchCapacity = BATCH_SIZE * BACKPRESSURE_MULTIPLIER;
                        expandedBatchCount++;
                        expandedBatchBytesAllocated += batchCapacity;
                    } else {
                        batchCapacity = BATCH_SIZE;
                        normalBatches++;
                    }
                    currentBatch = new Batch(batchCapacity, now);
                }
                if (!currentBatch.tryAppend(RECORD_SIZE, now)) {
                    pendingBatches.addLast(currentBatch);
                    currentBatch = null;
                    continue;
                }
                recordDebt -= 1.0;
            }

            // --- 3. Sender loop ---
            if (now % SENDER_INTERVAL_MS == 0) {
                int inflight = inflightCompletions.size();

                boolean clientReady = inflight < MAX_INFLIGHT;

                if (!clientReady) {
                    nodeInflightFull = true;
                    timeInflightFull++;
                } else {
                    nodeInflightFull = false;
                }

                // Drain and send batches while we have capacity
                while (clientReady && inflight < MAX_INFLIGHT) {
                    Batch toSend = pendingBatches.pollFirst();
                    if (toSend == null) {
                        // Try sending current partial batch if linger time has elapsed
                        if (currentBatch != null && currentBatch.usedBytes > 0
                                && (now - currentBatch.createdAtMs) >= LINGER_MS) {
                            toSend = currentBatch;
                            currentBatch = null;
                        } else {
                            break;
                        }
                    }

                    inflightCompletions.add(now + RTT_MS);
                    totalBytesSent += toSend.usedBytes;
                    totalRecordsSent += toSend.recordCount;
                    totalRequestsSent++;
                    inflight++;

                    // Track latency: each record's latency = (send time) - (record arrival time)
                    for (long arrivalMs : toSend.recordArrivalTimes) {
                        totalRecordLatencyMs += (now - arrivalMs);
                    }

                    // Track fill ratio for expanded batches
                    if (toSend.capacity > BATCH_SIZE) {
                        expandedBatchBytesUsed += toSend.usedBytes;
                    }
                }

                maxPendingBatches = Math.max(maxPendingBatches, pendingBatches.size());
            }
        }

        return new SimResult(
                expandedBatching,
                totalBytesSent,
                totalRecordsSent,
                totalRequestsSent,
                normalBatches,
                expandedBatchCount,
                timeInflightFull,
                maxPendingBatches,
                totalRecordLatencyMs,
                expandedBatchBytesAllocated,
                expandedBatchBytesUsed
        );
    }

    static void printDetailedComparison(SimResult baseline, SimResult expanded) {
        System.out.println("┌─────────────────────────────────┬──────────────────┬──────────────────┐");
        System.out.println("│ Metric                          │     Baseline     │     Expanded     │");
        System.out.println("├─────────────────────────────────┼──────────────────┼──────────────────┤");
        printRow("Total bytes sent",
                String.format("%,d", baseline.totalBytesSent),
                String.format("%,d", expanded.totalBytesSent));
        printRow("Total records sent",
                String.format("%,d", baseline.totalRecordsSent),
                String.format("%,d", expanded.totalRecordsSent));
        printRow("Total requests sent",
                String.format("%,d", baseline.totalRequestsSent),
                String.format("%,d", expanded.totalRequestsSent));
        printRow("Throughput (MB/s)",
                String.format("%.2f", baseline.totalBytesSent / (SIM_DURATION_MS / 1000.0) / 1_048_576.0),
                String.format("%.2f", expanded.totalBytesSent / (SIM_DURATION_MS / 1000.0) / 1_048_576.0));
        printRow("Throughput (records/s)",
                String.format("%,d", baseline.totalRecordsSent / (SIM_DURATION_MS / 1000)),
                String.format("%,d", expanded.totalRecordsSent / (SIM_DURATION_MS / 1000)));
        printRow("Avg bytes/request",
                String.format("%,d", baseline.totalRequestsSent > 0 ? baseline.totalBytesSent / baseline.totalRequestsSent : 0),
                String.format("%,d", expanded.totalRequestsSent > 0 ? expanded.totalBytesSent / expanded.totalRequestsSent : 0));
        printRow("Avg records/request",
                String.format("%,d", baseline.totalRequestsSent > 0 ? baseline.totalRecordsSent / baseline.totalRequestsSent : 0),
                String.format("%,d", expanded.totalRequestsSent > 0 ? expanded.totalRecordsSent / expanded.totalRequestsSent : 0));
        printRow("Normal batches created",
                String.format("%,d", baseline.normalBatches),
                String.format("%,d", expanded.normalBatches));
        printRow("Expanded batches created",
                String.format("%,d", baseline.expandedBatches),
                String.format("%,d", expanded.expandedBatches));
        printRow("Time inflight full (ms)",
                String.format("%,d", baseline.timeInflightFull),
                String.format("%,d", expanded.timeInflightFull));
        printRow("Max pending batches",
                String.format("%,d", baseline.maxPendingBatches),
                String.format("%,d", expanded.maxPendingBatches));

        double baseAvgLatency = baseline.totalRecordsSent > 0
                ? (double) baseline.totalRecordLatencyMs / baseline.totalRecordsSent : 0;
        double expAvgLatency = expanded.totalRecordsSent > 0
                ? (double) expanded.totalRecordLatencyMs / expanded.totalRecordsSent : 0;
        printRow("Avg record latency (ms)",
                String.format("%.1f", baseAvgLatency),
                String.format("%.1f", expAvgLatency));

        double avgFillRatio = expanded.expandedBatchBytesAllocated > 0
                ? 100.0 * expanded.expandedBatchBytesUsed / expanded.expandedBatchBytesAllocated : 0;
        printRow("Expanded batch fill ratio",
                "n/a",
                String.format("%.1f%%", avgFillRatio));

        long wastedBytes = expanded.expandedBatchBytesAllocated - expanded.expandedBatchBytesUsed;
        printRow("Wasted expanded alloc",
                "n/a",
                String.format("%,d KB", wastedBytes / 1024));

        System.out.println("└─────────────────────────────────┴──────────────────┴──────────────────┘");
    }

    static void printRow(String label, String baseVal, String expVal) {
        System.out.printf("│ %-31s │ %16s │ %16s │%n", label, baseVal, expVal);
    }

    static class Batch {
        final int capacity;
        final long createdAtMs;
        int usedBytes;
        int recordCount;
        final List<Long> recordArrivalTimes = new ArrayList<>();

        Batch(int capacity, long createdAtMs) {
            this.capacity = capacity;
            this.createdAtMs = createdAtMs;
        }

        boolean tryAppend(int recordSize, long arrivalMs) {
            if (usedBytes + recordSize > capacity) {
                return false;
            }
            usedBytes += recordSize;
            recordCount++;
            recordArrivalTimes.add(arrivalMs);
            return true;
        }
    }

    static class SimResult {
        final boolean expandedBatching;
        final long totalBytesSent;
        final long totalRecordsSent;
        final int totalRequestsSent;
        final int normalBatches;
        final int expandedBatches;
        final long timeInflightFull;
        final int maxPendingBatches;
        final long totalRecordLatencyMs;
        final long expandedBatchBytesAllocated;
        final long expandedBatchBytesUsed;

        SimResult(boolean expandedBatching, long totalBytesSent, long totalRecordsSent,
                  int totalRequestsSent, int normalBatches, int expandedBatches,
                  long timeInflightFull, int maxPendingBatches,
                  long totalRecordLatencyMs,
                  long expandedBatchBytesAllocated, long expandedBatchBytesUsed) {
            this.expandedBatching = expandedBatching;
            this.totalBytesSent = totalBytesSent;
            this.totalRecordsSent = totalRecordsSent;
            this.totalRequestsSent = totalRequestsSent;
            this.normalBatches = normalBatches;
            this.expandedBatches = expandedBatches;
            this.timeInflightFull = timeInflightFull;
            this.maxPendingBatches = maxPendingBatches;
            this.totalRecordLatencyMs = totalRecordLatencyMs;
            this.expandedBatchBytesAllocated = expandedBatchBytesAllocated;
            this.expandedBatchBytesUsed = expandedBatchBytesUsed;
        }
    }
}
