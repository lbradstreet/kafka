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
import java.util.Deque;
import java.util.PriorityQueue;

/**
 * Simulates the Kafka producer pipeline with and without expanded batching
 * under backpressure over a high-latency link.
 *
 * Models: record arrival, batch accumulation, inflight slot management,
 * RTT-based response completion, and the nodeInflightFull flag.
 */
public class BackpressureBatchingSim {

    // --- Configuration ---
    static final int BATCH_SIZE = 16_384;           // 16 KB
    static final int BACKPRESSURE_MULTIPLIER = 4;
    static final int MAX_INFLIGHT = 5;
    static final int RTT_MS = 100;                  // 100 ms round-trip
    static final int RECORD_SIZE = 100;             // bytes per record
    static final int RECORD_ARRIVAL_RATE = 50_000;  // records/sec arriving from application
    static final int SIM_DURATION_MS = 5_000;       // simulate 5 seconds
    static final int SENDER_INTERVAL_MS = 1;        // sender thread polls every 1 ms

    public static void main(String[] args) {
        System.out.println("=== Kafka Producer Backpressure Batching Simulation ===");
        System.out.println();
        printConfig();
        System.out.println();

        SimResult baseline = runSimulation(false);
        SimResult expanded = runSimulation(true);

        printComparison(baseline, expanded);
    }

    static void printConfig() {
        System.out.println("Configuration:");
        System.out.printf("  batch.size           = %,d bytes (%d KB)%n", BATCH_SIZE, BATCH_SIZE / 1024);
        System.out.printf("  max.in.flight        = %d%n", MAX_INFLIGHT);
        System.out.printf("  RTT                  = %d ms%n", RTT_MS);
        System.out.printf("  record size          = %d bytes%n", RECORD_SIZE);
        System.out.printf("  record arrival rate  = %,d records/sec%n", RECORD_ARRIVAL_RATE);
        System.out.printf("  simulation duration  = %,d ms%n", SIM_DURATION_MS);
        System.out.printf("  backpressure mult    = %dx (effective batch = %d KB)%n",
                BACKPRESSURE_MULTIPLIER, BATCH_SIZE * BACKPRESSURE_MULTIPLIER / 1024);
    }

    static SimResult runSimulation(boolean expandedBatching) {
        // Inflight tracking: priority queue of completion times
        PriorityQueue<Long> inflightCompletions = new PriorityQueue<>();
        // Accumulator: queue of batches waiting to be sent
        Deque<Batch> pendingBatches = new ArrayDeque<>();
        // Current batch being filled
        Batch currentBatch = null;

        boolean nodeInflightFull = false;

        long totalBytesSent = 0;
        long totalRecordsSent = 0;
        int totalRequestsSent = 0;
        int normalBatches = 0;
        int expandedBatchCount = 0;
        long timeInflightFull = 0;
        int maxPendingBatches = 0;

        // Records arrive continuously; we model fractional accumulation
        double recordDebt = 0.0;
        double recordsPerMs = RECORD_ARRIVAL_RATE / 1000.0;

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
                    } else {
                        batchCapacity = BATCH_SIZE;
                    }
                    currentBatch = new Batch(batchCapacity);
                    if (batchCapacity > BATCH_SIZE) {
                        expandedBatchCount++;
                    } else {
                        normalBatches++;
                    }
                }
                if (!currentBatch.tryAppend(RECORD_SIZE)) {
                    // Batch full — seal it and queue
                    pendingBatches.addLast(currentBatch);
                    currentBatch = null;
                    // Don't consume the record yet; loop will create a new batch
                    continue;
                }
                recordDebt -= 1.0;
            }

            // --- 3. Sender loop (runs every SENDER_INTERVAL_MS) ---
            if (now % SENDER_INTERVAL_MS == 0) {
                int inflight = inflightCompletions.size();

                // Determine if node is ready (has available inflight slots)
                boolean clientReady = inflight < MAX_INFLIGHT;

                // Set the backpressure flag (mirrors Sender.sendProducerData logic)
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
                        // Try sending the current partial batch if it exists
                        if (currentBatch != null && currentBatch.usedBytes > 0) {
                            toSend = currentBatch;
                            currentBatch = null;
                        } else {
                            break; // Nothing to send
                        }
                    }
                    inflightCompletions.add(now + RTT_MS);
                    totalBytesSent += toSend.usedBytes;
                    totalRecordsSent += toSend.recordCount;
                    totalRequestsSent++;
                    inflight++;
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
                maxPendingBatches
        );
    }

    static void printComparison(SimResult baseline, SimResult expanded) {
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
        System.out.println("└─────────────────────────────────┴──────────────────┴──────────────────┘");

        System.out.println();
        double byteImprovement = (double) expanded.totalBytesSent / baseline.totalBytesSent;
        double requestReduction = 1.0 - (double) expanded.totalRequestsSent / baseline.totalRequestsSent;
        double pendingReduction = 1.0 - (double) expanded.maxPendingBatches / baseline.maxPendingBatches;

        System.out.println("Impact Summary:");
        System.out.printf("  Throughput improvement:    %.1fx%n", byteImprovement);
        System.out.printf("  Request count reduction:   %.1f%%%n", requestReduction * 100);
        System.out.printf("  Peak pending reduction:    %.1f%%%n", pendingReduction * 100);
        System.out.println();
        System.out.println("Analysis:");
        System.out.println("  With max.in.flight=5 and RTT=100ms, the pipeline can sustain at most");
        System.out.printf("  5 requests per 100ms. At batch.size=16KB, that caps throughput at ~%.1f MB/s.%n",
                (double) MAX_INFLIGHT * BATCH_SIZE / RTT_MS * 1000 / 1_048_576.0);
        System.out.printf("  With 4x expanded batches under backpressure, the cap rises to ~%.1f MB/s.%n",
                (double) MAX_INFLIGHT * BATCH_SIZE * BACKPRESSURE_MULTIPLIER / RTT_MS * 1000 / 1_048_576.0);
        System.out.println("  The larger batches fill each inflight slot with more data, directly");
        System.out.println("  increasing the bandwidth-delay product the producer can sustain.");
        System.out.println("  Fewer, larger requests also reduce per-request overhead (headers, acks).");
    }

    static void printRow(String label, String baseVal, String expVal) {
        System.out.printf("│ %-31s │ %16s │ %16s │%n", label, baseVal, expVal);
    }

    static class Batch {
        final int capacity;
        int usedBytes;
        int recordCount;

        Batch(int capacity) {
            this.capacity = capacity;
        }

        boolean tryAppend(int recordSize) {
            if (usedBytes + recordSize > capacity) {
                return false;
            }
            usedBytes += recordSize;
            recordCount++;
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

        SimResult(boolean expandedBatching, long totalBytesSent, long totalRecordsSent,
                  int totalRequestsSent, int normalBatches, int expandedBatches,
                  long timeInflightFull, int maxPendingBatches) {
            this.expandedBatching = expandedBatching;
            this.totalBytesSent = totalBytesSent;
            this.totalRecordsSent = totalRecordsSent;
            this.totalRequestsSent = totalRequestsSent;
            this.normalBatches = normalBatches;
            this.expandedBatches = expandedBatches;
            this.timeInflightFull = timeInflightFull;
            this.maxPendingBatches = maxPendingBatches;
        }
    }
}
