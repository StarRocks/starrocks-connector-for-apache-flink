/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.data.load.stream;

import org.junit.Test;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;

public class SDKIngestionBenchmark {

    @Test
    public void runBenchmark() {
        main(new String[0]);
    }

    public static void main(String[] args) {
        System.out.println("=== StarRocks SDK Performance Benchmark ===");
        
        // 1. JSON Chunk Appends Benchmark
        benchmarkJsonChunkAppends();

        // 2. CSV Chunk Appends Benchmark
        benchmarkCsvChunkAppends();

        // 3. Arrow Chunk Appends Benchmark
        benchmarkArrowChunkAppends();
        
        // 4. String-to-Byte Conversion / Write overhead simulation
        benchmarkWriteOverhead();
    }

    private static void benchmarkJsonChunkAppends() {
        int warmups = 5;
        int iterations = 10;
        int rowsPerChunk = 200_000;
        byte[] row = "{\"timestamp_ns\":1718012345000000000,\"timestamp_dt\":\"2024-06-10 10:10:10\",\"trace_id\":\"4bf92f3577b34da6a3ce929d0e0e4736\",\"span_id\":\"00f067aa0ba902b7\",\"span_name\":\"HTTP GET /api/v1/resource\",\"span_kind\":\"SPAN_KIND_SERVER\",\"service_name\":\"my-service\",\"service_version\":\"1.0.0\",\"genai_operation_name\":\"chat\",\"genai_provider_name\":\"openai\",\"genai_request_model\":\"gpt-4\",\"genai_response_model\":\"gpt-4\",\"duration_ms\":120.5,\"input_tokens\":150,\"output_tokens\":200,\"cache_read_input_tokens\":0}".getBytes(StandardCharsets.UTF_8);

        System.out.println("\n--- 1. JSON Chunk Appends Benchmark ---");
        System.out.println("Appending " + rowsPerChunk + " rows per chunk...");

        // Warmup
        for (int i = 0; i < warmups; i++) {
            runJsonChunkIteration(rowsPerChunk, row);
        }

        // Measure
        com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long startAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            runJsonChunkIteration(rowsPerChunk, row);
        }

        long endTime = System.nanoTime();
        long endAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());

        double durationSec = (endTime - startTime) / 1_000_000_000.0;
        long totalAllocatedBytes = endAllocated - startAllocated;
        double allocatedMbPerChunk = (double) totalAllocatedBytes / iterations / (1024 * 1024);

        System.out.printf("Total duration: %.3f seconds%n", durationSec);
        System.out.printf("Throughput: %.2f ops/sec (chunks/sec)%n", (double) iterations / durationSec);
        System.out.printf("Heap allocated per chunk: %.2f MB%n", allocatedMbPerChunk);
    }

    private static void runJsonChunkIteration(int rows, byte[] row) {
        Chunk chunk = new Chunk(StreamLoadDataFormat.JSON, 1);
        for (int i = 0; i < rows; i++) {
            chunk.addRow(row);
        }
        chunk.release();
    }

    private static void benchmarkCsvChunkAppends() {
        int warmups = 5;
        int iterations = 10;
        int rowsPerChunk = 200_000;
        byte[] row = "1718012345000000000,2024-06-10 10:10:10,4bf92f3577b34da6a3ce929d0e0e4736,00f067aa0ba902b7,HTTP GET /api/v1/resource,SPAN_KIND_SERVER,my-service,1.0.0,chat,openai,gpt-4,gpt-4,120.5,150,200,0".getBytes(StandardCharsets.UTF_8);

        System.out.println("\n--- 2. CSV Chunk Appends Benchmark ---");
        System.out.println("Appending " + rowsPerChunk + " rows per chunk...");

        // Warmup
        for (int i = 0; i < warmups; i++) {
            runCsvChunkIteration(rowsPerChunk, row);
        }

        // Measure
        com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long startAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            runCsvChunkIteration(rowsPerChunk, row);
        }

        long endTime = System.nanoTime();
        long endAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());

        double durationSec = (endTime - startTime) / 1_000_000_000.0;
        long totalAllocatedBytes = endAllocated - startAllocated;
        double allocatedMbPerChunk = (double) totalAllocatedBytes / iterations / (1024 * 1024);

        System.out.printf("Total duration: %.3f seconds%n", durationSec);
        System.out.printf("Throughput: %.2f ops/sec (chunks/sec)%n", (double) iterations / durationSec);
        System.out.printf("Heap allocated per chunk: %.2f MB%n", allocatedMbPerChunk);
    }

    private static void runCsvChunkIteration(int rows, byte[] row) {
        Chunk chunk = new Chunk(StreamLoadDataFormat.CSV, 1);
        for (int i = 0; i < rows; i++) {
            chunk.addRow(row);
        }
        chunk.release();
    }

    private static void benchmarkArrowChunkAppends() {
        int warmups = 5;
        int iterations = 10;
        int rowsPerChunk = 200_000;
        // Simulate a pre-serialized Arrow RecordBatch payload
        byte[] row = new byte[256]; 

        System.out.println("\n--- 3. Arrow Chunk Appends Benchmark (Zero-copy binary append test) ---");
        System.out.println("Appending " + rowsPerChunk + " payloads per chunk...");

        // Warmup
        for (int i = 0; i < warmups; i++) {
            runArrowChunkIteration(rowsPerChunk, row);
        }

        // Measure
        com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long startAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        long startTime = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            runArrowChunkIteration(rowsPerChunk, row);
        }

        long endTime = System.nanoTime();
        long endAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());

        double durationSec = (endTime - startTime) / 1_000_000_000.0;
        long totalAllocatedBytes = endAllocated - startAllocated;
        double allocatedMbPerChunk = (double) totalAllocatedBytes / iterations / (1024 * 1024);

        System.out.printf("Total duration: %.3f seconds%n", durationSec);
        System.out.printf("Throughput: %.2f ops/sec (chunks/sec)%n", (double) iterations / durationSec);
        System.out.printf("Heap allocated per chunk: %.2f MB%n", allocatedMbPerChunk);
    }

    private static void runArrowChunkIteration(int rows, byte[] row) {
        Chunk chunk = new Chunk(StreamLoadDataFormat.ARROW, 1);
        for (int i = 0; i < rows; i++) {
            chunk.addRow(row);
        }
        chunk.release();
    }

    private static void benchmarkWriteOverhead() {
        int warmups = 5;
        int iterations = 1_000_000;
        String rowStr = "{\"timestamp_ns\":1718012345000000000,\"timestamp_dt\":\"2024-06-10 10:10:10\",\"trace_id\":\"4bf92f3577b34da6a3ce929d0e0e4736\",\"span_id\":\"00f067aa0ba902b7\",\"span_name\":\"HTTP GET /api/v1/resource\",\"span_kind\":\"SPAN_KIND_SERVER\",\"service_name\":\"my-service\",\"service_version\":\"1.0.0\",\"genai_operation_name\":\"chat\",\"genai_provider_name\":\"openai\",\"genai_request_model\":\"gpt-4\",\"genai_response_model\":\"gpt-4\",\"duration_ms\":120.5,\"input_tokens\":150,\"output_tokens\":200,\"cache_read_input_tokens\":0}";
        byte[] rowBytes = rowStr.getBytes(StandardCharsets.UTF_8);

        System.out.println("\n--- 4. Write Overhead Benchmark (String conversion vs Direct Bytes) ---");
        System.out.println("Processing " + iterations + " writes...");

        // Warmup
        for (int i = 0; i < warmups; i++) {
            runWriteStringIteration(iterations, rowStr);
            runWriteByteIteration(iterations, rowBytes);
        }

        // Measure String approach
        com.sun.management.ThreadMXBean threadMXBean = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        long startAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        long startTime = System.nanoTime();

        runWriteStringIteration(iterations, rowStr);

        long endTime = System.nanoTime();
        long endAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());

        double stringDuration = (endTime - startTime) / 1_000_000_000.0;
        long stringAllocatedBytes = endAllocated - startAllocated;

        // Measure Byte approach
        startAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());
        startTime = System.nanoTime();

        runWriteByteIteration(iterations, rowBytes);

        endTime = System.nanoTime();
        endAllocated = threadMXBean.getThreadAllocatedBytes(Thread.currentThread().getId());

        double byteDuration = (endTime - startTime) / 1_000_000_000.0;
        long byteAllocatedBytes = endAllocated - startAllocated;

        System.out.printf("String-to-Bytes duration: %.3f seconds, heap allocated: %.2f MB%n", 
                stringDuration, (double) stringAllocatedBytes / (1024 * 1024));
        System.out.printf("Direct-Bytes duration:    %.3f seconds, heap allocated: %.2f MB%n", 
                byteDuration, (double) byteAllocatedBytes / (1024 * 1024));
        System.out.printf("Throughput difference: String path: %.2f ops/sec, Byte path: %.2f ops/sec%n", 
                (double) iterations / stringDuration, (double) iterations / byteDuration);
        System.out.printf("Allocation savings: %.2f%%%n", 
                (1.0 - (double) byteAllocatedBytes / stringAllocatedBytes) * 100.0);
    }

    private static void runWriteStringIteration(int count, String row) {
        long dummy = 0;
        for (int i = 0; i < count; i++) {
            byte[] bytes = row.getBytes(StandardCharsets.UTF_8);
            dummy += bytes.length;
        }
    }

    private static void runWriteByteIteration(int count, byte[] row) {
        long dummy = 0;
        for (int i = 0; i < count; i++) {
            // direct write, no-op or just simulate reference use
            dummy += row.length;
        }
    }
}
