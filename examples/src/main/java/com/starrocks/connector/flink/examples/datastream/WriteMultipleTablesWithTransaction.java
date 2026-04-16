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

package com.starrocks.connector.flink.examples.datastream;

import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Demonstrates multi-table atomic transaction write to StarRocks using the standard
 * connector API ({@code SinkFunctionFactory.createSinkFunction}).
 *
 * <p>With {@code sink.transaction.multi-table.enabled=true}, the connector's internal
 * {@code StreamLoadManagerV2} uses a single StarRocks transaction label across all
 * tables. Multiple Kafka transactions accumulate in the buffer, and on timer-based
 * flush, all tables are atomically committed together via a single prepare+commit.
 *
 * <h2>Architecture</h2>
 * <pre>
 * Kafka Source
 *   → keyBy(partition)
 *   → TransactionAssembler (buffers until TXN_END, emits per-row)
 *   → keyBy(sourcePartition) → StarRocksDynamicSinkFunctionV2
 *       internally: StreamLoadManagerV2 with multi-table transaction mode
 *       → per-partition, per-table regions (PartitionCommitTracker)
 *       → commit only when ALL active partitions reach txnEnd + interval elapsed
 *       → each region sends data via /api/transaction/load
 *       → per-region prepare + commit
 * </pre>
 *
 * <h2>Key guarantees</h2>
 * <ul>
 *   <li>Multiple Kafka transactions can map to one StarRocks transaction (N:1)</li>
 *   <li>TransactionAssembler ensures only complete Kafka transactions are emitted</li>
 *   <li>Per-partition regions prevent one partition's switchChunk from affecting another</li>
 *   <li>Commit waits for all active partitions to reach txnEnd boundary</li>
 *   <li>Timer-based flush via {@code sink.buffer-flush.interval-ms}</li>
 * </ul>
 *
 * <h2>Prerequisites</h2>
 * <p>StarRocks >= 4.0, connector >= 1.2.9
 * <pre>
 * CREATE DATABASE `test`;
 *
 * CREATE TABLE `test`.`orders` (
 *     `order_id` BIGINT NOT NULL,
 *     `customer_id` BIGINT NOT NULL,
 *     `total_amount` DECIMAL(10,2) DEFAULT "0",
 *     `order_status` VARCHAR(32) DEFAULT ""
 * ) ENGINE=OLAP PRIMARY KEY(`order_id`)
 * DISTRIBUTED BY HASH(`order_id`)
 * PROPERTIES("replication_num" = "1");
 *
 * CREATE TABLE `test`.`order_items` (
 *     `item_id` BIGINT NOT NULL,
 *     `order_id` BIGINT NOT NULL,
 *     `product_name` VARCHAR(128) DEFAULT "",
 *     `quantity` INT DEFAULT "0",
 *     `price` DECIMAL(10,2) DEFAULT "0"
 * ) ENGINE=OLAP PRIMARY KEY(`item_id`)
 * DISTRIBUTED BY HASH(`item_id`)
 * PROPERTIES("replication_num" = "1");
 * </pre>
 */
public class WriteMultipleTablesWithTransaction {

    // ==================== Event Model ====================

    enum EventType { TXN_BEGIN, DATA, TXN_END }

    static class TxnEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        int partition;
        String txnId;
        EventType type;
        String database;
        String table;
        String json;

        TxnEvent() {}

        TxnEvent(int partition, String txnId, EventType type,
                 String database, String table, String json) {
            this.partition = partition;
            this.txnId = txnId;
            this.type = type;
            this.database = database;
            this.table = table;
            this.json = json;
        }

        static TxnEvent begin(int partition, String txnId) {
            return new TxnEvent(partition, txnId, EventType.TXN_BEGIN, null, null, null);
        }

        static TxnEvent data(int partition, String txnId, String db, String table, String json) {
            return new TxnEvent(partition, txnId, EventType.DATA, db, table, json);
        }

        static TxnEvent end(int partition, String txnId) {
            return new TxnEvent(partition, txnId, EventType.TXN_END, null, null, null);
        }
    }

    // ==================== Mock Source ====================

    static class MockTxnEventSource extends RichParallelSourceFunction<TxnEvent> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<TxnEvent> ctx) throws Exception {
            int partition = getRuntimeContext().getIndexOfThisSubtask();

            for (int txn = 0; txn < 3 && running; txn++) {
                String txnId = "txn-" + partition + "-" + txn;
                ctx.collect(TxnEvent.begin(partition, txnId));

                ctx.collect(TxnEvent.data(partition, txnId, "test", "orders",
                        String.format("{\"order_id\":%d, \"customer_id\":%d, " +
                                        "\"total_amount\":%.2f, \"order_status\":\"created\"}",
                                partition * 1000L + txn, txn + 100L, 99.99 + txn)));

                ctx.collect(TxnEvent.data(partition, txnId, "test", "order_items",
                        String.format("{\"item_id\":%d, \"order_id\":%d, " +
                                        "\"product_name\":\"product-%d\", \"quantity\":%d, \"price\":%.2f}",
                                partition * 10000L + txn * 10,
                                partition * 1000L + txn, txn, txn + 1, 49.99 + txn)));

                ctx.collect(TxnEvent.end(partition, txnId));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // ==================== Transaction Assembler ====================

    /**
     * Buffers DATA events per partition; on TXN_END emits the complete
     * transaction's rows as individual {@link DefaultStarRocksRowData} records.
     *
     * <p>Only emits when a Kafka transaction is fully closed (TXN_END received).
     * All rows are emitted synchronously within one {@code processElement()} call,
     * so they enter the downstream sink buffer without intervening checkpoint barriers.
     *
     * <p>Multiple Kafka transactions accumulate in the sink's buffer between
     * flush cycles—the connector handles grouping them into StarRocks transactions.
     */
    static class TransactionAssembler
            extends KeyedProcessFunction<Integer, TxnEvent, DefaultStarRocksRowData> {

        private transient ListState<TxnEvent> pendingRows;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<TxnEvent> descriptor = new ListStateDescriptor<>(
                    "pending-txn-rows", Types.POJO(TxnEvent.class));
            pendingRows = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(TxnEvent event, Context ctx,
                                   Collector<DefaultStarRocksRowData> out) throws Exception {
            switch (event.type) {
                case TXN_BEGIN:
                    pendingRows.clear();
                    break;

                case DATA:
                    pendingRows.add(event);
                    break;

                case TXN_END:
                    List<TxnEvent> rows = new ArrayList<>();
                    for (TxnEvent row : pendingRows.get()) {
                        rows.add(row);
                    }
                    int partition = ctx.getCurrentKey();
                    for (int i = 0; i < rows.size(); i++) {
                        TxnEvent row = rows.get(i);
                        DefaultStarRocksRowData rowData = new DefaultStarRocksRowData(
                                null, row.database, row.table, row.json);
                        rowData.setSourcePartition(partition);
                        if (i == rows.size() - 1) {
                            rowData.setTransactionEnd(true);
                        }
                        out.collect(rowData);
                    }
                    pendingRows.clear();
                    break;
            }
        }
    }

    // ==================== Main ====================

    public static void main(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        String jdbcUrl = params.get("jdbcUrl", "jdbc:mysql://127.0.0.1:9030");
        String loadUrl = params.get("loadUrl", "127.0.0.1:8030");
        String userName = params.get("userName", "root");
        String password = params.get("password", "");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);

        DataStream<TxnEvent> events = env.addSource(new MockTxnEventSource());

        // TransactionAssembler emits rows only after TXN_END.
        // Multiple closed Kafka txns accumulate in the sink buffer between flushes.
        DataStream<DefaultStarRocksRowData> rows = events
                .keyBy(e -> e.partition)
                .process(new TransactionAssembler());

        // keyBy(sourcePartition) routes same-partition data to the same sink subtask.
        // The connector uses per-partition regions internally, so even when multiple
        // partitions land on the same sink subtask, transaction boundaries are tracked
        // independently per partition via PartitionCommitTracker.
        DataStream<DefaultStarRocksRowData> partitionedRows = rows
                .keyBy(DefaultStarRocksRowData::getSourcePartition);

        // sink.transaction.multi-table.enabled=true activates per-partition region
        // tracking inside StreamLoadManagerV2: each partition's regions are switched
        // independently when its txnEnd arrives, and commit triggers only when all
        // active partitions have been switched.
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", jdbcUrl)
                .withProperty("load-url", loadUrl)
                .withProperty("database-name", "*")
                .withProperty("table-name", "*")
                .withProperty("username", userName)
                .withProperty("password", password)
                .withProperty("sink.version", "V2")
                .withProperty("sink.semantic", "at-least-once")
                .withProperty("sink.transaction.multi-table.enabled", "true")
                .withProperty("sink.buffer-flush.interval-ms", "1000")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();

        StreamLoadTableProperties orderItemsProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("order_items")
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build();
        options.addTableProperties(orderItemsProps);

        // Standard connector API — no custom SinkFunction needed.
        // addSink on the keyBy'd stream ensures partition affinity.
        SinkFunction<DefaultStarRocksRowData> sink = SinkFunctionFactory.createSinkFunction(options);
        partitionedRows.addSink(sink);

        env.execute("WriteMultipleTablesWithTransaction");
    }
}
