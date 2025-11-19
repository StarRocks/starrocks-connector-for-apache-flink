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

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.row.sink.StarRocksSinkOP;
import com.starrocks.connector.flink.row.sink.StarRocksSinkRowBuilder;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.legacy.api.TableSchema;

/**
 * This example will show how to load records to StarRocks table using Flink DataStream.
 * Each record is a user-defined java object {@link RowData} in Flink, and will be loaded
 * as a row of StarRocks table.
 */
public class LoadCustomJavaRecords {

    public static void main(String[] args) throws Exception {
        // To run the example, you should prepare in the following steps
        // 1. create a primary key table in your StarRocks cluster. The DDL is
        //  CREATE DATABASE `test`;
        //    CREATE TABLE `test`.`score_board`
        //    (
        //        `id` int(11) NOT NULL COMMENT "",
        //        `name` varchar(65533) NULL DEFAULT "" COMMENT "",
        //        `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
        //    )
        //    ENGINE=OLAP
        //    PRIMARY KEY(`id`)
        //    COMMENT "OLAP"
        //    DISTRIBUTED BY HASH(`id`)
        //    PROPERTIES(
        //        "replication_num" = "1"
        //    );
        //
        // 2. replace the connector options "jdbc-url" and "load-url" with your cluster configurations
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        String jdbcUrl = params.get("jdbcUrl", "jdbc:mysql://127.0.0.1:9030");
        String loadUrl = params.get("loadUrl", "127.0.0.1:8030");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Generate records which use RowData as the container.
        RowData[] records = new RowData[]{
                new RowData(1, "starrocks-rowdata", 100),
                new RowData(2, "flink-rowdata", 100),
            };
        DataStream<RowData> source = env.fromElements(records);

        // Configure the connector with the required properties
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", jdbcUrl)
                .withProperty("load-url", loadUrl)
                .withProperty("database-name", "test")
                .withProperty("table-name", "score_board")
                .withProperty("username", "root")
                .withProperty("password", "")
                .build();

        // connector will use a Java object array (Object[]) to represent a row of
        // StarRocks table, and each element is the value for a column. Need to define
        // the schema of the Object[] which matches that of StarRocks table
        TableSchema schema = TableSchema.builder()
                .field("id", DataTypes.INT().notNull())
                .field("name", DataTypes.STRING())
                .field("score", DataTypes.INT())
                // Must specify the primary key for StarRocks primary key table,
                // and DataTypes.INT().notNull() for `id` must specify notNull()
                .primaryKey("id")
                .build();
        // Transforms the RowData to the Object[] according to the schema
        RowDataTransformer transformer = new RowDataTransformer();
        // Create the sink with schema, options, and transformer
        SinkFunction<RowData> starRockSink = StarRocksSink.sink(schema, options, transformer);
        source.addSink(starRockSink);

        env.execute("LoadCustomJavaRecords");
    }

    /**
     * A simple POJO which includes three fields: id, name and core,
     * which match the schema of the StarRocks table `score_board`.
     */
    public static class RowData {
        public int id;
        public String name;
        public int score;

        public RowData() {}

        public RowData(int id, String name, int score) {
            this.id = id;
            this.name = name;
            this.score = score;
        }
    }

    /**
     * Transforms the input {@link RowData} to an Object[] which is the internal
     * representation for a row in StarRocks table.
     */
    private static class RowDataTransformer implements StarRocksSinkRowBuilder<RowData> {

        /**
         * Set each element of the object array according to the input RowData.
         * The schema of the array matches that of StarRocks table.
         */
        @Override
        public void accept(Object[] internalRow, RowData rowData) {
            internalRow[0] = rowData.id;
            internalRow[1] = rowData.name;
            internalRow[2] = rowData.score;
            // Only need for StarRocks primary key table. Set the last
            // element to tell whether the record is a UPSERT or DELETE.
            internalRow[internalRow.length - 1] = StarRocksSinkOP.UPSERT.ordinal();
        }
    }

}