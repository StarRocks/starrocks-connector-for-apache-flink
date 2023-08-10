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
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * This example will show how to load records to StarRocks table using Flink DataStream.
 * Each record is a csv {@link String} in Flink, and will be loaded as a row of StarRocks table.
 */
public class LoadCsvRecords {

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

        // Generate csv-format records. Each record has three fields separated by "\t". These
        // fields correspond to the columns `id`, `name`, and `score` in StarRocks table.
        String[] records = new String[]{
                "1\tstarrocks-csv\t100",
                "2\tflink-csv\t100"
        };
        DataStream<String> source = env.fromElements(records);

        // Configure the connector with the required properties, and you also need to add properties
        // "sink.properties.format" and "sink.properties.column_separator" to tell the connector the
        // input records are csv-format, and the separator is "\t". You can also use other separators
        // in the records, but remember to modify the "sink.properties.column_separator" correspondingly
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", jdbcUrl)
                .withProperty("load-url", loadUrl)
                .withProperty("database-name", "test")
                .withProperty("table-name", "score_board")
                .withProperty("username", "root")
                .withProperty("password", "")
                .withProperty("sink.properties.format", "csv")
                .withProperty("sink.properties.column_separator", "\t")
                .build();
        // Create the sink with the options
        SinkFunction<String> starRockSink = StarRocksSink.sink(options);
        source.addSink(starRockSink);

        env.execute("LoadCsvRecords");
    }
}