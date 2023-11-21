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
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * This example will show how to use one sink to write data to multiple StarRocks tables .
 * Note this example requires connector version >= 1.2.9.
 */
public class WriteMultipleTables {

    public static void main(String[] args) throws Exception {
        // To run the example, you should prepare in the following steps
        //
        // 1. create two primary key tables in your StarRocks cluster. The DDL is
        //  CREATE DATABASE `test`;
        //    CREATE TABLE `test`.`tbl1`
        //    (
        //        `id` int(11) NOT NULL COMMENT "",
        //        `name` varchar(65533) NULL DEFAULT "" COMMENT "",
        //        `score` int(11) DEFAULT "0" COMMENT ""
        //    )
        //    ENGINE=OLAP
        //    PRIMARY KEY(`id`)
        //    COMMENT "OLAP"
        //    DISTRIBUTED BY HASH(`id`)
        //    PROPERTIES(
        //        "replication_num" = "1"
        //    );
        //
        //    CREATE TABLE `test`.`tbl2`
        //    (
        //        `order_id` BIGINT NOT NULL COMMENT "",
        //        `order_state` INT DEFAULT "0" COMMENT "",
        //        `total_price` BIGINT DEFAULT "0" COMMENT ""
        //    )
        //    ENGINE=OLAP
        //    PRIMARY KEY(`order_id`)
        //    COMMENT "OLAP"
        //    DISTRIBUTED BY HASH(`order_id`)
        //    PROPERTIES(
        //        "replication_num" = "1"
        //    );
        //
        // 2. replace the connector options with your cluster configurations
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        String jdbcUrl = params.get("jdbcUrl", "jdbc:mysql://127.0.0.1:11903");
        String loadUrl = params.get("loadUrl", "127.0.0.1:11901");
        String userName = params.get("userName", "root");
        String password = params.get("password", "");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Generate records for tables `test`.`tbl1` and `test`.`tbl2`. Each record is represented with
        // the structure StarRocksRowData. A StarRocksRowData includes the meta (which db and table)
        // and data. The data is a json-format string whose fields correspond to partial or all columns
        // of the table.
        StarRocksRowData[] records =
            new StarRocksRowData[]{
                // write full columns of `test`.`tbl1`: `id`, `name` and `score`
                buildRow("test", "tbl1", "{\"id\":1, \"name\":\"starrocks-json\", \"score\":100}"),
                // write partial columns of `test`.`tbl1`: `id`, `name`
                buildRow("test", "tbl1", "{\"id\":2, \"name\":\"flink-json\"}"),
                // write full columns of `test`.`tbl2`: `order_id`, `order_state` and `total_price`
                buildRow("test", "tbl2", "{\"order_id\":1, \"order_state\":1, \"total_price\":100}"),
                // write partial columns of `test`.`tbl2`: `order_id`, `order_state`
                buildRow("test", "tbl2", "{\"order_id\":2, \"order_state\":2}"),
            };
        DataStream<StarRocksRowData> source = env.fromElements(records);

        // Configure the connector with the required properties, and you also need to add properties
        // "sink.properties.format" and "sink.properties.strip_outer_array" to tell the connector the
        // input records are json-format.
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", jdbcUrl)
                .withProperty("load-url", loadUrl)
                .withProperty("database-name", "*")
                .withProperty("table-name", "*")
                .withProperty("username", userName)
                .withProperty("password", password)
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .withProperty("sink.properties.ignore_json_size", "true")
                .build();

        // By default, all tables will use the stream load properties defined when building
        // StarRocksSinkOptions. You can also customize the stream load properties for each
        // table. Here we create custom properties for the table `test`.`tbl2`, and the main
        // difference is that enable partial_update. The table `test`.`tbl2` still use the
        // default properties.
        StreamLoadTableProperties tbl2Properties = StreamLoadTableProperties.builder()
                .database("test")
                .table("tbl2")
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .addProperty("partial_update", "true")
                .addProperty("columns", "`order_id`,`order_state`")
                .build();
        options.addTableProperties(tbl2Properties);

        // Create the sink with the options
        SinkFunction<StarRocksRowData> starRockSink = SinkFunctionFactory.createSinkFunction(options);
        source.addSink(starRockSink);

        env.execute("WriteMultipleTables");
    }

    private static StarRocksRowData buildRow(String db, String table, String data) {
        return new DefaultStarRocksRowData(null, db, table, data);
    }
}
