/*
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

package com.starrocks.connector.flink.it.sink.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.scanTable;
import static com.starrocks.connector.flink.it.sink.StarRocksTableUtils.verifyResult;
import static com.starrocks.connector.flink.it.sink.kafka.KafkaTableTestUtils.readLines;

// Tests for the pipeline from kafka to starrocks. Refer to Flink's KafkaChangelogTableITCase.
public class KafkaToStarRocksITTest extends KafkaTableTestBase {

    @Before
    public void before() {
        // we have to use single parallelism,
        // because we will count the messages in sink to terminate the job
        env.setParallelism(1);
    }

    @Test
    public void testUpsertAndDelete() throws Exception {
        // create kafka topic and write data
        final String topic = "upsert_and_delete_topic";
        createTestTopic(topic, 1, 1);
        List<String> lines = readLines("data/debezium-upsert-and-delete.txt");
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write debezium data to Kafka.", e);
        }

        // create SR table
        String tableName = "testUpsertAndDelete_" + genRandomUuid();
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 FLOAT," +
                                "c2 STRING" +
                                ") ENGINE = OLAP " +
                                "PRIMARY KEY(c0) " +
                                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        SR_DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);

        // ---------- Produce an event time stream into Kafka -------------------
        String bootstraps = getBootstrapServers();
        String sourceDDL =
                String.format(
                        "CREATE TABLE debezium_source ("
                                + " c0 INT NOT NULL,"
                                + " c1 FLOAT,"
                                + " c2 STRING,"
                                + " PRIMARY KEY (c0) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'kafka',"
                                + " 'topic' = '%s',"
                                + " 'properties.bootstrap.servers' = '%s',"
                                + " 'scan.startup.mode' = 'earliest-offset',"
                                + " 'value.format' = 'debezium-json'"
                                + ")",
                        topic, bootstraps);
        String sinkDDL =
               String.format(
                        "CREATE TABLE sink("
                                + "c0 INT,"
                                + "c1 FLOAT,"
                                + "c2 STRING,"
                                + "PRIMARY KEY (`c0`) NOT ENFORCED"
                                + ") WITH ( "
                                + "'connector' = 'starrocks',"
                                + "'jdbc-url'='%s',"
                                + "'load-url'='%s',"
                                + "'database-name' = '%s',"
                                + "'table-name' = '%s',"
                                + "'username' = '%s',"
                                + "'password' = '%s',"
                                + "'sink.buffer-flush.interval-ms' = '1000'"
                                + ")",
                       getSrJdbcUrl(), getSrHttpUrls(), SR_DB_NAME, tableName, "root", "");

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        TableResult tableResult = tEnv.executeSql("INSERT INTO sink SELECT * FROM debezium_source");
        // TODO find an elegant way to wait for the result
        try {
            Thread.sleep(10000);
        } catch (Exception e) {
            // ignore
        }
        tableResult.getJobClient().get().cancel().get(); // stop the job

        List<List<Object>> expectedData = Arrays.asList(
                Arrays.asList(1, 2.0f, "row1"),
                Arrays.asList(2, 2.0f, "row2")
        );
        List<List<Object>> actualData = scanTable(SR_DB_CONNECTION, SR_DB_NAME, tableName);
        verifyResult(expectedData, actualData);

        deleteTestTopic(topic);
    }

    private void writeRecordsToKafka(String topic, List<String> lines) throws Exception {
        DataStreamSource<String> stream = env.fromCollection(lines);
        SerializationSchema<String> serSchema = new SimpleStringSchema();
        FlinkKafkaPartitioner<String> partitioner = new FlinkFixedPartitioner<>();

        // the producer must not produce duplicates
        Properties producerProperties = getStandardProps();
        producerProperties.setProperty("retries", "0");
        stream.sinkTo(
                KafkaSink.<String>builder()
                        .setBootstrapServers(
                                producerProperties.getProperty(
                                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(topic)
                                        .setValueSerializationSchema(serSchema)
                                        .setPartitioner(partitioner)
                                        .build())
                        .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .build());
        env.execute("Write sequence");
    }

}
