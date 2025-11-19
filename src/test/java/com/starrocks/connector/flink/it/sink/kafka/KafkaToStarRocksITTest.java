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
import org.apache.flink.table.api.TableResult;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

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
        testPrimaryKeyBase(
                "upsert_and_delete_topic",
                "data/sink/debezium-upsert-and-delete.txt",
                Collections.emptyMap(),
                Arrays.asList(
                        Arrays.asList(1, 2.0f, "row1"),
                        Arrays.asList(2, 2.0f, "row2")
                    )
            );
    }

    @Test
    public void testUpdateWithPkChanged() throws Exception {
        testPrimaryKeyBase(
                "update_with_pk_changed_topic",
                "data/sink/debezium-update-with-pk-changed.txt",
                Collections.singletonMap("sink.ignore.update-before", "false"),
                Collections.singletonList(Arrays.asList(2, 2.0f, "row2"))
        );
    }

    private void testPrimaryKeyBase(String topic, String dataFile, Map<String, String> customProperties,
                                    List<List<Object>> expectedData) throws Exception {
        // create kafka topic and write data
        createTestTopic(topic, 1, 1);
        List<String> lines = readLines(dataFile);
        try {
            writeRecordsToKafka(topic, lines);
        } catch (Exception e) {
            throw new Exception("Failed to write debezium data to Kafka.", e);
        }

        // create SR table
        String tableName = "testPrimaryKeyBase_" + genRandomUuid();
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

        Map<String, String> propertiesMap = new HashMap<>();
        propertiesMap.put("connector", "starrocks");
        propertiesMap.put("jdbc-url", getSrJdbcUrl());
        propertiesMap.put("load-url", getSrHttpUrls());
        propertiesMap.put("database-name", SR_DB_NAME);
        propertiesMap.put("table-name", tableName);
        propertiesMap.put("username", "root");
        propertiesMap.put("password", "");
        propertiesMap.put("sink.buffer-flush.interval-ms", "1000");
        propertiesMap.putAll(customProperties);
        StringJoiner joiner = new StringJoiner(",");
        for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
            joiner.add(String.format("'%s' = '%s'", entry.getKey(), entry.getValue()));
        }

        String properties = joiner.toString();
        String sinkDDL = "CREATE TABLE sink("
                                + "c0 INT,"
                                + "c1 FLOAT,"
                                + "c2 STRING,"
                                + "PRIMARY KEY (`c0`) NOT ENFORCED"
                                + ") WITH ( "
                                + properties
                                + ")";

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
        List<List<Object>> actualData = scanTable(SR_DB_CONNECTION, SR_DB_NAME, tableName);
        verifyResult(expectedData, actualData);

        deleteTestTopic(topic);
    }

    private void writeRecordsToKafka(String topic, List<String> lines) throws Exception {
        DataStreamSource<String> stream = env.fromCollection(lines);
        SerializationSchema<String> serSchema = new SimpleStringSchema();
        FlinkFixedPartitioner<String> partitioner = new FlinkFixedPartitioner<>();

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
                        .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                        .setTransactionalIdPrefix("tid")
                        .build());
        env.execute("Write sequence");
    }

}
