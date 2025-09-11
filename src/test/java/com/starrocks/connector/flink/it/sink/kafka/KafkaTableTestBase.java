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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

import com.starrocks.connector.flink.it.env.StarRocksTestEnvironment;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

/**
 * Base class for Kafka Table IT Cases. Refer to Flink's KafkaTableTestBase.
 */
public abstract class KafkaTableTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTableTestBase.class);

    private static final boolean DEBUG_MODE = false;

    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final int zkTimeoutMills = 30000;

    protected static String SR_DB_NAME;
    protected static String SR_HTTP_URLS = "127.0.0.1:8030";
    protected static String SR_JDBC_URLS = "jdbc:mysql://127.0.0.1:9030";

    protected static String getSrHttpUrls() {
        return SR_HTTP_URLS;
    }

    protected static String getSrJdbcUrl() {
        return SR_JDBC_URLS;
    }

    protected static Connection SR_DB_CONNECTION;

    @ClassRule
    public static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.3.0")) {
                @Override
                protected void doStart() {
                    super.doStart();
                    if (LOG.isInfoEnabled()) {
                        this.followOutput(new Slf4jLogConsumer(LOG));
                    }
                }
            }.withEmbeddedZookeeper()
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS)
                    .withEnv(
                            "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                            String.valueOf(Duration.ofHours(2).toMillis()))
                    // Disable log deletion to prevent records from being deleted during test run
                    .withEnv("KAFKA_LOG_RETENTION_MS", "-1");

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    // Timer for scheduling logging task if the test hangs
    private final Timer loggingTimer = new Timer("Debug Logging Timer");

    @BeforeClass
    public static void setUp() throws Exception {
        if (!DEBUG_MODE) {
            StarRocksTestEnvironment env = StarRocksTestEnvironment.getInstance();
            env.startIfNeeded();
            SR_HTTP_URLS = env.getHttpAddress();
            SR_JDBC_URLS = env.getJdbcUrl();
        }
        assertTrue(SR_HTTP_URLS != null && SR_JDBC_URLS != null);

        SR_DB_NAME = "sr_sink_test_" + genRandomUuid();
        try {
            SR_DB_CONNECTION = DriverManager.getConnection(getSrJdbcUrl(), "root", "");
            LOG.info("Success to create db connection via jdbc {}", getSrJdbcUrl());
        } catch (Exception e) {
            LOG.error("Failed to create db connection via jdbc {}", getSrJdbcUrl(), e);
            throw e;
        }

        try {
            String createDb = "CREATE DATABASE " + SR_DB_NAME;
            executeSrSQL(createDb);
            LOG.info("Successful to create database {}", SR_DB_NAME);
        } catch (Exception e) {
            LOG.error("Failed to create database {}", SR_DB_NAME, e);
            throw e;
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (SR_DB_CONNECTION != null) {
            try {
                String dropDb = String.format("DROP DATABASE IF EXISTS %s FORCE", SR_DB_NAME);
                executeSrSQL(dropDb);
                LOG.info("Successful to drop database {}", SR_DB_NAME);
            } catch (Exception e) {
                LOG.error("Failed to drop database {}", SR_DB_NAME, e);
            }
            SR_DB_CONNECTION.close();
        }
    }

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());

        // Probe Kafka broker status per 30 seconds
        scheduleTimeoutLogger(
                Duration.ofSeconds(30),
                () -> {
                    // List all non-internal topics
                    final Map<String, TopicDescription> topicDescriptions =
                            describeExternalTopics();
                    LOG.info("Current existing topics: {}", topicDescriptions.keySet());

                    // Log status of topics
                    logTopicPartitionStatus(topicDescriptions);
                });
    }

    @After
    public void after() {
        // Cancel timer for debug logging
        cancelTimeoutLogger();
    }

    public Properties getStandardProps() {
        Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", "flink-tests");
        standardProps.put("enable.auto.commit", false);
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        standardProps.put("zookeeper.session.timeout.ms", zkTimeoutMills);
        standardProps.put("zookeeper.connection.timeout.ms", zkTimeoutMills);
        return standardProps;
    }

    public String getBootstrapServers() {
        return KAFKA_CONTAINER.getBootstrapServers();
    }

    public void createTestTopic(String topic, int numPartitions, int replicationFactor) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(
                    Collections.singletonList(
                            new NewTopic(topic, numPartitions, (short) replicationFactor)));
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> getConsumerOffset(String groupId) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            ListConsumerGroupOffsetsResult result = admin.listConsumerGroupOffsets(groupId);
            return result.partitionsToOffsetAndMetadata().get(20, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(
                    String.format("Fail to get consumer offsets with the group id [%s].", groupId),
                    e);
        }
    }

    public void deleteTestTopic(String topic) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.deleteTopics(Collections.singletonList(topic));
        }
    }

    // ------------------------ For Debug Logging Purpose ----------------------------------

    private void scheduleTimeoutLogger(Duration period, Runnable loggingAction) {
        TimerTask timeoutLoggerTask =
                new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            loggingAction.run();
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to execute logging action", e);
                        }
                    }
                };
        loggingTimer.schedule(timeoutLoggerTask, 0L, period.toMillis());
    }

    private void cancelTimeoutLogger() {
        loggingTimer.cancel();
    }

    private Map<String, TopicDescription> describeExternalTopics() {
        try (final AdminClient adminClient = AdminClient.create(getStandardProps())) {
            final List<String> topics =
                    adminClient.listTopics().listings().get().stream()
                            .filter(listing -> !listing.isInternal())
                            .map(TopicListing::name)
                            .collect(Collectors.toList());

            return adminClient.describeTopics(topics).all().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to list Kafka topics", e);
        }
    }

    private void logTopicPartitionStatus(Map<String, TopicDescription> topicDescriptions) {
        final Properties properties = getStandardProps();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-tests-debugging");
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getCanonicalName());
        final KafkaConsumer<?, ?> consumer = new KafkaConsumer<String, String>(properties);
        List<TopicPartition> partitions = new ArrayList<>();
        topicDescriptions.forEach(
                (topic, description) ->
                        description
                                .partitions()
                                .forEach(
                                        tpInfo ->
                                                partitions.add(
                                                        new TopicPartition(
                                                                topic, tpInfo.partition()))));
        final Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
        final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        partitions.forEach(
                partition ->
                        LOG.info(
                                "TopicPartition \"{}\": starting offset: {}, stopping offset: {}",
                                partition,
                                beginningOffsets.get(partition),
                                endOffsets.get(partition)));
    }


    protected static String genRandomUuid() {
        return UUID.randomUUID().toString().replace("-", "_");
    }

    protected static void executeSrSQL(String sql) throws Exception {
        try (PreparedStatement statement = SR_DB_CONNECTION.prepareStatement(sql)) {
            statement.execute();
        }
    }
}
