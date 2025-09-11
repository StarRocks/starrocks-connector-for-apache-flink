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

package com.starrocks.connector.flink.it;

import com.starrocks.connector.flink.it.env.StarRocksTestEnvironment;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public abstract class StarRocksITTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksITTestBase.class);

    private static final boolean DEBUG_MODE = false;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected static String HTTP_URLS = "127.0.0.1:8030";
    protected static String JDBC_URLS = "jdbc:mysql://127.0.0.1:9030";
    protected static String USERNAME = "root";
    protected static String PASSWORD = "";
    protected static String DB_NAME;
    protected static Connection DB_CONNECTION;
    protected static Set<String> DATABASE_SET_TO_CLEAN;

    @BeforeClass
    public static void setUp() throws Exception {
        if (!DEBUG_MODE) {
            try {
                StarRocksTestEnvironment env = StarRocksTestEnvironment.getInstance();
                env.startIfNeeded();
                HTTP_URLS = env.getHttpAddress();
                JDBC_URLS = env.getJdbcUrl();
                USERNAME = env.getUsername();
                PASSWORD = env.getPassword();
            } catch (Throwable t) {
                LOG.warn("Failed to start StarRocks container, ITs may be skipped if no external cluster is provided.", t);
            }
        }
        assertTrue(HTTP_URLS != null && JDBC_URLS != null);

        DB_NAME = "sr_test_" + genRandomUuid();
        try {
            DB_CONNECTION = DriverManager.getConnection(getJdbcUrl(), USERNAME, PASSWORD);
            LOG.info("Success to create db connection via jdbc {}", getJdbcUrl());
        } catch (Exception e) {
            LOG.error("Failed to create db connection via jdbc {}", getJdbcUrl(), e);
            throw e;
        }

        DATABASE_SET_TO_CLEAN = new HashSet<>();
        try {
            DATABASE_SET_TO_CLEAN.add(DB_NAME);
            createDatabase(DB_NAME);
            LOG.info("Successful to create database {}", DB_NAME);
        } catch (Exception e) {
            LOG.error("Failed to create database {}", DB_NAME, e);
            throw e;
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (DB_CONNECTION != null) {
            for (String database : DATABASE_SET_TO_CLEAN) {
                try {
                    String dropDb = String.format("DROP DATABASE IF EXISTS %s FORCE", database);
                    executeSrSQL(dropDb);
                    LOG.info("Successful to drop database {}", DB_NAME);
                } catch (Exception e) {
                    LOG.error("Failed to drop database {}", DB_NAME, e);
                }
            }
            DB_CONNECTION.close();
        }
    }

    protected static String genRandomUuid() {
        return UUID.randomUUID().toString().replace("-", "_");
    }

    protected static void createDatabase(String database) throws Exception {
        try (Statement statement = DB_CONNECTION.createStatement()) {
            DATABASE_SET_TO_CLEAN.add(database);
            statement.executeUpdate(String.format("CREATE DATABASE %s;", database));
        }
    }

    protected static List<String> listDatabase() throws Exception {
        List<String> databaseList = new ArrayList<>();
        try (Statement statement = DB_CONNECTION.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(
                    "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;")) {
                while (resultSet.next()) {
                    String columnValue = resultSet.getString(1);
                    databaseList.add(columnValue);
                }
            }
        }
        return databaseList;
    }

    protected static void executeSrSQL(String sql) throws Exception {
        try (PreparedStatement statement = DB_CONNECTION.prepareStatement(sql)) {
            statement.execute();
        }
    }

    protected static String getStarRocksFeConfigure(String configureName) throws Exception {
        String sql = String.format("admin show frontend config like \"%s\"", configureName);
        try (PreparedStatement statement = DB_CONNECTION.prepareStatement(sql)) {
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new IllegalStateException(
                            String.format("No row returned for FE config '%s'", configureName));
                }

                String value = resultSet.getString("Value");

                if (resultSet.next()) {
                    throw new IllegalStateException(
                            String.format("More than one row returned for FE config '%s'", configureName));
                }

                return value;
            }
        }
    }

    protected static String getHttpUrls() {
        return HTTP_URLS;
    }

    protected static String getJdbcUrl() {
        return JDBC_URLS;
    }

    public static class TransactionInfo {
        public final long transactionId;
        public final String label;
        public final String coordinator;
        public final String transactionStatus;
        public final String loadJobSourceType;
        public final String prepareTime;
        public final String preparedTime;
        public final String commitTime;
        public final String publishTime;
        public final String finishTime;
        public final String reason;
        public final int errorReplicasCount;
        public final String listenerId;
        public final long timeoutMs;
        public final long preparedTimeoutMs;
        public final String errMsg;

        public TransactionInfo(long transactionId, String label, String coordinator, String transactionStatus,
                               String loadJobSourceType, String prepareTime, String preparedTime, String commitTime,
                               String publishTime, String finishTime, String reason, int errorReplicasCount,
                               String listenerId, long timeoutMs, long preparedTimeoutMs, String errMsg) {
            this.transactionId = transactionId;
            this.label = label;
            this.coordinator = coordinator;
            this.transactionStatus = transactionStatus;
            this.loadJobSourceType = loadJobSourceType;
            this.prepareTime = prepareTime;
            this.preparedTime = preparedTime;
            this.commitTime = commitTime;
            this.publishTime = publishTime;
            this.finishTime = finishTime;
            this.reason = reason;
            this.errorReplicasCount = errorReplicasCount;
            this.listenerId = listenerId;
            this.timeoutMs = timeoutMs;
            this.preparedTimeoutMs = preparedTimeoutMs;
            this.errMsg = errMsg;
        }
    }

    protected static List<TransactionInfo> getFinishedTransactionInfo(String labelPrefix) throws Exception {
        String sql = String.format("show proc '/transactions/%s/finished'", DB_NAME);
        List<TransactionInfo> transactionInfoList = new ArrayList<>();
        try (PreparedStatement statement = DB_CONNECTION.prepareStatement(sql)) {
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String label = rs.getString(2);
                    if (labelPrefix == null || (label != null && label.startsWith(labelPrefix))) {
                        long transactionId = rs.getLong(1);
                        String coordinator = rs.getString(3);
                        String transactionStatus = rs.getString(4);
                        String loadJobSourceType = rs.getString(5);
                        String prepareTime = rs.getString(6);
                        String preparedTime = rs.getString(7);
                        String commitTime = rs.getString(8);
                        String publishTime = rs.getString(9);
                        String finishTime = rs.getString(10);
                        String reason = rs.getString(11);
                        int errorReplicasCount = rs.getInt(12);
                        String listenerId = rs.getString(13);
                        long timeoutMs = rs.getLong(14);
                        long preparedTimeoutMs = rs.getLong(15);
                        String errMsg = rs.getString(16);
                        transactionInfoList.add(new TransactionInfo(
                                transactionId, label, coordinator, transactionStatus, loadJobSourceType,
                                prepareTime, preparedTime, commitTime, publishTime, finishTime, reason,
                                errorReplicasCount, listenerId, timeoutMs, preparedTimeoutMs, errMsg));
                    }
                }
            }
        }
        return transactionInfoList;
    }
}
