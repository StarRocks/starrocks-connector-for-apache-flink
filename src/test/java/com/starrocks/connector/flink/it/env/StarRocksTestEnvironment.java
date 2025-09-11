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

package com.starrocks.connector.flink.it.env;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.FixedHostPortGenericContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;

/**
 * Simple Testcontainers-based StarRocks all-in-one environment for ITs.
 * Not considering cross-run reuse; one container per JVM when needed.
 */
public final class StarRocksTestEnvironment {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksTestEnvironment.class);

    private static final int FE_HTTP_PORT = 8030;
    private static final int FE_MYSQL_PORT = 9030;
    private static final int BE_HTTP_PORT = 8040;
    private static final int BE_THRIFT_PORT = 9060;
    private static final int BE_BRPC_PORT = 8060;

    private static final String DEFAULT_IMAGE = "starrocks/allin1-ubuntu:3.5.5";

    private static volatile StarRocksTestEnvironment INSTANCE;

    private static String PROBE_DB_NAME = "_probe_db_";
    private static String PROBE_DB_DDL =
        String.format("CREATE DATABASE IF NOT EXISTS `%s`", PROBE_DB_NAME);
    private static String PROBE_TABLE_NAME = "_probe_table_";
    private static String PROBE_TABLE_DDL =
            String.format("CREATE TABLE IF NOT EXISTS `%s`.`%s` (id INT)", PROBE_DB_NAME, PROBE_TABLE_NAME);
    private static String PROBE_TABLE_DML = 
            String.format("INSERT INTO `%s`.`%s` VALUES (1)", PROBE_DB_NAME, PROBE_TABLE_NAME);
    private static String PROBE_TABLE_DQL = 
        String.format("SELECT COUNT(*) FROM `%s`.`%s`", PROBE_DB_NAME, PROBE_TABLE_NAME);

    private FixedHostPortGenericContainer<?> container;
    private String httpAddress;
    private String jdbcUrl;
    private String username = "root";
    private String password = "";
    private boolean started;

    private StarRocksTestEnvironment() {
    }

    public static StarRocksTestEnvironment getInstance() {
        if (INSTANCE == null) {
            synchronized (StarRocksTestEnvironment.class) {
                if (INSTANCE == null) {
                    INSTANCE = new StarRocksTestEnvironment();
                }
            }
        }
        return INSTANCE;
    }

    @SuppressWarnings({"resource", "deprecation"})
    public synchronized void startIfNeeded() {
        if (started) {
            return;
        }
        String image = System.getProperty("it.starrocks.image", DEFAULT_IMAGE);
        try {
            container = new FixedHostPortGenericContainer<>(image)
                    .withStartupTimeout(Duration.ofMinutes(8))
                    .withFixedExposedPort(FE_HTTP_PORT, FE_HTTP_PORT)
                    .withFixedExposedPort(FE_MYSQL_PORT, FE_MYSQL_PORT)
                    .withFixedExposedPort(BE_HTTP_PORT, BE_HTTP_PORT)
                    .withFixedExposedPort(BE_THRIFT_PORT, BE_THRIFT_PORT)
                    .withFixedExposedPort(BE_BRPC_PORT, BE_BRPC_PORT);

            // Optional platform override for Apple Silicon, if user specifies it
            String platform = System.getProperty("it.starrocks.platform");
            if (platform != null && !platform.isEmpty()) {
                container = container.withCreateContainerCmdModifier(cmd -> cmd.withPlatform(platform));
            }

            container.start();

            String host = container.getHost();
            this.httpAddress = host + ":" + FE_HTTP_PORT;
            this.jdbcUrl = "jdbc:mysql://" + host + ":" + FE_MYSQL_PORT;

            waitUntilReady();

            started = true;
            LOG.info("StarRocks Testcontainer started. http={}, jdbc={}", httpAddress, jdbcUrl);
        } catch (Throwable t) {
            LOG.warn("Failed to start StarRocks Testcontainer, will skip ITs if no external cluster is provided.", t);
            safeStop();
        }
    }

    private void waitUntilReady() throws Exception {
        long deadlineMs = System.currentTimeMillis() + Duration.ofMinutes(8).toMillis();
        long sleepMs = 1000L;
        Throwable lastError = null;
        while (System.currentTimeMillis() < deadlineMs) {
            try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
                // Probe by creating DB, creating table, and running a SELECT
                try (PreparedStatement s1 = conn.prepareStatement(PROBE_DB_DDL)) {
                    s1.execute();
                }
                try (PreparedStatement s2 = conn.prepareStatement(PROBE_TABLE_DDL)) {
                    s2.execute();
                }
                try (PreparedStatement s3 = conn.prepareStatement(PROBE_TABLE_DML)) {
                    s3.execute();
                }
                try (PreparedStatement s4 = conn.prepareStatement(PROBE_TABLE_DQL)) {
                    try (ResultSet rs = s4.executeQuery()) {
                        if (rs.next()) {
                            return;
                        }
                    }
                }
            } catch (Throwable t) {
                lastError = t;
            }
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw ie;
            }
            if (sleepMs < 8000L) {
                sleepMs = Math.min(8000L, sleepMs * 2);
            }
        }
        IllegalStateException ex = new IllegalStateException("StarRocks container did not become ready in time.", lastError);
        LOG.error("Container readiness probe failed: {}", ex.getMessage());
        throw ex;
    }

    public synchronized void safeStop() {
        if (container != null) {
            try {
                container.stop();
            } catch (Throwable t) {
                LOG.warn("Error when stopping container", t);
            } finally {
                container = null;
            }
        }
        started = false;
    }

    public String getHttpAddress() {
        return httpAddress;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}


