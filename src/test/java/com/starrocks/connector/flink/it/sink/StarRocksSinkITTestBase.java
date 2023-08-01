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

package com.starrocks.connector.flink.it.sink;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.UUID;

import static org.junit.Assume.assumeTrue;

public abstract class StarRocksSinkITTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkITTestBase.class);

    private static final boolean DEBUG_MODE = false;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected static String DB_NAME;
    protected static String HTTP_URLS;
    protected static String JDBC_URLS;

    protected static String getHttpUrls() {
        return HTTP_URLS;
    }

    protected static String getJdbcUrl() {
        return JDBC_URLS;
    }

    protected static Connection DB_CONNECTION;
    @BeforeClass
    public static void setUp() throws Exception {
        HTTP_URLS = DEBUG_MODE ? "127.0.0.1:11901" : System.getProperty("http_urls");
        JDBC_URLS = DEBUG_MODE ? "jdbc:mysql://127.0.0.1:11903" : System.getProperty("jdbc_urls");
        assumeTrue(HTTP_URLS != null && JDBC_URLS != null);

        DB_NAME = "sr_sink_test_" + genRandomUuid();
        try {
            DB_CONNECTION = DriverManager.getConnection(getJdbcUrl(), "root", "");
            LOG.info("Success to create db connection via jdbc {}", getJdbcUrl());
        } catch (Exception e) {
            LOG.error("Failed to create db connection via jdbc {}", getJdbcUrl(), e);
            throw e;
        }

        try {
            String createDb = "CREATE DATABASE " + DB_NAME;
            executeSrSQL(createDb);
            LOG.info("Successful to create database {}", DB_NAME);
        } catch (Exception e) {
            LOG.error("Failed to create database {}", DB_NAME, e);
            throw e;
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (DB_CONNECTION != null) {
            try {
                String dropDb = String.format("DROP DATABASE IF EXISTS %s FORCE", DB_NAME);
                executeSrSQL(dropDb);
                LOG.info("Successful to drop database {}", DB_NAME);
            } catch (Exception e) {
                LOG.error("Failed to drop database {}", DB_NAME, e);
            }
            DB_CONNECTION.close();
        }
    }

    protected static String genRandomUuid() {
        return UUID.randomUUID().toString().replace("-", "_");
    }

    protected static void executeSrSQL(String sql) throws Exception {
        try (PreparedStatement statement = DB_CONNECTION.prepareStatement(sql)) {
            statement.execute();
        }
    }
}
