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

package com.starrocks.connector.flink.it;

import com.esotericsoftware.minlog.Log;
import com.starrocks.connector.flink.container.StarRocksCluster;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

/** Abstract IT case class for StarRocks. */
public abstract class StarRocksITTestBase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksITTestBase.class);

    public static StarRocksCluster STARROCKS_CLUSTER =
            new StarRocksCluster(1, 0, 3);

    protected static Connection DB_CONNECTION;

    @BeforeClass
    public static void setUp() throws Exception {
        try {
            STARROCKS_CLUSTER.start();
            LOG.info("StarRocks cluster is started.");
        } catch (Exception e) {
            LOG.error("Failed to star StarRocks cluster.", e);
            throw e;
        }
        Log.info("StarRocks cluster try to connect.");
        try {
            DB_CONNECTION = DriverManager.getConnection(getJdbcUrl(), "root", "");
            LOG.info("Success to create db connection via jdbc {}", getJdbcUrl());
        } catch (Exception e) {
            LOG.error("Failed to create db connection via jdbc {}", getJdbcUrl(), e);
            throw e;
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (DB_CONNECTION != null) {
            DB_CONNECTION.close();
            LOG.info("Close db connection");
        }

        if (STARROCKS_CLUSTER != null) {
            STARROCKS_CLUSTER.stop();
            STARROCKS_CLUSTER.close();
            Log.info("Stop and close StarRocks cluster.");
        }
    }

    protected static String getJdbcUrl() {
        return "jdbc:mysql://" + STARROCKS_CLUSTER.getQueryUrls();
    }

    /**
     * Query the data in the table, and compare the result with the expected data.
     *
     * <p> For example, we can call this method as follows:
     *      verifyResult(
     *                 new Row[] {
     *                     Row.of("1", 1.4, Timestamp.valueOf("2022-09-06 20:00:00.000")),
     *                     Row.of("2", 2.4, Timestamp.valueOf("2022-09-06 20:00:00.000")),
     *                 },
     *                 "select * from db.table"
     *             );
     */
    public static void verifyResult(Row[] expectedData, String sql) throws Exception {
        ArrayList<Row> resultData = getSQLResult(sql);
        LOG.info("resultData: {}",resultData);
        LOG.info("expectedData: {}",Arrays.asList(expectedData));
        assertEquals(expectedData.length, resultData.size());
        for (int i = 0; i < expectedData.length; i++) {
            assertEquals(expectedData[i].toString(), resultData.get(i).toString());
        }
    }

    public static ArrayList<Row> getSQLResult(String sql) throws Exception {
        if (!DB_CONNECTION.isValid(3000)) {
            reestablishConnection();
        }
        ArrayList<Row> resultData = new ArrayList<>();
        try (PreparedStatement statement = DB_CONNECTION.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                Row row = new Row(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    row.setField(i, resultSet.getObject(i + 1));
                }
                resultData.add(row);
            }
        }
        return resultData;
    }

    private static void reestablishConnection() throws Exception{
        try {
            DB_CONNECTION = DriverManager.getConnection(getJdbcUrl(), "root", "");
            LOG.info("Success to create db connection via jdbc {}", getJdbcUrl());
        } catch (Exception e) {
            LOG.error("Failed to create db connection via jdbc {}", getJdbcUrl(), e);
            throw e;
        }
    }

    public static void verifyResult(Function<Row[], Boolean> verifyFunction, String sql) throws Exception {
        ArrayList<Row> resultData = new ArrayList<>();
        if (!DB_CONNECTION.isValid(3000)) {
            reestablishConnection();
        }
        try (PreparedStatement statement = DB_CONNECTION.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                Row row = new Row(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    row.setField(i, resultSet.getObject(i + 1));
                }
                resultData.add(row);
            }
        }
        if (!verifyFunction.apply(resultData.toArray(new Row[0])))
        {
            LOG.info("verifyResult is : {}", resultData.toString());
            Assert.fail();
        }
    }
}
