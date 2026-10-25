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

package com.starrocks.connector.flink.it.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;

import com.starrocks.connector.flink.it.StarRocksITTestBase;
import com.starrocks.connector.flink.table.source.StarRocksSourceBeReader;
import com.starrocks.connector.flink.table.source.StarRocksSourceCommonFunc;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import com.starrocks.connector.flink.table.source.StarRocksSourceSplit;
import com.starrocks.connector.flink.table.source.StarRocksSplitReader;
import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.QueryPlan;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import org.junit.Test;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/** Reads a split whose query plan the BE no longer serves, as after a restore from an old checkpoint. */
public class StarRocksSourcePlanExpiryITTest extends StarRocksITTestBase {

    private static final int BATCHES = 6;
    private static final int BATCH_ROWS = 100;
    private static final String[] SWEEP_CONFIGS = {
            "tablet_rowset_stale_sweep_time_sec", "min_garbage_sweep_interval", "max_garbage_sweep_interval"};

    @Test
    public void testSplitWithAnExpiredPlanIsReadWithANewPlan() throws Exception {
        assumeTrue("needs UPDATE information_schema.be_configs", isStarRocksVersionAtLeast(3, 5));
        String tableName = "plan_expiry_" + genRandomUuid();
        executeSrSQL(String.format("CREATE TABLE `%s`.`%s` (c0 INT) ENGINE = OLAP DUPLICATE KEY(c0) " +
                "DISTRIBUTED BY HASH (c0) BUCKETS 1 PROPERTIES (\"replication_num\" = \"1\")", DB_NAME, tableName));
        insertBatch(tableName, 0);

        StarRocksSourceOptions options = options(tableName);
        ResolvedSchema schema = ResolvedSchema.physical(new String[] {"c0"}, new DataType[] {DataTypes.INT()});
        Map<String, ColumnRichInfo> columnMap = StarRocksSourceCommonFunc.genColumnMap(schema);
        List<ColumnRichInfo> columnRichInfos = StarRocksSourceCommonFunc.genColumnRichInfo(columnMap);
        SelectColumn[] selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(
                columnMap, options, columnRichInfos);
        String sql = String.format("select `c0` from `%s`.`%s`", DB_NAME, tableName);
        QueryInfo queryInfo = StarRocksSourceCommonFunc.getQueryInfo(options, sql);
        QueryBeXTablets beXTablets = queryInfo.getBeXTablets().get(0);
        StarRocksSourceSplit split = new StarRocksSourceSplit(
                beXTablets, "split-0-" + beXTablets.getBeNode(), queryInfo.getQueryPlan().getOpaqued_query_plan());

        for (int batch = 1; batch < BATCHES; batch++) {
            insertBatch(tableName, batch);
        }
        Map<String, String> defaults = beConfigs();
        try {
            for (String name : SWEEP_CONFIGS) {
                setBeConfig(name, "1");
            }
            executeSrSQL(String.format("ALTER TABLE `%s`.`%s` COMPACT", DB_NAME, tableName));
            awaitPlanExpired(split, options, columnRichInfos, selectColumns);
        } finally {
            for (Map.Entry<String, String> config : defaults.entrySet()) {
                setBeConfig(config.getKey(), config.getValue());
            }
        }

        Callable<QueryPlan> planner = () -> StarRocksSourceCommonFunc.getQueryInfo(options, sql).getQueryPlan();
        try {
            readSplit(new StarRocksSplitReader(options(tableName, 0), columnRichInfos, selectColumns, planner), split);
            fail("without retries the split must fail on its expired plan");
        } catch (IOException expected) {
            assertTrue(expected.getCause().getMessage(), expected.getCause().getMessage().contains("compacted"));
        }

        // the new plan reads the table as it is now, including the batches written after the first plan
        Set<Integer> rows = readSplit(new StarRocksSplitReader(options, columnRichInfos, selectColumns, planner), split);
        assertEquals(BATCHES * BATCH_ROWS, rows.size());
    }

    private static Set<Integer> readSplit(StarRocksSplitReader reader, StarRocksSourceSplit split) throws IOException {
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
        Set<Integer> rows = new HashSet<>();
        try {
            while (true) {
                RecordsWithSplitIds<GenericRowData> records = reader.fetch();
                while (records.nextSplit() != null) {
                    GenericRowData row;
                    while ((row = records.nextRecordFromSplit()) != null) {
                        rows.add(row.getInt(0));
                    }
                }
                if (records.finishedSplits().contains(split.splitId())) {
                    return rows;
                }
            }
        } finally {
            reader.close();
        }
    }

    private static void awaitPlanExpired(StarRocksSourceSplit split, StarRocksSourceOptions options,
                                         List<ColumnRichInfo> columnRichInfos, SelectColumn[] selectColumns)
            throws Exception {
        long deadline = System.currentTimeMillis() + 120_000;
        while (true) {
            QueryBeXTablets beXTablets = split.getBeXTablets();
            StarRocksSourceBeReader beReader = new StarRocksSourceBeReader(
                    beXTablets.getBeNode(), columnRichInfos, selectColumns, options);
            try {
                beReader.openScanner(beXTablets.getTabletIds(), split.getOpaquedQueryPlan(), options);
                beReader.startToRead();
            } catch (RuntimeException expired) {
                return;
            } finally {
                beReader.close();
            }
            assertTrue("the BE still serves the versions of the first plan", System.currentTimeMillis() < deadline);
            Thread.sleep(1000);
        }
    }

    private static void insertBatch(String tableName, int batch) throws Exception {
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < BATCH_ROWS; i++) {
            values.append(i == 0 ? "" : ",").append('(').append(batch * BATCH_ROWS + i).append(')');
        }
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES %s", DB_NAME, tableName, values));
    }

    private static Map<String, String> beConfigs() throws Exception {
        Map<String, String> configs = new LinkedHashMap<>();
        String names = "'" + String.join("','", SWEEP_CONFIGS) + "'";
        try (PreparedStatement statement = DB_CONNECTION.prepareStatement(
                "SELECT NAME, VALUE FROM information_schema.be_configs WHERE NAME IN (" + names + ")");
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                configs.put(resultSet.getString(1), resultSet.getString(2));
            }
        }
        assertEquals(SWEEP_CONFIGS.length, configs.size());
        return configs;
    }

    private static void setBeConfig(String name, String value) throws Exception {
        executeSrSQL(String.format(
                "UPDATE information_schema.be_configs SET VALUE = '%s' WHERE NAME = '%s'", value, name));
    }

    private static StarRocksSourceOptions options(String tableName) {
        return options(tableName, StarRocksSourceOptions.SCAN_MAX_RETRIES.defaultValue());
    }

    private static StarRocksSourceOptions options(String tableName, int maxRetries) {
        Configuration conf = new Configuration();
        conf.set(StarRocksSourceOptions.SCAN_MAX_RETRIES, maxRetries);
        conf.set(StarRocksSourceOptions.SCAN_URL, getHttpUrls());
        conf.set(StarRocksSourceOptions.JDBC_URL, getJdbcUrl());
        conf.set(StarRocksSourceOptions.USERNAME, USERNAME);
        conf.set(StarRocksSourceOptions.PASSWORD, PASSWORD);
        conf.set(StarRocksSourceOptions.DATABASE_NAME, DB_NAME);
        conf.set(StarRocksSourceOptions.TABLE_NAME, tableName);
        return new StarRocksSourceOptions(conf, conf.toMap());
    }
}
