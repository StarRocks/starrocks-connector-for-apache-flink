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

package com.starrocks.connector.flink.catalog;

import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StarRocksCatalogFactoryTest {

    @Test
    public void testCreateCatalog() {
        final Map<String, String> options = new HashMap<>();
        options.put("type", "starrocks");
        options.put("jdbc-url", "jdbc:mysql://127.0.0.1:11903");
        options.put("http-url", "127.0.0.1:11901");
        options.put("default-database", "starrocks_db");
        options.put("username", "root");
        options.put("password", "123456");
        options.put("scan.connect.timeout-ms", "2000");
        options.put("scan.params.batch-rows", "8192");
        options.put("sink.semantic", "exactly-once");
        options.put("sink.properties.format", "json");
        options.put("table.num-buckets", "10");
        options.put("table.properties.replication_num", "1");

        Catalog catalog = FactoryUtil.createCatalog(
                        "test_catalog",
                        options,
                        null,
                        Thread.currentThread().getContextClassLoader()
                    );
        assertTrue(catalog instanceof StarRocksCatalog);
        StarRocksCatalog starRocksCatalog = (StarRocksCatalog) catalog;
        assertEquals("test_catalog", starRocksCatalog.getName());
        assertEquals("starrocks_db", starRocksCatalog.getDefaultDatabase());
        assertEquals("jdbc:mysql://127.0.0.1:11903", starRocksCatalog.getJdbcUrl());
        assertEquals("127.0.0.1:11901", starRocksCatalog.getHttpUrl());
        assertEquals("root", starRocksCatalog.getUsername());
        assertEquals("123456", starRocksCatalog.getPassword());

        Map<String, String> sourceBaseConfig = starRocksCatalog.getSourceBaseConfig().toMap();
        assertEquals(2, sourceBaseConfig.size());
        assertEquals("2000", sourceBaseConfig.get("scan.connect.timeout-ms"));
        assertEquals("8192", sourceBaseConfig.get("scan.params.batch-rows"));

        Map<String, String> sinkBaseConfig = starRocksCatalog.getSinkBaseConfig().toMap();
        assertEquals(2, sinkBaseConfig.size());
        assertEquals("exactly-once", sinkBaseConfig.get("sink.semantic"));
        assertEquals("json", sinkBaseConfig.get("sink.properties.format"));

        Map<String, String> tableBaseConfig = starRocksCatalog.getTableBaseConfig().toMap();
        assertEquals(2, tableBaseConfig.size());
        assertEquals("10", tableBaseConfig.get("table.num-buckets"));
        assertEquals("1", tableBaseConfig.get("table.properties.replication_num"));
    }
}
