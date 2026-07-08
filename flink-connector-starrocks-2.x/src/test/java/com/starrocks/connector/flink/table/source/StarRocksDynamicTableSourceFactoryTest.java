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

package com.starrocks.connector.flink.table.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StarRocksDynamicTableSourceFactoryTest {

    @Test
    public void testSourceGetsPhysicalSchemaOnly() throws Exception {
        // metadata column deliberately first: with the full schema, planner pushdown
        // indexes (based on the physical row) would shift and hit the wrong column
        Column metadata = Column.metadata("m1", DataTypes.STRING(), null, true);
        Column physical = Column.physical("c1", DataTypes.INT());
        ResolvedSchema resolvedSchema = new ResolvedSchema(
                Arrays.asList(metadata, physical), Collections.emptyList(), null);

        Map<String, String> options = new HashMap<>();
        options.put("connector", "starrocks");
        options.put("scan-url", "127.0.0.1:8030");
        options.put("jdbc-url", "jdbc:mysql://127.0.0.1:9030");
        options.put("username", "root");
        options.put("password", "");
        options.put("database-name", "test_db");
        options.put("table-name", "test_table");
        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                .options(options)
                .build();
        FactoryUtil.DefaultDynamicTableContext context = new FactoryUtil.DefaultDynamicTableContext(
                ObjectIdentifier.of("catalog", "test_db", "test_table"),
                new ResolvedCatalogTable(catalogTable, resolvedSchema),
                Collections.emptyMap(),
                new Configuration(),
                Thread.currentThread().getContextClassLoader(),
                false);

        StarRocksDynamicTableSource source = (StarRocksDynamicTableSource)
                new StarRocksDynamicTableSourceFactory().createDynamicTableSource(context);

        Field field = StarRocksDynamicTableSource.class.getDeclaredField("flinkSchema");
        field.setAccessible(true);
        ResolvedSchema schema = (ResolvedSchema) field.get(source);
        assertEquals(Collections.singletonList(physical), schema.getColumns());
    }
}
