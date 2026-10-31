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

package com.starrocks.connector.flink.table.sink;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;

import com.starrocks.connector.flink.catalog.StarRocksUtils;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StarRocksDynamicTableSinkFactoryTest {

    @Test
    public void testToPhysicalSchemaDropsNonPhysicalColumns() {
        Column physical = Column.physical("c1", DataTypes.INT().notNull());
        Column metadata = Column.metadata("m1", DataTypes.STRING(), null, true);
        ResolvedSchema schema = new ResolvedSchema(
                Arrays.asList(physical, metadata),
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Collections.singletonList("c1")));

        ResolvedSchema result = StarRocksUtils.toPhysicalSchema(schema);

        assertEquals(Collections.singletonList(physical), result.getColumns());
        assertTrue(result.getPrimaryKey().isPresent());
        assertEquals(Collections.singletonList("c1"), result.getPrimaryKey().get().getColumns());
    }
}
