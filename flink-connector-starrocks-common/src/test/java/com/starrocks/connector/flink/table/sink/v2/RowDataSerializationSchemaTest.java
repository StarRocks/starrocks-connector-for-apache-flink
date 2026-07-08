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

package com.starrocks.connector.flink.table.sink.v2;

import com.starrocks.connector.flink.StarRocksSinkBaseTest;
import com.starrocks.connector.flink.row.sink.StarRocksGenericRowTransformer;
import com.starrocks.connector.flink.row.sink.StarRocksISerializer;
import com.starrocks.connector.flink.row.sink.StarRocksSerializerFactory;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RowDataSerializationSchemaTest extends StarRocksSinkBaseTest {

    static class UserPojo {
        public byte age = 21;
        public String resume = "hello";
        public String birthDate = "2023-01-01";
        public String birthDateTime = "2023-01-01 00:00:00";
        public BigDecimal savings = new BigDecimal("100.10");
        public short todaySteps = 100;
        public String name = "pojo-user";
    }

    @Test
    public void testSerializeNonRowDataRecord() {
        StarRocksGenericRowTransformer<UserPojo> transformer = new StarRocksGenericRowTransformer<>((slots, u) -> {
            slots[0] = u.age;
            slots[1] = u.resume;
            slots[2] = u.birthDate;
            slots[3] = u.birthDateTime;
            slots[4] = u.savings;
            slots[5] = u.todaySteps;
            slots[6] = u.name;
        });
        transformer.setTableSchema(TABLE_SCHEMA);
        StarRocksISerializer serializer = StarRocksSerializerFactory.createSerializer(
                OPTIONS, TABLE_SCHEMA.getColumnNames().toArray(new String[0]));

        RowDataSerializationSchema<UserPojo> schema = new RowDataSerializationSchema<>(
                OPTIONS.getDatabaseName(),
                OPTIONS.getTableName(),
                OPTIONS.supportUpsertDelete(),
                OPTIONS.getIgnoreUpdateBefore(),
                serializer,
                transformer);
        schema.open(null, null);

        StarRocksRowData row = schema.serialize(new UserPojo());

        assertNotNull(row);
        assertEquals(OPTIONS.getDatabaseName(), row.getDatabase());
        assertEquals(OPTIONS.getTableName(), row.getTable());
        assertTrue(row.getRow().contains("pojo-user"));
    }
}
