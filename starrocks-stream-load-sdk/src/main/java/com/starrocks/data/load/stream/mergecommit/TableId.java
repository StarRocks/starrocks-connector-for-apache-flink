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

package com.starrocks.data.load.stream.mergecommit;

import java.util.Objects;

public class TableId {
    public String db;
    public String table;

    public static TableId of(String db, String table) {
        TableId tableId = new TableId();
        tableId.db = db;
        tableId.table = table;
        return tableId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableId tableId = (TableId) o;
        return Objects.equals(db, tableId.db) && Objects.equals(table, tableId.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(db, table);
    }

    @Override
    public String toString() {
        return "TableId{" +
                "db='" + db + '\'' +
                ", table='" + table + '\'' +
                '}';
    }
}
