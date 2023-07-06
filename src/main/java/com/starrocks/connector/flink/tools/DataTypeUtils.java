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

package com.starrocks.connector.flink.tools;

import org.apache.arrow.vector.types.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class DataTypeUtils {

    public static DataType map(Types.MinorType minorType) {
        switch (minorType) {
            case INT:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT4:
            case FLOAT8:
                return DataTypes.FLOAT();
            case NULL:
                return DataTypes.NULL();
            case TINYINT:
                return DataTypes.TINYINT();
            case VARCHAR:
                return DataTypes.STRING();
            case SMALLINT:
                return DataTypes.SMALLINT();
            case DECIMAL:
                return DataTypes.DECIMAL(10,0);
            case DECIMAL256:
                return DataTypes.DECIMAL(38, 38);
            case DATEDAY:
                return DataTypes.DATE();
            case DATEMILLI:
                return DataTypes.TIME();
            case TIMEMILLI:
                return DataTypes.TIMESTAMP();
            default:
                return null;
        }
    }
}
