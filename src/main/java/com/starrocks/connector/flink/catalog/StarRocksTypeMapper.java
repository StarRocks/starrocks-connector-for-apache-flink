/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class StarRocksTypeMapper {

    // -------------------------number----------------------------
    private static final String STARROCKS_TINYINT = "TINYINT";
    private static final String STARROCKS_SMALLINT = "SMALLINT";
    private static final String STARROCKS_INT = "INT";
    private static final String STARROCKS_BIGINT = "BIGINT";
    private static final String STARROCKS_LARGEINT = "BIGINT UNSIGNED";
    private static final String STARROCKS_DECIMAL = "DECIMAL";
    private static final String STARROCKS_FLOAT = "FLOAT";
    private static final String STARROCKS_DOUBLE = "DOUBLE";

    // -------------------------string----------------------------
    private static final String STARROCKS_CHAR = "CHAR";
    private static final String STARROCKS_VARCHAR = "VARCHAR";
    private static final String STARROCKS_STRING = "STRING";
    private static final String STARROCKS_TEXT = "TEXT";

    // ------------------------------time-------------------------
    private static final String STARROCKS_DATE = "DATE";
    private static final String STARROCKS_DATETIME = "DATETIME";

    //------------------------------bool------------------------
    private static final String STARROCKS_BOOLEAN = "BOOLEAN";


    public static DataType toFlinkType(String columnName, String columnType, int precision, int scale) {
        columnType = columnType.toUpperCase();
        switch (columnType) {
            case STARROCKS_BOOLEAN:
                return DataTypes.BOOLEAN();
            case STARROCKS_TINYINT:
                if (precision == 0) {
                    //The boolean type will become tinyint when queried in information_schema, and precision=0
                    return DataTypes.BOOLEAN();
                } else {
                    return DataTypes.TINYINT();
                }
            case STARROCKS_SMALLINT:
                return DataTypes.SMALLINT();
            case STARROCKS_INT:
                return DataTypes.INT();
            case STARROCKS_BIGINT:
                return DataTypes.BIGINT();
            case STARROCKS_DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case STARROCKS_FLOAT:
                return DataTypes.FLOAT();
            case STARROCKS_DOUBLE:
                return DataTypes.DOUBLE();
            case STARROCKS_CHAR:
                return DataTypes.CHAR(precision);
            case STARROCKS_LARGEINT:
            case STARROCKS_VARCHAR:
            case STARROCKS_STRING:
            case STARROCKS_TEXT:
                return DataTypes.STRING();
            case STARROCKS_DATE:
                return DataTypes.DATE();
            case STARROCKS_DATETIME:
                return DataTypes.TIMESTAMP(0);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support StarRocks type '%s' on column '%s'", columnType, columnName));
        }
    }
}
