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

package com.starrocks.connector.flink.row.source;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.starrocks.connector.flink.it.source.StarRocksSourceBaseTest;
import com.starrocks.connector.flink.table.source.StarRocksSourceCommonFunc;
import com.starrocks.connector.flink.table.source.struct.Column;
import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;
import com.starrocks.connector.flink.table.source.struct.Const;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.table.source.struct.StarRocksSchema;
import com.starrocks.connector.flink.thrift.TScanBatchResult;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.junit.Before;
import org.junit.Test;

public class StarRocksSourceFlinkRowsTest extends StarRocksSourceBaseTest {
    
    protected StarRocksSchema srSchema = new StarRocksSchema();
    protected SelectColumn[] selectColumns;
    protected List<ColunmRichInfo> colunmRichInfos;
    protected String curPath = System.getProperty("user.dir");
    protected Map<String, ColunmRichInfo> columnMap;

    @Before
    public void initParams() {  
        srSchema.setStatus(0);
        ArrayList<Column> properties = new ArrayList<>();
        properties.add(new Column("date_1", "DATE", "", 0, 0));
        properties.add(new Column("datetime_1", "DATETIME", "", 0, 0));
        properties.add(new Column("char_1", "CHAR", "", 0, 0));
        properties.add(new Column("varchar_1", "VARCHAR", "", 0, 0));
        properties.add(new Column("boolean_1", "BOOLEAN", "", 0, 0));
        properties.add(new Column("tinyint_1", "TINYINT", "", 0, 0));
        properties.add(new Column("smallint_1", "SMALLINT", "", 0, 0));
        properties.add(new Column("int_1", "INT", "", 0, 0));
        properties.add(new Column("bigint_1", "BIGINT", "", 0, 0));
        properties.add(new Column("largeint_1", "LARGEINT", "", 0, 0));
        properties.add(new Column("float_1", "FLOAT", "", 0, 0));
        properties.add(new Column("double_1", "DOUBLE", "", 0, 0));
        properties.add(new Column("decimal_1", "DECIMAL128", "", 0, 0));
        srSchema.setProperties(properties);
        columnMap = StarRocksSourceCommonFunc.genColumnMap(TABLE_SCHEMA_NOT_NULL);
        colunmRichInfos = StarRocksSourceCommonFunc.genColunmRichInfo(columnMap);
        selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(columnMap, OPTIONS, colunmRichInfos);
    }

    @Test
    public void testGenFlinkRows() throws FileNotFoundException, IOException {
        String fileName = curPath + "/src/test/resources/rowsData";
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            line = br.readLine();
        }
        assertTrue(line != null);
        String dataStrArray[] = line.split(",");
        ArrayList<Byte> byteList = new ArrayList<>();
        for (int i = 0; i < dataStrArray.length; i ++) {
            byteList.add((byte)Integer.parseInt(dataStrArray[i].trim()));
        }
        byte[] byteArray = new byte[byteList.size()];
        for (int i = 0; i < byteArray.length; i ++) {
            byteArray[i] = byteList.get(i).byteValue();
        }
        TScanBatchResult nextResult = new TScanBatchResult();
        nextResult.setRows(byteArray);
        StarRocksSourceFlinkRows flinkRows = new StarRocksSourceFlinkRows(nextResult, colunmRichInfos, srSchema, selectColumns);
        flinkRows = flinkRows.genFlinkRowsFromArrow();
        int dataCount = 0;
        while (flinkRows.hasNext()) {
            dataCount ++;
            GenericRowData preparedData = flinkRows.next();
            for (int i = 0; i < preparedData.getArity(); i ++) {
                Object currentObj = preparedData.getField(i);
                if (i == 0) {
                    assertTrue(currentObj instanceof Integer); 
                    assertTrue((Integer)currentObj == 17978);
                }
                if (i == 1) {
                    assertTrue(currentObj instanceof TimestampData);
                    TimestampData actual = (TimestampData)currentObj;
                    TimestampData expect = TimestampData.fromEpochMillis(1584921600189L, 760000);
                    assertTrue(actual.equals(expect));
                }
                if (i == 2) {
                    assertTrue(currentObj instanceof StringData);
                    assertTrue(StringData.fromString("DEF").equals((StringData)currentObj));
                }
                if (i == 3) {
                    assertTrue(currentObj instanceof StringData);
                    assertTrue(StringData.fromString("A").equals((StringData)currentObj));
                }
                if (i == 4) {
                    assertTrue(currentObj instanceof Boolean);
                    assertTrue((Boolean)currentObj == true);
                }
                if (i == 5) {
                    assertTrue(currentObj instanceof Byte);
                    assertTrue((Byte)currentObj == 0);
                }
                if (i == 6) {
                    assertTrue(currentObj instanceof Short);
                    assertTrue((Short)currentObj == -32768);
                }
                if (i == 7) {
                    assertTrue(currentObj instanceof Integer);
                    assertTrue((Integer)currentObj == -2147483648);
                }
                if (i == 8) {
                    assertTrue(currentObj instanceof Long);
                    assertTrue((Long)currentObj == -9223372036854775808L);
                }
                if (i == 9) {
                    assertTrue(currentObj instanceof StringData);
                    assertTrue(StringData.fromString("-18446744073709551616").equals((StringData)currentObj));
                }
                if (i == 10) {
                    assertTrue(currentObj instanceof Float);
                    assertTrue((Float)currentObj == -3.1F);
                }
                if (i == 11) {
                    assertTrue(currentObj instanceof Double);
                    assertTrue((Double)currentObj == -3.14D);
                }
                if (i == 12) {
                    assertTrue(currentObj instanceof DecimalData);
                    DecimalData cur = (DecimalData)currentObj;
                    assertTrue(cur.toUnscaledLong() == -3141291000L);
                    assertTrue(cur.precision() == 10);
                    assertTrue(cur.scale() == 9);
                }
            }
        }
        assertTrue(flinkRows.getReadRowCount() == dataCount);
    }


    @Test
    public void testGenFlinkRowsWithNull() throws FileNotFoundException, IOException {
        String fileName = curPath + "/src/test/resources/rowsDataWithNull";
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))){
            line = br.readLine();
        }
        assertTrue(line != null);
        String dataStrArray[] = line.split(",");
        ArrayList<Byte> byteList = new ArrayList<>();
        for (int i = 0; i < dataStrArray.length; i ++) {
            byteList.add((byte)Integer.parseInt(dataStrArray[i].trim()));
        }
        byte[] byteArray = new byte[byteList.size()];
        for (int i = 0; i < byteArray.length; i ++) {
            byteArray[i] = byteList.get(i).byteValue();
        }
        TScanBatchResult nextResult = new TScanBatchResult();
        nextResult.setRows(byteArray);
        StarRocksSourceFlinkRows flinkRows = new StarRocksSourceFlinkRows(nextResult, colunmRichInfos, srSchema, selectColumns);
        String eMsg = null;
        try {
            flinkRows = flinkRows.genFlinkRowsFromArrow();
        } catch (Exception e){
            eMsg = e.getMessage();
        }
        assertTrue(eMsg.contains("Data could not be null. please check create table SQL, column index is"));
    }

    @Test
    public void testDataTypeTrans() {

        Const.DataTypeRelationMap.entrySet().stream().forEach(entry -> {
            if (entry.getKey().equals(LogicalTypeRoot.DATE)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_DATE)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkDate);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_DATETIME)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkTimestamp);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_DATETIME)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkTimestamp);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_DATETIME)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkTimestamp);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.CHAR)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_CHAR)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkChar);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.VARCHAR)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_VARCHAR)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkChar);
                    }
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_LARGEINT)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkChar);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.BOOLEAN)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_BOOLEAN)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkBoolean);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.TINYINT)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_TINYINT)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkTinyInt);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.SMALLINT)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_SMALLINT)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkSmallInt);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.INTEGER)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_INT)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkInt);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.BIGINT)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_BIGINT)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkBigInt);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.FLOAT)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_FLOAT)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkFloat);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.DOUBLE)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_DOUBLE)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkDouble);
                    }
                });
            }
            if (entry.getKey().equals(LogicalTypeRoot.DECIMAL)) {
                entry.getValue().entrySet().stream().forEach(type -> {
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_DECIMAL)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkDecimal);
                    }
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_DECIMALV2)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkDecimal);
                    }
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_DECIMAL32)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkDecimal);
                    }
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_DECIMAL64)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkDecimal);
                    }
                    if (type.getKey().equals(Const.DATA_TYPE_STARROCKS_DECIMAL128)) {
                        assertTrue(type.getValue() instanceof StarRocksToFlinkTranslators.ToFlinkDecimal);
                    }
                });
            }
        });
    }
}
