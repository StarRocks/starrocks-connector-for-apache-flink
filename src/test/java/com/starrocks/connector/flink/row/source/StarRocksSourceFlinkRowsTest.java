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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.starrocks.connector.flink.it.source.StarRocksSourceBaseTest;
import com.starrocks.connector.flink.table.source.StarRocksSourceCommonFunc;
import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;
import com.starrocks.connector.flink.table.source.struct.Const;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.table.source.struct.StarRocksSchema;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanColumnDesc;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StarRocksSourceFlinkRowsTest extends StarRocksSourceBaseTest {
    
    protected StarRocksSchema srSchema = new StarRocksSchema();
    protected StarRocksSchema srWrongOrderSchema = new StarRocksSchema();
    protected StarRocksSchema srLessSchema = new StarRocksSchema();
    protected SelectColumn[] selectColumns;
    protected List<ColunmRichInfo> colunmRichInfos;
    protected String curPath = System.getProperty("user.dir");
    protected Map<String, ColunmRichInfo> columnMap;

    @Before
    public void initParams() {  
        
        srSchema = StarRocksSchema.genSchema(this.rightOrderList());
        srWrongOrderSchema = StarRocksSchema.genSchema(this.wrongOrderList());
        srLessSchema = StarRocksSchema.genSchema(this.lessList());
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
        // generate flinkRows1 with right srSchema
        StarRocksSourceFlinkRows flinkRows1 = new StarRocksSourceFlinkRows(nextResult, colunmRichInfos, srSchema, selectColumns);
        flinkRows1 = flinkRows1.genFlinkRowsFromArrow();
        checkFlinkRows(flinkRows1);
        // generate flinkRows2 with wrong srSchema
        StarRocksSourceFlinkRows flinkRows2 = new StarRocksSourceFlinkRows(nextResult, colunmRichInfos, srWrongOrderSchema, selectColumns);
        flinkRows2 = flinkRows2.genFlinkRowsFromArrow();
        checkFlinkRows(flinkRows2);

        // generate flinkRows3 with less column srSchema
        try {
            StarRocksSourceFlinkRows flinkRows3 = new StarRocksSourceFlinkRows(nextResult, colunmRichInfos, srLessSchema, selectColumns);
            flinkRows3 = flinkRows3.genFlinkRowsFromArrow();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Can not find StarRocks column info from "));
        }
        
    }

    public void checkFlinkRows(StarRocksSourceFlinkRows flinkRows) {
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
    public void testParallel() {
        final AtomicInteger index = new AtomicInteger(0);
        List<Integer> testList = new ArrayList<>();
        for (int i = 0; i < 10; i ++) {
            testList.add(i);
        }
        testList.stream().map(column -> new Object[]{column, index.getAndAdd(1)})
        .collect(Collectors.toList())
        .parallelStream().forEach(columnAndIndex -> {
            Integer item = (Integer) columnAndIndex[0];
            int colIndex = (int) columnAndIndex[1];
            Assert.assertTrue(item == colIndex);
        });
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

    public List<TScanColumnDesc> rightOrderList() {
        List<TScanColumnDesc> list = new ArrayList<>();
        // date_1
        TScanColumnDesc c0 = new TScanColumnDesc();
        c0.name = "date_1";
        c0.type = TPrimitiveType.DATE;
        list.add(c0);
        // datetime_1
        TScanColumnDesc c1 = new TScanColumnDesc();
        c1.name = "datetime_1";
        c1.type = TPrimitiveType.DATETIME;
        list.add(c1);
        // "char_1"
        TScanColumnDesc c2 = new TScanColumnDesc();
        c2.name = "char_1";
        c2.type = TPrimitiveType.CHAR;
        list.add(c2);
        // "varchar_1"
        TScanColumnDesc c3 = new TScanColumnDesc();
        c3.name = "varchar_1";
        c3.type = TPrimitiveType.VARCHAR;
        list.add(c3);
        // "boolean_1"
        TScanColumnDesc c4 = new TScanColumnDesc();
        c4.name = "boolean_1";
        c4.type = TPrimitiveType.BOOLEAN;
        list.add(c4);
        // "tinyint_1""
        TScanColumnDesc c5 = new TScanColumnDesc();
        c5.name = "tinyint_1";
        c5.type = TPrimitiveType.TINYINT;
        list.add(c5);
        // "smallint_1"
        TScanColumnDesc c6 = new TScanColumnDesc();
        c6.name = "smallint_1";
        c6.type = TPrimitiveType.SMALLINT;
        list.add(c6);
        // "int_1"
        TScanColumnDesc c7 = new TScanColumnDesc();
        c7.name = "int_1";
        c7.type = TPrimitiveType.INT;
        list.add(c7);
        // "bigint_1"
        TScanColumnDesc c8 = new TScanColumnDesc();
        c8.name = "bigint_1";
        c8.type = TPrimitiveType.BIGINT;
        list.add(c8);
        // "largeint_1"
        TScanColumnDesc c9 = new TScanColumnDesc();
        c9.name = "largeint_1";
        c9.type = TPrimitiveType.LARGEINT;
        list.add(c9);
        // "float_1"
        TScanColumnDesc c10 = new TScanColumnDesc();
        c10.name = "float_1";
        c10.type = TPrimitiveType.FLOAT;
        list.add(c10);
        // "double_1"
        TScanColumnDesc c11 = new TScanColumnDesc();
        c11.name = "double_1";
        c11.type = TPrimitiveType.DOUBLE;
        list.add(c11);
        // "decimal_1"
        TScanColumnDesc c12 = new TScanColumnDesc();
        c12.name = "decimal_1";
        c12.type = TPrimitiveType.DECIMALV2;
        list.add(c12);
        return list;
    }

    public List<TScanColumnDesc> wrongOrderList() {
        List<TScanColumnDesc> list = new ArrayList<>();
        // date_1
        TScanColumnDesc c0 = new TScanColumnDesc();
        c0.name = "date_1";
        c0.type = TPrimitiveType.DATE;
        list.add(c0);
        // "char_1"
        TScanColumnDesc c2 = new TScanColumnDesc();
        c2.name = "char_1";
        c2.type = TPrimitiveType.CHAR;
        list.add(c2);
        // "varchar_1"
        TScanColumnDesc c3 = new TScanColumnDesc();
        c3.name = "varchar_1";
        c3.type = TPrimitiveType.VARCHAR;
        list.add(c3);
        // "boolean_1"
        TScanColumnDesc c4 = new TScanColumnDesc();
        c4.name = "boolean_1";
        c4.type = TPrimitiveType.BOOLEAN;
        list.add(c4);
        // "tinyint_1""
        TScanColumnDesc c5 = new TScanColumnDesc();
        c5.name = "tinyint_1";
        c5.type = TPrimitiveType.TINYINT;
        list.add(c5);
        // "smallint_1"
        TScanColumnDesc c6 = new TScanColumnDesc();
        c6.name = "smallint_1";
        c6.type = TPrimitiveType.SMALLINT;
        list.add(c6);
        // "int_1"
        TScanColumnDesc c7 = new TScanColumnDesc();
        c7.name = "int_1";
        c7.type = TPrimitiveType.INT;
        list.add(c7);
        // "bigint_1"
        TScanColumnDesc c8 = new TScanColumnDesc();
        c8.name = "bigint_1";
        c8.type = TPrimitiveType.BIGINT;
        list.add(c8);
        // "largeint_1"
        TScanColumnDesc c9 = new TScanColumnDesc();
        c9.name = "largeint_1";
        c9.type = TPrimitiveType.LARGEINT;
        list.add(c9);
        // "float_1"
        TScanColumnDesc c10 = new TScanColumnDesc();
        c10.name = "float_1";
        c10.type = TPrimitiveType.FLOAT;
        list.add(c10);
        // "double_1"
        TScanColumnDesc c11 = new TScanColumnDesc();
        c11.name = "double_1";
        c11.type = TPrimitiveType.DOUBLE;
        list.add(c11);
        // "decimal_1"
        TScanColumnDesc c12 = new TScanColumnDesc();
        c12.name = "decimal_1";
        c12.type = TPrimitiveType.DECIMALV2;
        list.add(c12);
        // datetime_1
        TScanColumnDesc c1 = new TScanColumnDesc();
        c1.name = "datetime_1";
        c1.type = TPrimitiveType.DATETIME;
        list.add(c1);
        return list;
    }

    public List<TScanColumnDesc> lessList() {
        List<TScanColumnDesc> list = new ArrayList<>();
        // date_1
        TScanColumnDesc c0 = new TScanColumnDesc();
        c0.name = "date_1";
        c0.type = TPrimitiveType.DATE;
        list.add(c0);
        return list;
    }
}
