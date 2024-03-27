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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import com.starrocks.connector.flink.it.source.StarRocksSourceBaseTest;
import com.starrocks.connector.flink.table.source.StarRocksSourceCommonFunc;
import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.thrift.TScanColumnDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StarRocksSourceFlinkRowsTest extends StarRocksSourceBaseTest {

    protected SelectColumn[] selectColumns;
    protected List<ColumnRichInfo> columnRichInfos;
    protected String curPath = System.getProperty("user.dir");
    protected Map<String, ColumnRichInfo> columnMap;

    @Before
    public void initParams() {
        columnMap = StarRocksSourceCommonFunc.genColumnMap(TABLE_SCHEMA_NOT_NULL);
        columnRichInfos = StarRocksSourceCommonFunc.genColumnRichInfo(columnMap);
        selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(columnMap, OPTIONS, columnRichInfos);
    }

    @Test
    public void testGenFlinkRows() throws IOException {
        String fileName = curPath + "/src/test/resources/data/source/rowsData";
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            line = br.readLine();
        }
        assertNotNull(line);
        String[] dataStrArray = line.split(",");
        ArrayList<Byte> byteList = new ArrayList<>();
        for (String s : dataStrArray) {
            byteList.add((byte) Integer.parseInt(s.trim()));
        }
        byte[] byteArray = new byte[byteList.size()];
        for (int i = 0; i < byteArray.length; i ++) {
            byteArray[i] = byteList.get(i);
        }
        TScanBatchResult nextResult = new TScanBatchResult();
        nextResult.setRows(byteArray);
        StarRocksSourceFlinkRows flinkRows1 = new StarRocksSourceFlinkRows(nextResult, columnRichInfos, selectColumns);
        List<ArrowFieldConverter> fieldConverters = new ArrayList<>();
        flinkRows1.init(fieldConverters);
        checkFlinkRows(flinkRows1);
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
                    assertTrue(cur.precision() == 27);
                    assertTrue(cur.scale() == 9);
                }
            }
        }
        assertTrue(flinkRows.getReadRowCount() == dataCount);
    }

    @Test
    public void testGenFlinkRowsWithNull() throws IOException {
        String fileName = curPath + "/src/test/resources/data/source/rowsDataWithNull";
        String line;
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))){
            line = br.readLine();
        }
        assertNotNull(line);
        String[] dataStrArray = line.split(",");
        ArrayList<Byte> byteList = new ArrayList<>();
        for (String s : dataStrArray) {
            byteList.add((byte) Integer.parseInt(s.trim()));
        }
        byte[] byteArray = new byte[byteList.size()];
        for (int i = 0; i < byteArray.length; i ++) {
            byteArray[i] = byteList.get(i);
        }
        TScanBatchResult nextResult = new TScanBatchResult();
        nextResult.setRows(byteArray);
        StarRocksSourceFlinkRows flinkRows = new StarRocksSourceFlinkRows(nextResult, columnRichInfos, selectColumns);
        String eMsg = null;
        try {
            List<ArrowFieldConverter> fieldConverters = new ArrayList<>();
            flinkRows.init(fieldConverters);
        } catch (Exception e){
            eMsg = e.getCause().getMessage();
        }
        assertTrue(eMsg.contains("The value is null for a non-nullable column"));
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
