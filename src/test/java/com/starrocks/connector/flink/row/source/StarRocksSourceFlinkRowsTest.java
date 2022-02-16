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

import com.starrocks.connector.flink.it.source.StarRocksSourceBaseTest;
import com.starrocks.connector.flink.table.source.StarRocksSourceCommonFunc;
import com.starrocks.connector.flink.table.source.struct.Column;
import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.table.source.struct.StarRocksSchema;
import com.starrocks.connector.flink.thrift.TScanBatchResult;

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
            flinkRows.next();
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
}
