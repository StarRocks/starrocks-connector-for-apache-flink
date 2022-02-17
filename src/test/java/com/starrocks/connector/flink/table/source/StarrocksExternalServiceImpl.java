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

package com.starrocks.connector.flink.table.source;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.starrocks.connector.flink.thrift.TPrimitiveType;
import com.starrocks.connector.flink.thrift.TScanBatchResult;
import com.starrocks.connector.flink.thrift.TScanCloseParams;
import com.starrocks.connector.flink.thrift.TScanCloseResult;
import com.starrocks.connector.flink.thrift.TScanColumnDesc;
import com.starrocks.connector.flink.thrift.TScanNextBatchParams;
import com.starrocks.connector.flink.thrift.TScanOpenParams;
import com.starrocks.connector.flink.thrift.TScanOpenResult;
import com.starrocks.connector.flink.thrift.TStarrocksExternalService;
import com.starrocks.connector.flink.thrift.TStatus;
import com.starrocks.connector.flink.thrift.TStatusCode;

import org.apache.thrift.TException;

public class StarrocksExternalServiceImpl implements TStarrocksExternalService.Iface {

    @Override
    public TScanOpenResult open_scanner(TScanOpenParams params) throws TException {
        TScanOpenResult result = new TScanOpenResult();
        TStatus status = new TStatus();
        status.status_code = TStatusCode.OK;
        result.setStatus(status);
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
        result.setSelected_columns(list);
        result.setContext_id("id_pkocj1d813xasu9dpjjiueqnc");
        return result;
    }

    @Override
    public TScanBatchResult get_next(TScanNextBatchParams params) throws TException {
        TScanBatchResult result = new TScanBatchResult();
        TStatus status = new TStatus();
        status.status_code = TStatusCode.OK;
        result.setStatus(status);
        if (params.offset == 1) {
            result.setEos(true);
            return result;
        }
        String fileName = System.getProperty("user.dir") + "/src/test/resources/rowsData";
        String line = null;
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            line = br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
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
        result.setRows(byteArray);
        result.setEos(false);
        return result;
    }

    @Override
    public TScanCloseResult close_scanner(TScanCloseParams params) throws TException {
        TScanCloseResult result = new TScanCloseResult();
        TStatus status = new TStatus();
        status.status_code = TStatusCode.OK;
        result.setStatus(status);
        return result;
    }

}