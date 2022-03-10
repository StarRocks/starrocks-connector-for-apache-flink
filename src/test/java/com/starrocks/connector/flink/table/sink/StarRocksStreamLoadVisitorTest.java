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

package com.starrocks.connector.flink.table.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.starrocks.connector.flink.StarRocksSinkBaseTest;
import com.starrocks.connector.flink.manager.StarRocksSinkBufferEntity;
import com.starrocks.connector.flink.manager.StarRocksStreamLoadVisitor;

public class StarRocksStreamLoadVisitorTest extends StarRocksSinkBaseTest {

    @Test
    public void testNoAvailableHost() throws IOException {
        stopHttpServer();
        StarRocksStreamLoadVisitor visitor = new StarRocksStreamLoadVisitor(OPTIONS, TABLE_SCHEMA.getFieldNames(), true);
        // test failed
        String exMsg = "";
        try {
            mockFailedResponse();
            StarRocksSinkBufferEntity entity = new StarRocksSinkBufferEntity(OPTIONS.getDatabaseName(), OPTIONS.getTableName(), OPTIONS.getLabelPrefix());
            entity.addToBuffer("aaaa".getBytes());
            visitor.doStreamLoad(entity);
        } catch (Exception e) {
            exMsg = e.getLocalizedMessage();
        }
        assertEquals(0, exMsg.indexOf("None of the host"));
    }

    @Test
    public void testDoStreamLoad() throws IOException {
        StarRocksStreamLoadVisitor visitor = new StarRocksStreamLoadVisitor(OPTIONS, TABLE_SCHEMA.getFieldNames(), true);
        // test failed
        String exMsg = "";
        try {
            mockFailedResponse();
            StarRocksSinkBufferEntity entity = new StarRocksSinkBufferEntity(OPTIONS.getDatabaseName(), OPTIONS.getTableName(), OPTIONS.getLabelPrefix());
            entity.addToBuffer("aaaa".getBytes());
            visitor.doStreamLoad(entity);
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(0 < exMsg.length());
        // test suucess
        exMsg = "";
        try {
            mockSuccessResponse();
            StarRocksSinkBufferEntity entity = new StarRocksSinkBufferEntity(OPTIONS.getDatabaseName(), OPTIONS.getTableName(), OPTIONS.getLabelPrefix());
            entity.addToBuffer("aaaa".getBytes());
            visitor.doStreamLoad(entity);
        } catch (Exception e) {
            exMsg = e.getLocalizedMessage();
        }
        assertEquals(0, exMsg.length());
        // test label already exists
        exMsg = "";
        try {
            mockLabelExistsResponse(new String[]{"PREPARE", "ABORTED"});
            StarRocksSinkBufferEntity entity = new StarRocksSinkBufferEntity(OPTIONS.getDatabaseName(), OPTIONS.getTableName(), OPTIONS.getLabelPrefix());
            entity.addToBuffer("aaaa".getBytes());
            visitor.doStreamLoad(entity);
        } catch (Exception e) {
            exMsg = e.getLocalizedMessage();
        }
        assertTrue(0 < exMsg.length());
        exMsg = "";
        try {
            mockLabelExistsResponse(new String[]{"UNKONW"});
            StarRocksSinkBufferEntity entity = new StarRocksSinkBufferEntity(OPTIONS.getDatabaseName(), OPTIONS.getTableName(), OPTIONS.getLabelPrefix());
            entity.addToBuffer("aaaa".getBytes());
            visitor.doStreamLoad(entity);
        } catch (Exception e) {
            exMsg = e.getLocalizedMessage();
        }
        assertTrue(0 < exMsg.length());
        exMsg = "";
        try {
            mockLabelExistsResponse(new String[]{"PREPARE", "VISIBLE"});
            StarRocksSinkBufferEntity entity = new StarRocksSinkBufferEntity(OPTIONS.getDatabaseName(), OPTIONS.getTableName(), OPTIONS.getLabelPrefix());
            entity.addToBuffer("aaaa".getBytes());
            visitor.doStreamLoad(entity);
        } catch (Exception e) {
            exMsg = e.getLocalizedMessage();
        }
        assertEquals(0, exMsg.length());
        exMsg = "";
        try {
            mockLabelExistsResponse(new String[]{"PREPARE", "COMMITTED"});
            StarRocksSinkBufferEntity entity = new StarRocksSinkBufferEntity(OPTIONS.getDatabaseName(), OPTIONS.getTableName(), OPTIONS.getLabelPrefix());
            entity.addToBuffer("aaaa".getBytes());
            visitor.doStreamLoad(entity);
        } catch (Exception e) {
            exMsg = e.getLocalizedMessage();
        }
        assertEquals(0, exMsg.length());
    }
        
    @Test
    public void testMemoryUsage()  throws Exception {
        // Runtime rt = Runtime.getRuntime();
        // List<String> rows = new ArrayList<>();
        // for (int i = 0; i < 500000; i++) {
        //     rows.add("10000000000000000000100000000000000000001000000000000000000010000000000000000000100000000000000000001000000000000000000010000000000000000000100000000000000000001000000000000000000010000000000000000000");
        // }
        // long memBefore = rt.totalMemory();
        // byte[] c = joinRows(rows);
        // long memAfter = rt.totalMemory();
        // assertEquals(memBefore, memAfter);
    }
}
