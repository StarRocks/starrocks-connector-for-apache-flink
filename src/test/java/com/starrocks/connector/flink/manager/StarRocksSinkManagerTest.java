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

package com.starrocks.connector.flink.manager;

import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import java.util.ArrayList;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema.Builder;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.starrocks.connector.flink.StarRocksSinkBaseTest;

import mockit.Expectations;

public class StarRocksSinkManagerTest extends StarRocksSinkBaseTest {

    @Test
    public void testValidateTableStructure() {
        mockTableStructure();
        OPTIONS.getSinkStreamLoadProperties().remove("columns");
        assertTrue(!OPTIONS.hasColumnMappingProperty());
        // test succeeded
        try {
            new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
        } catch (Exception e) {
            throw e;
        }
        // test failed
        new Expectations(){
            {
                v.getTableColumnsMetaData();
                result = Lists.newArrayList();
            }
        };
        String exMsg = "";
        try {
            new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(exMsg.length() > 0);
        // test failed
        new Expectations(){
            {
                v.getTableColumnsMetaData();
                result = STARROCKS_TABLE_META.keySet().stream().map(k -> new HashMap<String, Object>(){{
                    put("COLUMN_NAME", k);
                    put("COLUMN_KEY", "");
                    put("DATA_TYPE", "varchar");
                }}).collect(Collectors.toList());;
            }
        };
        exMsg = "";
        try {
            new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(exMsg.length() > 0);


        Builder schemaBuilder = createTableSchemaBuilder();
        schemaBuilder.field("v6", DataTypes.VARCHAR(20));
        try {
            new StarRocksSinkManager(OPTIONS, schemaBuilder.build());
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals("Fields count of test_tbl mismatch. \n"
                + "flinkSchema[8]:k1,k2,v1,v2,v3,v4,v5,v6\n"
                + " realTab[7]:k1,k2,v1,v2,v3,v4,v5",exMsg);
    }

    @Test
    public void testWriteMaxBatch() throws IOException {
        mockTableStructure();
        long maxRows = OPTIONS.getSinkMaxRows();
        stopHttpServer();
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
            for (int i = 0; i < maxRows - 1; i++) {
                mgr.writeRecord("test record");
            }
        } catch (Exception e) {
            throw e;
        }
        String exMsg = "";
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            for (int i = 0; i < maxRows * 3; i++) {
                mgr.writeRecord("test record"+i);
            }
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(0 < exMsg.length());
    }

    @Test
    public void testWriteMaxBytes() throws IOException {
        mockTableStructure();
        long maxSize = OPTIONS.getSinkMaxBytes();
        stopHttpServer();
        int rowLength = 100000;
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
            for (int i = 0; i < maxSize / rowLength - 1; i++) {
                mgr.writeRecord(new String(new char[rowLength]));
            }
        } catch (Exception e) {
            throw e;
        }
        String exMsg = "";
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            for (int i = 0; i < maxSize / rowLength + 1; i++) {
                mgr.writeRecord(new String(new char[rowLength]));
            }
            mgr.writeRecord(new String(new char[rowLength]));
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(0 < exMsg.length());
    }

    @Test
    public void testWriteMaxRetries() throws IOException {
        mockTableStructure();
        int maxRetries = OPTIONS.getSinkMaxRetries();
        if (maxRetries <= 0) return;
        stopHttpServer();
        mockSuccessResponse();
        String exMsg = "";
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            for (int i = 0; i < OPTIONS.getSinkMaxRows(); i++) {
                mgr.writeRecord("");
            }
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(0 < exMsg.length());

        // Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("test")).schedule(() -> {
        //     try {
        //         createHttpServer();
        //     } catch (Exception e) {}
        // }, maxRetries * 1000 - 500, TimeUnit.MILLISECONDS);
        // try {
        //     StarRocksSinkManager mgr = new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
        //     for (int i = 0; i < OPTIONS.getSinkMaxRows(); i++) {
        //         mgr.writeRecord("");
        //     }
        // } catch (Exception e) {
        //     throw e;
        // }
    }

    @Test
    public void testFlush() throws Exception {
        mockTableStructure();
        mockSuccessResponse();
        String exMsg = "";
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            mgr.writeRecord("");
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
            throw e;
        }
        assertEquals(0, exMsg.length());
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            mgr.writeRecord("");
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
            throw e;
        }
        assertEquals(0, exMsg.length());
        stopHttpServer();
        try {
            StarRocksSinkManager mgr = new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
            mgr.startAsyncFlushing();
            mgr.writeRecord("");
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertTrue(0 < exMsg.length());
    }

    @Test
    public void testClose() throws InterruptedException {
        mockTableStructure();
        mockSuccessResponse();
        String exMsg = "";

        // test close
        StarRocksSinkManager mgr = new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA);
        try {
            mgr.startAsyncFlushing();
            mgr.writeRecord("");
            mgr.flush(mgr.createBatchLabel(), true);
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals(0, exMsg.length());
        assertTrue(mgr.closed);
        TimeUnit.MILLISECONDS.sleep(100L); // wait flush thread exit
        assertFalse(mgr.flushThreadAlive);
    }

    @Test
    public void testOffer() throws InterruptedException {
        mockTableStructure();
        mockSuccessResponse();
        String exMsg = "";
        long offerTimeoutMs = 500L;

        // flush cost more than offer timeout
        StarRocksSinkManager mgr = mockStarRocksSinkManager(offerTimeoutMs, offerTimeoutMs + 100L);
        try {
            mgr.startAsyncFlushing();
            mgr.writeRecord("");
            mgr.flush(mgr.createBatchLabel(), true);
            mgr.writeRecord("");
            mgr.flush(mgr.createBatchLabel(), true);
        } catch (Exception e) {
            exMsg = e.getMessage();
            e.printStackTrace();
        }
        assertTrue(0 < exMsg.length());
        assertTrue(exMsg.startsWith("Timeout while offering data to flushQueue"));

        exMsg = "";
        // flush cost less than offer timeout
        mgr = mockStarRocksSinkManager(offerTimeoutMs, offerTimeoutMs - 100L);
        try {
            mgr.startAsyncFlushing();
            mgr.writeRecord("");
            mgr.flush(mgr.createBatchLabel(), true);
            mgr.writeRecord("");
            mgr.flush(mgr.createBatchLabel(), true);
            mgr.close();
        } catch (Exception e) {
            exMsg = e.getMessage();
        }
        assertEquals(0, exMsg.length());
        assertTrue(mgr.closed);
        TimeUnit.MILLISECONDS.sleep(100L); // wait flush thread exit
        assertFalse(mgr.flushThreadAlive);
    }

    private StarRocksSinkManager mockStarRocksSinkManager(final long offerTimeoutMs, final long mockFlushCostMs) {
        return new StarRocksSinkManager(OPTIONS, TABLE_SCHEMA) {

            @Override
            public boolean asyncFlush() throws Exception {
                Tuple3<String, Long, ArrayList<byte[]>> flushData = flushQueue.poll(10L, TimeUnit.MILLISECONDS);
                if (flushData == null || Strings.isNullOrEmpty(flushData.f0)) {
                    return true;
                }
                if (EOF.equals(flushData.f0)) {
                    return false;
                }
                TimeUnit.MILLISECONDS.sleep(mockFlushCostMs);
                return true;
            }

            @Override
            void offer(Tuple3<String, Long, ArrayList<byte[]>> tuple3) throws InterruptedException {
                if (!flushThreadAlive) {
                    return;
                }

                if (!flushQueue.offer(tuple3, offerTimeoutMs, TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException(
                        "Timeout while offering data to flushQueue, exceed " + offerTimeoutMs + " ms, see " +
                            StarRocksSinkOptions.SINK_BATCH_OFFER_TIMEOUT.key());
                }
            }
        };
    }
}
