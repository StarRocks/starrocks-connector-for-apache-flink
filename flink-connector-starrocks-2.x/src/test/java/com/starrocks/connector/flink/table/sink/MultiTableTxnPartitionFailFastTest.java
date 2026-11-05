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

package com.starrocks.connector.flink.table.sink;

import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.sink.v2.StarRocksWriter;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Verifies the fail-fast path in {@link StarRocksWriter} when multi-table
 * transaction mode is enabled but a
 * {@link DefaultStarRocksRowData} with a negative {@code sourcePartition} (the
 * default) is emitted.
 *
 * <p>Without the fail-fast, such a row would silently route to the legacy
 * {@code write(uniqueKey, ...)} path on the SDK side; that path does not
 * participate in partition-scoped {@code setCommitAllowed} tracking and, in
 * multi-table mode, would eventually stall the task thread on
 * {@code blockIfCacheFull} because the region's activeChunk can never be
 * switched (see {@code TransactionTableRegion.write0}). The fail-fast throws
 * {@link IllegalStateException} at the entry point instead.
 *
 * <p>The writer constructor is heavy (it creates a
 * {@code StreamLoadManagerV2} which requires a real JDBC connection to look
 * up the StarRocks version during construction). Because the fail-fast check
 * happens BEFORE any sinkManager interaction, we side-step construction via
 * {@link Unsafe#allocateInstance(Class)} and only populate the single field
 * the check reads ({@code sinkOptions}). The other fields are left at their
 * Java-default values, which is fine because the test exercises only the
 * partition &lt; 0 branch of {@code write} where the row is a direct
 * {@code StarRocksRowData} — it never accesses {@code sinkManager}
 * on the fail-fast path.
 */
public class MultiTableTxnPartitionFailFastTest {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Builds a minimal, validation-passing {@link StarRocksSinkOptions} with
     * multi-table transaction mode enabled.
     */
    private static StarRocksSinkOptions buildMultiTableEnabledSinkOptions() {
        Configuration conf = new Configuration();
        conf.set(StarRocksSinkOptions.TABLE_NAME, "test_table");
        conf.set(StarRocksSinkOptions.DATABASE_NAME, "test_db");
        conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:8030");
        conf.set(StarRocksSinkOptions.JDBC_URL, "jdbc:mysql://127.0.0.1:9030");
        conf.set(StarRocksSinkOptions.USERNAME, "root");
        conf.set(StarRocksSinkOptions.PASSWORD, "");
        conf.set(StarRocksSinkOptions.SINK_MULTI_TABLE_TXN_ENABLED, true);
        // Validation requires at-least-once semantic with multi-table mode,
        // which is the default, but set it explicitly for clarity.
        conf.set(StarRocksSinkOptions.SINK_SEMANTIC,
                StarRocksSinkSemantic.AT_LEAST_ONCE.getName());
        return new StarRocksSinkOptions(conf, conf.toMap());
    }

    /**
     * Builds a non-multi-table-enabled variant for the negative-case test
     * (partition &lt; 0 should be silently accepted in normal mode).
     */
    private static StarRocksSinkOptions buildNonMultiTableSinkOptions() {
        Configuration conf = new Configuration();
        conf.set(StarRocksSinkOptions.TABLE_NAME, "test_table");
        conf.set(StarRocksSinkOptions.DATABASE_NAME, "test_db");
        conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:8030");
        conf.set(StarRocksSinkOptions.JDBC_URL, "jdbc:mysql://127.0.0.1:9030");
        conf.set(StarRocksSinkOptions.USERNAME, "root");
        conf.set(StarRocksSinkOptions.PASSWORD, "");
        // Multi-table NOT enabled.
        return new StarRocksSinkOptions(conf, conf.toMap());
    }

    @SuppressWarnings("unchecked")
    private static <T> T allocateWithoutConstructor(Class<T> cls) throws Exception {
        return (T) UNSAFE.allocateInstance(cls);
    }

    private static void setField(Object target, Class<?> declaringClass, String name, Object value)
            throws Exception {
        Field f = declaringClass.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static DefaultStarRocksRowData rowWithPartition(int partition) {
        DefaultStarRocksRowData row = new DefaultStarRocksRowData("uk", "test_db", "test_table",
                "{\"order_id\":1}");
        row.setSourcePartition(partition);
        return row;
    }

    /**
     * Builds a control-only txnEnd marker with {@code row==null} and the given
     * partition. This models the signal a Kafka transaction assembler emits at
     * the end of a source transaction when there is no data row to attach the
     * txnEnd flag to.
     */
    private static DefaultStarRocksRowData txnEndControlRowWithPartition(int partition) {
        DefaultStarRocksRowData row = new DefaultStarRocksRowData("test_db", "test_table");
        row.setTransactionEnd(true);
        row.setSourcePartition(partition);
        return row;
    }

    // -------------------------------------------------------------------------
    // Sink v2 (StarRocksWriter) tests
    // -------------------------------------------------------------------------

    @Test
    public void testSinkV2ThrowsWhenMultiTableEnabledAndPartitionNegative() throws Exception {
        StarRocksSinkOptions sinkOptions = buildMultiTableEnabledSinkOptions();
        assertTrue("Test setup: multi-table should be enabled",
                sinkOptions.isMultiTableTransactionEnabled());

        // Allocate a StarRocksWriter without running its constructor, and
        // inject sinkOptions via reflection. The write() fail-fast check
        // consults sinkOptions before touching sinkManager, so the other
        // fields being null is fine for this code path.
        StarRocksWriter<Object> writer = allocateWithoutConstructor(StarRocksWriter.class);
        setField(writer, StarRocksWriter.class, "sinkOptions", sinkOptions);
        // StarRocksWriter.write calls serializationSchema.serialize(element)
        // first. We bypass that by injecting a StarRocksRowData as the
        // "serialized" result via a minimal schema subclass.
        setField(writer, StarRocksWriter.class, "serializationSchema",
                new TestRecordSerializationSchema(rowWithPartition(-1)));

        IllegalStateException caught = null;
        try {
            writer.write(new Object(), null);
            fail("Expected IllegalStateException for partition<0 in multi-table mode");
        } catch (IllegalStateException e) {
            caught = e;
        }

        assertNotNull(caught);
        assertTrue("Error should mention multi-table transaction: " + caught.getMessage(),
                caught.getMessage().contains("Multi-table transaction mode"));
        assertTrue("Error should mention partition: " + caught.getMessage(),
                caught.getMessage().contains("partition=-1"));
    }

    @Test
    public void testSinkV2ThrowsOnControlRowWhenMultiTableEnabledAndPartitionNegative() throws Exception {
        // Same rationale as the sink-v1 control-row test above: partition<0
        // on a control-only txnEnd row must trip the fail-fast in multi-table
        // mode, not slip past into setCommitAllowed(-1, true).
        StarRocksSinkOptions sinkOptions = buildMultiTableEnabledSinkOptions();
        assertTrue("Test setup: multi-table should be enabled",
                sinkOptions.isMultiTableTransactionEnabled());

        StarRocksWriter<Object> writer = allocateWithoutConstructor(StarRocksWriter.class);
        setField(writer, StarRocksWriter.class, "sinkOptions", sinkOptions);
        setField(writer, StarRocksWriter.class, "serializationSchema",
                new TestRecordSerializationSchema(txnEndControlRowWithPartition(-1)));

        IllegalStateException caught = null;
        try {
            writer.write(new Object(), null);
            fail("Expected IllegalStateException for control-only txnEnd row "
                    + "with partition<0 in multi-table mode");
        } catch (IllegalStateException e) {
            caught = e;
        }

        assertNotNull(caught);
        assertTrue("Error should mention multi-table transaction: " + caught.getMessage(),
                caught.getMessage().contains("Multi-table transaction mode"));
        assertTrue("Error should mention partition: " + caught.getMessage(),
                caught.getMessage().contains("partition=-1"));
    }

    @Test
    public void testSinkV2DoesNotThrowWhenMultiTableDisabledAndPartitionNegative() throws Exception {
        // Same rationale as testSinkV1DoesNotThrowWhenMultiTableDisabledAndPartitionNegative:
        // in non-multi-table mode, partition<0 must fall through to the legacy
        // write path without the fail-fast. Because sinkManager is null here
        // the fall-through manifests as an NPE, which is sufficient to prove
        // the guard was not tripped.
        StarRocksSinkOptions sinkOptions = buildNonMultiTableSinkOptions();
        assertTrue("Test setup: multi-table should be disabled",
                !sinkOptions.isMultiTableTransactionEnabled());

        StarRocksWriter<Object> writer = allocateWithoutConstructor(StarRocksWriter.class);
        setField(writer, StarRocksWriter.class, "sinkOptions", sinkOptions);
        setField(writer, StarRocksWriter.class, "serializationSchema",
                new TestRecordSerializationSchema(rowWithPartition(-1)));

        try {
            writer.write(new Object(), null);
            fail("Expected NullPointerException from the legacy write path "
                    + "(sinkManager is null in this bypassed-construction test)");
        } catch (IllegalStateException e) {
            fail("Fail-fast must NOT trigger in non-multi-table mode: " + e.getMessage());
        } catch (NullPointerException expected) {
            // Expected: execution proceeded past the fail-fast guard into
            // sinkManager.write(...) which is null.
        }
    }

    /**
     * Minimal test stub for the v2 {@code RecordSerializationSchema}. Returns
     * the pre-built {@link DefaultStarRocksRowData} regardless of the input
     * element, so the writer sees a fixed partition value.
     */
    private static final class TestRecordSerializationSchema
            implements com.starrocks.connector.flink.table.sink.v2.RecordSerializationSchema<Object> {
        private static final long serialVersionUID = 1L;
        private final DefaultStarRocksRowData row;

        TestRecordSerializationSchema(DefaultStarRocksRowData row) {
            this.row = row;
        }

        @Override
        public void open(
                org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext context,
                com.starrocks.connector.flink.table.sink.v2.StarRocksSinkContext sinkContext) {
            // no-op
        }

        @Override
        public DefaultStarRocksRowData serialize(Object element) {
            return row;
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
