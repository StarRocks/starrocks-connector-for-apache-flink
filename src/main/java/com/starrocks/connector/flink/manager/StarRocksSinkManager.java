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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.StarRocksSinkSemantic;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.constraints.UniqueConstraint;

public class StarRocksSinkManager implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkManager.class);

    private final StarRocksJdbcConnectionProvider jdbcConnProvider;
    private final StarRocksQueryVisitor starrocksQueryVisitor;
    private final StarRocksStreamLoadVisitor starrocksStreamLoadVisitor;
    private final StarRocksSinkOptions sinkOptions;
    private final Map<String, List<LogicalTypeRoot>> typesMap;
    private final LinkedBlockingDeque<Tuple3<String, Long, ArrayList<byte[]>>> flushQueue = new LinkedBlockingDeque<>(1);

    private transient Counter totalFlushBytes;
    private transient Counter totalFlushRows;
    private transient Counter totalFlushTime;
    private transient Counter totalFlushTimeWithoutRetries;
    private static final String COUNTER_TOTAL_FLUSH_BYTES = "totalFlushBytes";
    private static final String COUNTER_TOTAL_FLUSH_ROWS = "totalFlushRows";
    private static final String COUNTER_TOTAL_FLUSH_COST_TIME_WITHOUT_RETRIES = "totalFlushTimeNsWithoutRetries";
    private static final String COUNTER_TOTAL_FLUSH_COST_TIME = "totalFlushTimeNs";

    private final ArrayList<byte[]> buffer = new ArrayList<>();
    private int batchCount = 0;
    private long batchSize = 0;
    private volatile boolean closed = false;
    private volatile Exception flushException;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture;

    public StarRocksSinkManager(StarRocksSinkOptions sinkOptions, TableSchema flinkSchema) {
        this.sinkOptions = sinkOptions;
        StarRocksJdbcConnectionOptions jdbcOptions = new StarRocksJdbcConnectionOptions(sinkOptions.getJdbcUrl(), sinkOptions.getUsername(), sinkOptions.getPassword());
        this.jdbcConnProvider = new StarRocksJdbcConnectionProvider(jdbcOptions);
        this.starrocksQueryVisitor = new StarRocksQueryVisitor(jdbcConnProvider, sinkOptions.getDatabaseName(), sinkOptions.getTableName());
        // validate table structure
        typesMap = new HashMap<>();
        typesMap.put("bigint", Lists.newArrayList(LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("largeint", Lists.newArrayList(LogicalTypeRoot.DECIMAL, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("char", Lists.newArrayList(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR));
        typesMap.put("date", Lists.newArrayList(LogicalTypeRoot.DATE, LogicalTypeRoot.VARCHAR));
        typesMap.put("datetime", Lists.newArrayList(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, LogicalTypeRoot.VARCHAR));
        typesMap.put("decimal", Lists.newArrayList(LogicalTypeRoot.DECIMAL, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.DOUBLE, LogicalTypeRoot.FLOAT));
        typesMap.put("double", Lists.newArrayList(LogicalTypeRoot.DOUBLE, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER));
        typesMap.put("float", Lists.newArrayList(LogicalTypeRoot.FLOAT, LogicalTypeRoot.INTEGER));
        typesMap.put("int", Lists.newArrayList(LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("tinyint", Lists.newArrayList(LogicalTypeRoot.TINYINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY, LogicalTypeRoot.BOOLEAN));
        typesMap.put("smallint", Lists.newArrayList(LogicalTypeRoot.SMALLINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("varchar", Lists.newArrayList(LogicalTypeRoot.VARCHAR, LogicalTypeRoot.ARRAY, LogicalTypeRoot.MAP, LogicalTypeRoot.ROW));
        typesMap.put("string", Lists.newArrayList(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR, LogicalTypeRoot.ARRAY, LogicalTypeRoot.MAP, LogicalTypeRoot.ROW));
        validateTableStructure(flinkSchema);
        this.starrocksStreamLoadVisitor = new StarRocksStreamLoadVisitor(sinkOptions, null == flinkSchema ? new String[]{} : flinkSchema.getFieldNames());
    }

    public void setRuntimeContext(RuntimeContext runtimeCtx) {
        totalFlushBytes = runtimeCtx.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_BYTES);
        totalFlushRows = runtimeCtx.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_ROWS);
        totalFlushTime = runtimeCtx.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_COST_TIME);
        totalFlushTimeWithoutRetries = runtimeCtx.getMetricGroup().counter(COUNTER_TOTAL_FLUSH_COST_TIME_WITHOUT_RETRIES);
    }

    public void startAsyncFlushing() {
        // start flush thread
        Thread flushThread = new Thread(new Runnable(){
            public void run() {
                while(true) {
                    try {
                        asyncFlush();
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }   
        });
        flushThread.setDaemon(true);
        flushThread.start();
    }

    public void startScheduler() throws IOException {
        if (StarRocksSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            return;
        }
        stopScheduler();
        this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("starrocks-interval-sink"));
        this.scheduledFuture = this.scheduler.schedule(() -> {
            synchronized (StarRocksSinkManager.this) {
                if (!closed) {
                    try {
                        String label = createBatchLabel();
                        LOG.info(String.format("StarRocks interval Sinking triggered: label[%s].", label));
                        if (batchCount == 0) {
                            startScheduler();
                        }
                        flush(label, false);
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }
        }, sinkOptions.getSinkMaxFlushInterval(), TimeUnit.MILLISECONDS);
    }

    public void stopScheduler() {
        if (this.scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }
    }

    public final synchronized void writeRecord(String record) throws IOException {
        checkFlushException();
        try {
            byte[] bts = record.getBytes(StandardCharsets.UTF_8);
            buffer.add(bts);
            batchCount++;
            batchSize += bts.length;
            if (StarRocksSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
                return;
            }
            if (batchCount >= sinkOptions.getSinkMaxRows() || batchSize >= sinkOptions.getSinkMaxBytes()) {
                String label = createBatchLabel();
                LOG.info(String.format("StarRocks buffer Sinking triggered: rows[%d] label[%s].", batchCount, label));
                flush(label, false);
            }
        } catch (Exception e) {
            throw new IOException("Writing records to StarRocks failed.", e);
        }
    }

    public synchronized void flush(String label, boolean waitUtilDone) throws Exception {
        checkFlushException();
        if (batchCount == 0) {
            if (waitUtilDone) {
                waitAsyncFlushingDone();
            }
            return;
        }
        flushQueue.put(new Tuple3<>(label, batchSize,  new ArrayList<>(buffer)));
        if (waitUtilDone) {
            // wait the last flush
            waitAsyncFlushingDone();
        }
        buffer.clear();
        batchCount = 0;
        batchSize = 0;
    }
    
    public synchronized void close() {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }
            try {
                String label = createBatchLabel();
                if (batchCount > 0) LOG.info(String.format("StarRocks Sink is about to close: label[%s].", label));
                flush(label, true);
            } catch (Exception e) {
                throw new RuntimeException("Writing records to StarRocks failed.", e);
            }
        }
        checkFlushException();
    }

    public String createBatchLabel() {
        return UUID.randomUUID().toString();
    }

    public List<byte[]> getBufferedBatchList() {
        return buffer;
    }

    public void setBufferedBatchList(List<byte[]> list) throws IOException {
        if (!StarRocksSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            return;
        }
        this.buffer.clear();
        batchCount = 0;
        batchSize = 0;
        for (byte[] row : list) {
            writeRecord(new String(row, StandardCharsets.UTF_8));
        }
    }

    private void waitAsyncFlushingDone() throws InterruptedException {
        // wait for previous flushings
        flushQueue.put(new Tuple3<>("", 0l, null));
        flushQueue.put(new Tuple3<>("", 0l, null));
        checkFlushException();
    }

    private void asyncFlush() throws Exception {
        Tuple3<String, Long, ArrayList<byte[]>> flushData = flushQueue.take();
        if (Strings.isNullOrEmpty(flushData.f0)) {
            return;
        }
        stopScheduler();
        LOG.info(String.format("Async stream load: rows[%d] bytes[%d] label[%s].", flushData.f2.size(), flushData.f1, flushData.f0));
        long startWithRetries = System.nanoTime();
        for (int i = 0; i <= sinkOptions.getSinkMaxRetries(); i++) {
            try {
                long start = System.nanoTime();
                // flush to StarRocks with stream load
                starrocksStreamLoadVisitor.doStreamLoad(flushData);
                LOG.info(String.format("Async stream load finished: label[%s].", flushData.f0));
                // metrics
                if (null != totalFlushBytes) {
                    totalFlushBytes.inc(flushData.f1);
                    totalFlushRows.inc(flushData.f2.size());
                    totalFlushTime.inc(System.nanoTime() - startWithRetries);
                    totalFlushTimeWithoutRetries.inc(System.nanoTime() - start);
                }
                startScheduler();
                break;
            } catch (Exception e) {
                LOG.warn("Failed to flush batch data to StarRocks, retry times = {}", i, e);
                if (i >= sinkOptions.getSinkMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    Thread.sleep(1000l * (i + 1));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Unable to flush, interrupted while doing another attempt", e);
                }
            }
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            for(int i = 0; i < stack.length; i++){
                LOG.info(stack[i].getClassName()+"."+stack[i].getMethodName()+" line:"+stack[i].getLineNumber());
            }
            throw new RuntimeException("Writing records to StarRocks failed.", flushException);
        }
    }
    
    private void validateTableStructure(TableSchema flinkSchema) {
        if (null == flinkSchema) {
            return;
        }
        Optional<UniqueConstraint> constraint = flinkSchema.getPrimaryKey();
        List<Map<String, Object>> rows = starrocksQueryVisitor.getTableColumnsMetaData();
        if (null == rows) {
            throw new IllegalArgumentException("Couldn't get the sink table's column info.");
        }
        // validate primary keys
        List<String> primayKeys = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            String keysType = rows.get(i).get("COLUMN_KEY").toString();
            if (!"PRI".equals(keysType)) {
                continue;
            }
            primayKeys.add(rows.get(i).get("COLUMN_NAME").toString().toLowerCase());
        }
        if (!primayKeys.isEmpty()) {
            if (!constraint.isPresent()) {
                throw new IllegalArgumentException("Source table schema should contain primary keys.");
            }
            if (constraint.get().getColumns().size() != primayKeys.size() ||
                !constraint.get().getColumns().stream().allMatch(col -> primayKeys.contains(col.toLowerCase()))) {
                throw new IllegalArgumentException("Primary keys of the source table do not match with the ones of the sink table.");
            }
            sinkOptions.enableUpsertDelete();
        }

        if (sinkOptions.hasColumnMappingProperty()) {
            return;
        }
        if (flinkSchema.getFieldCount() != rows.size()) {
            throw new IllegalArgumentException("Fields count mismatch.");
        }
        List<TableColumn> flinkCols = flinkSchema.getTableColumns();
        for (int i = 0; i < rows.size(); i++) {
            String starrocksField = rows.get(i).get("COLUMN_NAME").toString().toLowerCase();
            String starrocksType = rows.get(i).get("DATA_TYPE").toString().toLowerCase();
            List<TableColumn> matchedFlinkCols = flinkCols.stream()
                .filter(col -> col.getName().toLowerCase().equals(starrocksField) && (!typesMap.containsKey(starrocksType) || typesMap.get(starrocksType).contains(col.getType().getLogicalType().getTypeRoot())))
                .collect(Collectors.toList());
            if (matchedFlinkCols.isEmpty()) {
                throw new IllegalArgumentException("Fields name or type mismatch for:" + starrocksField);
            }
        }
    }
}
