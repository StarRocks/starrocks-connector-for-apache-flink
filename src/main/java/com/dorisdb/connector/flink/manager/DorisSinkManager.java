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

package com.dorisdb.connector.flink.manager;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.table.api.TableColumn;

import com.dorisdb.connector.flink.table.DorisSinkOptions;
import com.dorisdb.connector.flink.table.DorisSinkSemantic;
import com.dorisdb.connector.flink.connection.DorisJdbcConnectionOptions;
import com.dorisdb.connector.flink.connection.DorisJdbcConnectionProvider;

public class DorisSinkManager implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkManager.class);

    private final DorisJdbcConnectionProvider jdbcConnProvider;
    private final DorisQueryVisitor dorisQueryVisitor;
    private final DorisStreamLoadVisitor dorisStreamLoadVisitor;
    private final DorisSinkOptions sinkOptions;
    private final Map<String, List<LogicalTypeRoot>> typesMap;


    private final List<String> buffer = new ArrayList<>();
    private int batchCount = 0;
    private long batchSize = 0;
    private volatile boolean closed = false;
    private volatile Exception flushException;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture;

    public DorisSinkManager(DorisSinkOptions sinkOptions, TableSchema flinkSchema) {
        this.sinkOptions = sinkOptions;
        DorisJdbcConnectionOptions jdbcOptions = new DorisJdbcConnectionOptions(sinkOptions.getJdbcUrl(), sinkOptions.getUsername(), sinkOptions.getPassword());
        this.jdbcConnProvider = new DorisJdbcConnectionProvider(jdbcOptions);
        this.dorisQueryVisitor = new DorisQueryVisitor(jdbcConnProvider, sinkOptions.getDatabaseName(), sinkOptions.getTableName());
        this.dorisStreamLoadVisitor = new DorisStreamLoadVisitor(sinkOptions, null == flinkSchema ? new String[]{} : flinkSchema.getFieldNames());
        // validate table structure
        typesMap = new HashMap<>();
        typesMap.put("bigint", Lists.newArrayList(LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER));
        typesMap.put("largeint", Lists.newArrayList(LogicalTypeRoot.DECIMAL, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER));
        typesMap.put("boolean", Lists.newArrayList(LogicalTypeRoot.BOOLEAN));
        typesMap.put("char", Lists.newArrayList(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR));
        typesMap.put("date", Lists.newArrayList(LogicalTypeRoot.DATE, LogicalTypeRoot.VARCHAR));
        typesMap.put("datetime", Lists.newArrayList(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, LogicalTypeRoot.VARCHAR));
        typesMap.put("decimal", Lists.newArrayList(LogicalTypeRoot.DECIMAL, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.DOUBLE, LogicalTypeRoot.FLOAT));
        typesMap.put("double", Lists.newArrayList(LogicalTypeRoot.DOUBLE, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER));
        typesMap.put("float", Lists.newArrayList(LogicalTypeRoot.FLOAT, LogicalTypeRoot.INTEGER));
        typesMap.put("int", Lists.newArrayList(LogicalTypeRoot.INTEGER));
        typesMap.put("tinyint", Lists.newArrayList(LogicalTypeRoot.TINYINT, LogicalTypeRoot.INTEGER));
        typesMap.put("smallint", Lists.newArrayList(LogicalTypeRoot.SMALLINT, LogicalTypeRoot.INTEGER));
        typesMap.put("varchar", Lists.newArrayList(LogicalTypeRoot.VARCHAR));
        typesMap.put("bitmap", Lists.newArrayList(LogicalTypeRoot.VARCHAR));
        if (!sinkOptions.hasColumnMappingProperty() && null != flinkSchema) {
            validateTableStructure(flinkSchema);
        }
    }

    public void startScheduler() throws IOException {
        if (DorisSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            return;
        }
        if (this.scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }
        this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("doris-table-sink"));
        this.scheduledFuture = this.scheduler.schedule(() -> {
            synchronized (DorisSinkManager.this) {
                if (!closed) {
                    try {
                        flush(createBatchLabel());
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }
        }, sinkOptions.getSinkMaxFlushInterval(), TimeUnit.MILLISECONDS);
    }

    public final synchronized void writeRecord(String record) throws IOException {
        checkFlushException();
        try {
            buffer.add(record);
            batchCount++;
            batchSize += record.length();
            if (DorisSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
                return;
            }
            if (batchCount >= sinkOptions.getSinkMaxRows() || batchSize >= sinkOptions.getSinkMaxBytes()) {
                flush(createBatchLabel());
            }
        } catch (Exception e) {
            throw new IOException("Writing records to Doris failed.", e);
        }
    }

    public synchronized void flush(String label) throws IOException {
        checkFlushException();
        if (batchCount == 0) {
            return;
        }
        for (int i = 0; i <= sinkOptions.getSinkMaxRetries(); i++) {
            try {
                tryToFlush(label);
                batchCount = 0;
                batchSize = 0;
                startScheduler();
                break;
            } catch (IOException e) {
                LOG.warn("Failed to flush batch data to doris, retry times = {}", i, e);
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
    
    public synchronized void close() {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            if (batchCount > 0) {
                try {
                    flush(createBatchLabel());
                } catch (Exception e) {
                    throw new RuntimeException("Writing records to Doris failed.", e);
                }
            }
        }
        checkFlushException();
    }

    public String createBatchLabel() {
        return UUID.randomUUID().toString();
    }

    public List<String> getBufferedBatchList() {
        return buffer;
    }

    public void setBufferedBatchList(List<String> buffer) {
        if (!DorisSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            return;
        }
        this.buffer.clear();
        this.buffer.addAll(buffer);
    }

    private void tryToFlush(String label) throws IOException {
        // flush to Doris with stream load
        dorisStreamLoadVisitor.doStreamLoad(new Tuple2<>(label, buffer));
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to Doris failed.", flushException);
        }
    }
    
    private void validateTableStructure(TableSchema flinkSchema) {
        List<Map<String, Object>> rows = dorisQueryVisitor.getTableColumnsMetaData();
        if (null == rows || flinkSchema.getFieldCount() != rows.size()) {
            throw new IllegalArgumentException("Fields count mismatch.");
        }
        List<TableColumn> flinkCols = flinkSchema.getTableColumns();
        for (int i = 0; i < rows.size(); i++) {
            String dorisField = rows.get(i).get("COLUMN_NAME").toString().toLowerCase();
            String dorisType = rows.get(i).get("DATA_TYPE").toString().toLowerCase();
            List<TableColumn> matchedFlinkCols = flinkCols.stream()
                .filter(col -> col.getName().toLowerCase().equals(dorisField) && (!typesMap.containsKey(dorisType) || typesMap.get(dorisType).contains(col.getType().getLogicalType().getTypeRoot())))
                .collect(Collectors.toList());
            if (matchedFlinkCols.isEmpty()) {
                throw new IllegalArgumentException("Fields name or type mismatch for:" + dorisField);
            }
        }
    }
}
