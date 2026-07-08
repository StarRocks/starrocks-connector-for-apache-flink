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

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/** FLIP-27 SourceReader that reads data from StarRocks BE nodes. */
public class StarRocksSourceReader implements SourceReader<RowData, StarRocksSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSourceReader.class);

    private final SourceReaderContext context;
    private final StarRocksSourceOptions sourceOptions;
    private final String[] columnNames;
    private final List<ColumnRichInfo> columnRichInfos;
    private final SelectColumn[] selectColumns;
    private final StarRocksSourceQueryType queryType;
    private final String filter;
    private final long limit;

    private final Queue<StarRocksSourceSplit> assignedSplits = new ArrayDeque<>();
    private boolean noMoreSplits = false;
    private CompletableFuture<Void> availableFuture = new CompletableFuture<>();

    private StarRocksSourceDataReader currentReader;
    private StarRocksSourceSplit currentSplit;
    private QueryInfo queryInfo;

    public StarRocksSourceReader(
            SourceReaderContext context,
            StarRocksSourceOptions sourceOptions,
            ResolvedSchema flinkSchema,
            String filter,
            long limit,
            SelectColumn[] selectColumns,
            StarRocksSourceQueryType queryType) {
        this.context = context;
        this.sourceOptions = sourceOptions;
        this.columnNames = flinkSchema.getColumnNames().toArray(new String[0]);
        this.filter = filter;
        this.limit = limit;
        this.queryType = queryType;

        Map<String, ColumnRichInfo> columnMap = StarRocksSourceCommonFunc.genColumnMap(flinkSchema);
        this.columnRichInfos = StarRocksSourceCommonFunc.genColumnRichInfo(columnMap);
        // For count queries, selectColumns aren't needed (no data columns to read)
        if (queryType == StarRocksSourceQueryType.QueryCount) {
            this.selectColumns = null;
        } else if (selectColumns != null) {
            this.selectColumns = selectColumns;
        } else {
            // When no projection push-down, generate selectColumns from all columns
            this.selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(columnMap, sourceOptions, columnRichInfos);
        }
    }

    @Override
    public void start() {
        if (queryType != StarRocksSourceQueryType.QueryCount) {
            String sql = StarRocksSourceCommonFunc.buildSQL(
                    queryType, selectColumns, columnNames, sourceOptions, filter);
            this.queryInfo = StarRocksSourceCommonFunc.getQueryInfo(sourceOptions, sql);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (currentReader != null && currentReader.hasNext()) {
            GenericRowData row = currentReader.getNext();
            if (row != null) {
                output.collect(row);
                return InputStatus.MORE_AVAILABLE;
            }
        }

        // Current reader exhausted, close it and try next split
        closeCurrentReader();

        if (!assignedSplits.isEmpty()) {
            openNextSplit();
            return InputStatus.MORE_AVAILABLE;
        }

        if (noMoreSplits) {
            return InputStatus.END_OF_INPUT;
        }

        return InputStatus.NOTHING_AVAILABLE;
    }

    private void openNextSplit() {
        currentSplit = assignedSplits.poll();
        if (currentSplit == null) {
            return;
        }

        QueryBeXTablets beXTablets = currentSplit.getBeXTablets();

        if (queryType == StarRocksSourceQueryType.QueryCount) {
            // Parse count from split ID (format: "count-<number>")
            String splitId = currentSplit.splitId();
            long count = Long.parseLong(splitId.substring("count-".length()));
            currentReader = new StarRocksSourceTrickReader(count);
        } else {
            StarRocksSourceBeReader beReader = new StarRocksSourceBeReader(
                    beXTablets.getBeNode(), columnRichInfos, selectColumns, sourceOptions);
            try {
                beReader.openScanner(
                        beXTablets.getTabletIds(),
                        queryInfo.getQueryPlan().getOpaqued_query_plan(),
                        sourceOptions);
                beReader.startToRead();
            } catch (Exception e) {
                beReader.close();
                throw e;
            }
            currentReader = beReader;
        }
        LOG.info("Opened split {} for reading", currentSplit.splitId());
    }

    private void closeCurrentReader() {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
            currentSplit = null;
        }
    }

    @Override
    public List<StarRocksSourceSplit> snapshotState(long checkpointId) {
        List<StarRocksSourceSplit> state = new ArrayList<>(assignedSplits);
        if (currentSplit != null) {
            state.add(0, currentSplit);
        }
        return state;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (currentReader != null || !assignedSplits.isEmpty() || noMoreSplits) {
            return CompletableFuture.completedFuture(null);
        }
        return availableFuture;
    }

    @Override
    public void addSplits(List<StarRocksSourceSplit> splits) {
        assignedSplits.addAll(splits);
        completeFuture();
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
        completeFuture();
    }

    private void completeFuture() {
        availableFuture.complete(null);
        availableFuture = new CompletableFuture<>();
    }

    @Override
    public void close() throws Exception {
        closeCurrentReader();
    }
}
