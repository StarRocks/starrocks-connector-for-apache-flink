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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * FLIP-27 Source implementation for StarRocks.
 * Reads data from StarRocks using the BE scan interface.
 */
public class StarRocksFlinkSource
        implements Source<RowData, StarRocksSourceSplit, StarRocksSourceEnumeratorState> {

    private static final long serialVersionUID = 1L;

    private final StarRocksSourceOptions sourceOptions;
    // Store column names and types separately since ResolvedSchema is not Serializable in Flink 2.0
    private final String[] columnNames;
    private final DataType[] columnTypes;
    private final String filter;
    private final long limit;
    // resolved once so the planned SQL and the readers agree on the columns; null for count queries
    private final SelectColumn[] selectColumns;
    private final StarRocksSourceQueryType queryType;
    // the DataStream API runs a count in scan.columns as written, like 1.x; null means count(*)
    @Nullable
    private final String countExpression;

    public StarRocksFlinkSource(
            StarRocksSourceOptions sourceOptions,
            ResolvedSchema flinkSchema,
            String filter,
            long limit,
            SelectColumn[] selectColumns,
            StarRocksSourceQueryType queryType) {
        this.sourceOptions = sourceOptions;
        // Only include physical columns — computed/virtual columns (e.g. PROCTIME()) don't exist in StarRocks
        this.columnNames = flinkSchema.getColumns().stream()
                .filter(Column::isPhysical)
                .map(Column::getName)
                .toArray(String[]::new);
        this.columnTypes = flinkSchema.getColumns().stream()
                .filter(Column::isPhysical)
                .map(Column::getDataType)
                .toArray(DataType[]::new);
        this.filter = filter;
        this.limit = limit;
        // Auto-detect QueryCount from scan.columns option when using DataStream API
        String columns = sourceOptions.getColumns();
        if (queryType == null && columns != null && columns.trim().toLowerCase().contains("count(")) {
            this.queryType = StarRocksSourceQueryType.QueryCount;
            this.countExpression = columns.trim();
        } else {
            this.queryType = queryType;
            this.countExpression = null;
        }
        if (this.queryType == StarRocksSourceQueryType.QueryCount) {
            this.selectColumns = null;
        } else if (selectColumns != null) {
            this.selectColumns = selectColumns;
        } else {
            Map<String, ColumnRichInfo> columnMap = StarRocksSourceCommonFunc.genColumnMap(flinkSchema);
            this.selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(
                    columnMap, sourceOptions, StarRocksSourceCommonFunc.genColumnRichInfo(columnMap));
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, StarRocksSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        ResolvedSchema flinkSchema = ResolvedSchema.physical(columnNames, columnTypes);
        Map<String, ColumnRichInfo> columnMap = StarRocksSourceCommonFunc.genColumnMap(flinkSchema);
        List<ColumnRichInfo> columnRichInfos = StarRocksSourceCommonFunc.genColumnRichInfo(columnMap);
        String sql = scanSql();
        return new StarRocksSourceReader(sourceOptions, columnRichInfos, selectColumns,
                () -> StarRocksSourceCommonFunc.getQueryInfo(sourceOptions, sql).getQueryPlan(), readerContext);
    }

    @Override
    public SplitEnumerator<StarRocksSourceSplit, StarRocksSourceEnumeratorState> createEnumerator(
            SplitEnumeratorContext<StarRocksSourceSplit> enumContext) throws Exception {
        return new StarRocksSourceEnumerator(
                enumContext,
                splitDiscoverer(enumContext.currentParallelism()),
                new StarRocksSourceEnumeratorState(false, new ArrayList<>()));
    }

    @Override
    public SplitEnumerator<StarRocksSourceSplit, StarRocksSourceEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<StarRocksSourceSplit> enumContext,
            StarRocksSourceEnumeratorState checkpoint) throws Exception {
        return new StarRocksSourceEnumerator(
                enumContext, splitDiscoverer(enumContext.currentParallelism()), checkpoint);
    }

    private Callable<List<StarRocksSourceSplit>> splitDiscoverer(int parallelism) {
        return () -> discoverSplits(parallelism);
    }

    private String scanSql() {
        return StarRocksSourceCommonFunc.buildSQL(
                queryType, selectColumns, columnNames, countExpression, sourceOptions, filter);
    }

    private List<StarRocksSourceSplit> discoverSplits(int parallelism) {
        String sql = scanSql();
        List<StarRocksSourceSplit> splits = new ArrayList<>();
        if (queryType == StarRocksSourceQueryType.QueryCount) {
            Long count = StarRocksSourceCommonFunc.getQueryCount(sourceOptions, sql);
            splits.add(new StarRocksSourceSplit(new QueryBeXTablets("count", new ArrayList<>()), "count-" + count));
            return splits;
        }
        QueryInfo queryInfo = StarRocksSourceCommonFunc.getQueryInfo(sourceOptions, sql);
        String opaquedQueryPlan = queryInfo.getQueryPlan().getOpaqued_query_plan();
        int splitIndex = 0;
        for (List<QueryBeXTablets> subtaskSplits : StarRocksSourceCommonFunc.splitQueryBeXTablets(parallelism, queryInfo)) {
            for (QueryBeXTablets beXTablets : subtaskSplits) {
                String splitId = "split-" + splitIndex + "-" + beXTablets.getBeNode();
                splits.add(new StarRocksSourceSplit(beXTablets, splitId, opaquedQueryPlan));
                splitIndex++;
            }
        }
        return splits;
    }

    @Override
    public SimpleVersionedSerializer<StarRocksSourceSplit> getSplitSerializer() {
        return StarRocksSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<StarRocksSourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return StarRocksSourceEnumeratorStateSerializer.INSTANCE;
    }
}
