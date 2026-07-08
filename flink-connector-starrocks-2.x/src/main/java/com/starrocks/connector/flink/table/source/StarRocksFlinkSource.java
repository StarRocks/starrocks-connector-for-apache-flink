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

import com.starrocks.connector.flink.table.source.struct.SelectColumn;

import java.util.ArrayList;
import java.util.Collection;

/**
 * FLIP-27 Source implementation for StarRocks.
 * Reads data from StarRocks using the BE scan interface.
 */
public class StarRocksFlinkSource
        implements Source<RowData, StarRocksSourceSplit, Collection<StarRocksSourceSplit>> {

    private static final long serialVersionUID = 1L;

    private final StarRocksSourceOptions sourceOptions;
    // Store column names and types separately since ResolvedSchema is not Serializable in Flink 2.0
    private final String[] columnNames;
    private final DataType[] columnTypes;
    private final String filter;
    private final long limit;
    private final SelectColumn[] selectColumns;
    private final StarRocksSourceQueryType queryType;

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
        this.selectColumns = selectColumns;
        // Auto-detect QueryCount from scan.columns option when using DataStream API
        if (queryType != null) {
            this.queryType = queryType;
        } else {
            String columns = sourceOptions.getColumns();
            if (columns != null && columns.trim().startsWith("count(")) {
                this.queryType = StarRocksSourceQueryType.QueryCount;
            } else {
                this.queryType = null;
            }
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, StarRocksSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        ResolvedSchema flinkSchema = ResolvedSchema.physical(columnNames, columnTypes);
        return new StarRocksSourceReader(
                readerContext, sourceOptions, flinkSchema, filter, limit, selectColumns, queryType);
    }

    @Override
    public SplitEnumerator<StarRocksSourceSplit, Collection<StarRocksSourceSplit>> createEnumerator(
            SplitEnumeratorContext<StarRocksSourceSplit> enumContext) throws Exception {
        return new StarRocksSourceEnumerator(
                enumContext, sourceOptions, columnNames, filter, limit, selectColumns, queryType, new ArrayList<>());
    }

    @Override
    public SplitEnumerator<StarRocksSourceSplit, Collection<StarRocksSourceSplit>> restoreEnumerator(
            SplitEnumeratorContext<StarRocksSourceSplit> enumContext,
            Collection<StarRocksSourceSplit> checkpoint) throws Exception {
        return new StarRocksSourceEnumerator(
                enumContext, sourceOptions, columnNames, filter, limit, selectColumns, queryType, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<StarRocksSourceSplit> getSplitSerializer() {
        return StarRocksSourceSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<Collection<StarRocksSourceSplit>> getEnumeratorCheckpointSerializer() {
        return StarRocksSourceEnumeratorStateSerializer.INSTANCE;
    }
}
