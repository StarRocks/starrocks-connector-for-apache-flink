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

import com.google.common.base.Strings;
import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.tools.EnvUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class StarRocksDynamicSourceFunction extends RichParallelSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicSourceFunction.class);

    private final StarRocksSourceOptions sourceOptions;
    private QueryInfo queryInfo;
    private Long dataCount;
    private final SelectColumn[] selectColumns;
    private final List<ColumnRichInfo> columnRichInfos;
    private List<StarRocksSourceDataReader> dataReaderList;
    
    private StarRocksSourceQueryType queryType;

    private transient Counter counterTotalScannedRows;
    private transient AtomicBoolean dataReaderClosed;
    private static final String TOTAL_SCANNED_ROWS = "totalScannedRows";

    public StarRocksDynamicSourceFunction(TableSchema flinkSchema, StarRocksSourceOptions sourceOptions) {
        // StarRocksSourceCommonFunc.validateTableStructure(sourceOptions, flinkSchema);
        this.sourceOptions = sourceOptions;
        Map<String, ColumnRichInfo> columnMap = StarRocksSourceCommonFunc.genColumnMap(flinkSchema);
        this.columnRichInfos = StarRocksSourceCommonFunc.genColumnRichInfo(columnMap);
        String SQL = genSQL(sourceOptions);
        if (this.sourceOptions.getColumns().trim().toLowerCase().contains("count(")) {
            this.queryType = StarRocksSourceQueryType.QueryCount;
            this.dataCount = StarRocksSourceCommonFunc.getQueryCount(this.sourceOptions, SQL);
            this.selectColumns = null;
        } else {
            this.queryInfo = StarRocksSourceCommonFunc.getQueryInfo(this.sourceOptions, SQL);
            this.selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(columnMap, sourceOptions, columnRichInfos);
        }
        this.dataReaderList = new ArrayList<>();
    }

    public StarRocksDynamicSourceFunction(StarRocksSourceOptions sourceOptions, TableSchema flinkSchema, 
                                            String filter, long limit, SelectColumn[] selectColumns, String columns, StarRocksSourceQueryType queryType) {
        // StarRocksSourceCommonFunc.validateTableStructure(sourceOptions, flinkSchema);
        this.sourceOptions = sourceOptions;
        Map<String, ColumnRichInfo> columnMap = StarRocksSourceCommonFunc.genColumnMap(flinkSchema);
        this.columnRichInfos = StarRocksSourceCommonFunc.genColumnRichInfo(columnMap);
        if (queryType == null) {
            queryType = StarRocksSourceQueryType.QueryAllColumns;
            this.selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(columnMap, sourceOptions, columnRichInfos);
        } else {
            this.selectColumns = selectColumns;
        }
        String SQL = genSQL(queryType, columns, filter, limit);
        if (queryType == StarRocksSourceQueryType.QueryCount) {
            this.dataCount = StarRocksSourceCommonFunc.getQueryCount(this.sourceOptions, SQL);
        } else {
            this.queryInfo = StarRocksSourceCommonFunc.getQueryInfo(this.sourceOptions, SQL);
        }
        this.queryType = queryType;
        this.dataReaderList = new ArrayList<>();
    }

    private String genSQL(StarRocksSourceOptions options) {
        String columns = options.getColumns().isEmpty() ? "*" : options.getColumns();
        String filter = options.getFilter().isEmpty() ? "" : " where " + options.getFilter();
        return "select " + columns +
                " from " +
                "`" + sourceOptions.getDatabaseName() + "`" +
                "." +
                "`" + sourceOptions.getTableName() + "`" +
                filter;
    }

    private String genSQL(StarRocksSourceQueryType queryType, String columns, String filter, long limit) {
        StringBuilder sqlSb = new StringBuilder("select ");
        switch (queryType) {
        case QueryCount:
            sqlSb.append("count(*)");
            break;
        case QueryAllColumns:
            sqlSb.append("*");
            break;
        case QuerySomeColumns:
            sqlSb.append(columns);
            break;
        }
        sqlSb.append(" from ");
        sqlSb.append("`").append(sourceOptions.getDatabaseName()).append("`");
        sqlSb.append(".");
        sqlSb.append("`").append(sourceOptions.getTableName()).append("`");
        if (!Strings.isNullOrEmpty(filter)) {
            sqlSb.append(" where ");
            sqlSb.append(filter);
        }
        if (limit > 0) {
            // (not support) SQL = SQL + " limit " + limit;
            throw new RuntimeException("Read data from be not support limit now !");
        }
        return sqlSb.toString();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.dataReaderClosed = new AtomicBoolean(false);
        this.counterTotalScannedRows = getRuntimeContext().getMetricGroup().counter(TOTAL_SCANNED_ROWS);

        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        if (this.queryType == StarRocksSourceQueryType.QueryCount) {
            if (subTaskId == 0) {
                StarRocksSourceTrickReader reader = new StarRocksSourceTrickReader(this.dataCount);
                this.dataReaderList.add(reader);
            }
        } else {
            List<List<QueryBeXTablets>> lists = StarRocksSourceCommonFunc.splitQueryBeXTablets(getRuntimeContext().getNumberOfParallelSubtasks(), queryInfo);
            lists.get(subTaskId).forEach(beXTablets -> {
                StarRocksSourceBeReader beReader = new StarRocksSourceBeReader(beXTablets.getBeNode(), columnRichInfos, selectColumns, sourceOptions);
                beReader.openScanner(beXTablets.getTabletIds(), queryInfo.getQueryPlan().getOpaqued_query_plan(), sourceOptions);
                beReader.startToRead();
                this.dataReaderList.add(beReader);
            });
        }
        LOG.info("Open source function. {}", EnvUtils.getGitInformation());
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) {
        this.dataReaderList.parallelStream().forEach(dataReader -> {
            while (dataReader.hasNext()) {
                RowData row = dataReader.getNext();
                counterTotalScannedRows.inc(1);
                sourceContext.collect(row);
            }
        });
    }

    @Override
    public void cancel() {
        internalClose();
    }

    @Override
    public void close() {
        internalClose();
    }

    private void internalClose() {
        if (dataReaderClosed.compareAndSet(false, true)) {
            LOG.info("Close readers");
            this.dataReaderList.parallelStream().forEach(dataReader -> {
                if (dataReader != null) {
                    dataReader.close();
                }
            });
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(new TypeHint<RowData>(){});
    }
}
