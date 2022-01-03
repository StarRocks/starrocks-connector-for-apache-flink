package com.starrocks.connector.flink.table.source;

import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
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

import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StarRocksDynamicSourceFunction extends RichParallelSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

    private final StarRocksSourceOptions sourceOptions;
    private QueryInfo queryInfo;
    private int dataCount;
    private final SelectColumn[] selectColumns;
    private final List<ColunmRichInfo> colunmRichInfos;
    private List<StarRocksSourceDataReader> dataReaderList;
    
    private StarRocksSourceQueryType queryType;

    private transient Counter totalCountDataRowsOfSingleSubTask;
    private static final String COUNTER_TOTAL_COUNT_DATA_ROWS_OF_SINGLE_SUBTASK = "totalCountDataRowsOfSingleSubTask";

    public StarRocksDynamicSourceFunction(StarRocksSourceOptions sourceOptions, TableSchema flinkSchema) {
        
        // StarRocksSourceCommonFunc.validateTableStructure(sourceOptions, flinkSchema);
        this.sourceOptions = sourceOptions;
        Map<String, ColunmRichInfo> columnMap = StarRocksSourceCommonFunc.genColumnMap(flinkSchema);
        this.colunmRichInfos = StarRocksSourceCommonFunc.genColunmRichInfo(columnMap);
        String SQL = genSQL(sourceOptions);
        if (this.sourceOptions.getColumns().trim().equals("count(*)")) {
            this.queryType = StarRocksSourceQueryType.QueryCount;
            this.dataCount = StarRocksSourceCommonFunc.getQueryCount(this.sourceOptions, SQL);
            this.selectColumns = null;
        } else {
            this.queryInfo = StarRocksSourceCommonFunc.getQueryInfo(this.sourceOptions, SQL);
            this.selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(columnMap, sourceOptions, colunmRichInfos);
        }
        this.dataReaderList = new ArrayList<>();
    }

    public StarRocksDynamicSourceFunction(StarRocksSourceOptions sourceOptions, TableSchema flinkSchema, 
                                            String filter, long limit, SelectColumn[] selectColumns, String columns, StarRocksSourceQueryType queryType) {
        
        // StarRocksSourceCommonFunc.validateTableStructure(sourceOptions, flinkSchema);
        this.sourceOptions = sourceOptions;
        Map<String, ColunmRichInfo> columnMap = StarRocksSourceCommonFunc.genColumnMap(flinkSchema);
        this.colunmRichInfos = StarRocksSourceCommonFunc.genColunmRichInfo(columnMap);
        if (queryType == null) {
            queryType = StarRocksSourceQueryType.QueryAllColumns;
            this.selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(columnMap, sourceOptions, colunmRichInfos);
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

        String columns = options.getColumns().equals("") ? "*" : options.getColumns();
        String filter = options.getFilter().equals("") ? "" : " where " + options.getFilter();
        String SQL = "select " + columns + " from " + options.getDatabaseName() + "." + options.getTableName() + filter;
        return SQL;
    }

    private String genSQL(StarRocksSourceQueryType queryType, String columns, String filter, long limit) {

        String SQL = "select";
        switch (queryType) {
        case QueryCount:
            SQL = SQL + " count(*) ";
            break;
        case QueryAllColumns:
            SQL = SQL + " * ";
            break;
        case QuerySomeColumns:
            SQL = SQL + " " + columns + " ";
            break;
        }
        SQL = SQL + " from " + sourceOptions.getDatabaseName() + "." + sourceOptions.getTableName();
        if (!(filter == null || filter.equals(""))) {
            SQL = SQL + " where " + filter;
        }
        if (limit > 0) {
            // (not support) SQL = SQL + " limit " + limit;
            throw new RuntimeException("Read data from be not support limit now !");
        }
        return SQL;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.totalCountDataRowsOfSingleSubTask = getRuntimeContext().getMetricGroup().counter(COUNTER_TOTAL_COUNT_DATA_ROWS_OF_SINGLE_SUBTASK);

        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        if (this.queryType == StarRocksSourceQueryType.QueryCount) {
            if (subTaskId == 0) {
                StarRocksSourceTrickReader reader = new StarRocksSourceTrickReader(this.dataCount);
                this.dataReaderList.add(reader);
            }
        } else {
            List<List<QueryBeXTablets>> lists = StarRocksSourceCommonFunc.splitQueryBeXTablets(getRuntimeContext().getNumberOfParallelSubtasks(), queryInfo);
            lists.get(subTaskId).forEach(beXTablets -> {
                StarRocksSourceBeReader beReader = new StarRocksSourceBeReader(beXTablets.getBeNode(), colunmRichInfos, selectColumns, sourceOptions);
                beReader.openScanner(beXTablets.getTabletIds(), queryInfo.getQueryPlan().getOpaqued_query_plan(), sourceOptions);
                beReader.startToRead();
                this.dataReaderList.add(beReader);
            });
        }
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) {

        this.dataReaderList.forEach(dataReader -> {
            while (dataReader.hasNext()) {
                RowData row = dataReader.getNext();
                totalCountDataRowsOfSingleSubTask.inc(1);
                sourceContext.collect(row);
            }
        });
    }

    @Override
    public void cancel() {
        this.dataReaderList.forEach(dataReader -> {
            if (dataReader != null) {
                dataReader.close();
            }
        });
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(new TypeHint<RowData>(){});
    }
}
