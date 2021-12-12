package com.starrocks.connector.flink.table.source;

import com.starrocks.connector.flink.manager.StarRocksQueryPlanVisitor;
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
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        Map<String, ColunmRichInfo> columnMap = genColumnMap(flinkSchema);
        this.colunmRichInfos = columnMap.values().stream().collect(Collectors.toList()).
                                stream().sorted(Comparator.comparing(ColunmRichInfo::getColunmIndexInSchema)).collect(Collectors.toList());
        
        String SQL = genSQL(sourceOptions);
        if (this.sourceOptions.getColumns().trim().equals("count(*)")) {
            this.queryType = StarRocksSourceQueryType.QueryCount;
            this.dataCount = StarRocksSourceCommonFunc.getQueryCount(this.sourceOptions, SQL);
            this.selectColumns = null;
        } else {
            this.queryInfo = getQueryInfo(SQL);
            this.selectColumns = genSelectedColumns(columnMap).toArray(new SelectColumn[0]);
        }
        this.dataReaderList = new ArrayList<>();
    }

    public StarRocksDynamicSourceFunction(StarRocksSourceOptions sourceOptions, TableSchema flinkSchema, 
                                            String filter, long limit, SelectColumn[] selectColumns, String columns, StarRocksSourceQueryType queryType) {
        
        // StarRocksSourceCommonFunc.validateTableStructure(sourceOptions, flinkSchema);
        this.sourceOptions = sourceOptions;
        Map<String, ColunmRichInfo> columnMap = genColumnMap(flinkSchema);
        this.colunmRichInfos = columnMap.values().stream().collect(Collectors.toList()).
                                stream().sorted(Comparator.comparing(ColunmRichInfo::getColunmIndexInSchema)).collect(Collectors.toList());
        if (queryType == null) {
            queryType = StarRocksSourceQueryType.QueryAllColumns;
            this.selectColumns = genSelectedColumns(columnMap).toArray(new SelectColumn[0]);
        } else {
            this.selectColumns = selectColumns;
        }
        String SQL = genSQL(queryType, columns, filter, limit);
        if (queryType == StarRocksSourceQueryType.QueryCount) {
            this.dataCount = StarRocksSourceCommonFunc.getQueryCount(this.sourceOptions, SQL);
        } else {
            this.queryInfo = getQueryInfo(SQL);
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
        }
        return SQL;
    }

    private List<SelectColumn> genSelectedColumns(Map<String, ColunmRichInfo> columnMap) {
        List<SelectColumn> selectedColumns = new ArrayList<>();
        // user selected colums from sourceOptions
        String selectColumnString = sourceOptions.getColumns();
        if (selectColumnString.equals("")) {
            // select *
            for (int i = 0; i < colunmRichInfos.size(); i ++ ) {
                selectedColumns.add(new SelectColumn(colunmRichInfos.get(i).getColumnName(), i));
            }
        } else {
            String[] oPcolumns = selectColumnString.split(",");
            for (int i = 0; i < oPcolumns.length; i ++) {
                String cName = oPcolumns[i].trim();
                if (!columnMap.containsKey(cName)) {
                    throw new RuntimeException("Colunm not found in the table schema");
                }
                ColunmRichInfo colunmRichInfo = columnMap.get(cName);
                selectedColumns.add(new SelectColumn(colunmRichInfo.getColumnName(), colunmRichInfo.getColunmIndexInSchema()));
            }
        }
        return selectedColumns;
    }

    private Map<String, ColunmRichInfo> genColumnMap(TableSchema flinkSchema) {

        Map<String, ColunmRichInfo> columnMap = new HashMap<>();
        List<TableColumn> flinkColumns = flinkSchema.getTableColumns();
        for (int i = 0; i < flinkColumns.size(); i++) {
            TableColumn column = flinkColumns.get(i);
            ColunmRichInfo colunmRichInfo = new ColunmRichInfo(column.getName(), i, column.getType());
            columnMap.put(column.getName(), colunmRichInfo);
        }
        return columnMap;
    }

    private QueryInfo getQueryInfo(String SQL) {

        StarRocksQueryPlanVisitor starRocksQueryPlanVisitor = new StarRocksQueryPlanVisitor(sourceOptions);
        QueryInfo queryInfo = null;
        try {
            queryInfo = starRocksQueryPlanVisitor.getQueryInfo(SQL);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get queryInfo:" + e.getMessage());
        }
        return queryInfo;
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
