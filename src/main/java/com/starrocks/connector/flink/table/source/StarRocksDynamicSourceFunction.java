package com.starrocks.connector.flink.table.source;

import com.starrocks.connector.flink.manager.StarRocksFeHttpVisitor;
import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarRocksDynamicSourceFunction extends RichParallelSourceFunction<List<?>> implements ResultTypeQueryable<List<?>> {

    private final StarRocksSourceOptions sourceOptions;
    private final QueryInfo queryInfo;
    private final SelectColumn[] selectColumns;
    private final List<ColunmRichInfo> colunmRichInfos;
    private List<StarRocksSourceBeReader> beReaderList;
    

    public StarRocksDynamicSourceFunction(StarRocksSourceOptions sourceOptions, TableSchema flinkSchema) {
        

        List<ColunmRichInfo> colunmRichInfos = new ArrayList<>();
        List<TableColumn> flinkColumns = flinkSchema.getTableColumns();
        Map<String, ColunmRichInfo> columnMap = new HashMap<>();
        for (int i = 0; i < flinkColumns.size(); i++) {
            TableColumn column = flinkColumns.get(i);
            ColunmRichInfo colunmRichInfo = new ColunmRichInfo(column.getName(), i, column.getType());
            colunmRichInfos.add(colunmRichInfo);
            columnMap.put(column.getName(), colunmRichInfo);
        }

        List<SelectColumn> selectedColumns = new ArrayList<>();
        // user selected colums from sourceOptions
        String selectColumnString = sourceOptions.getColumns();
        if (selectColumnString.equals("")) {
            // select *
            for (int i = 0; i < colunmRichInfos.size(); i ++ ) {
                selectedColumns.add(new SelectColumn(colunmRichInfos.get(i).getColumnName(), i, true));
            }
        } else {
            String[] oPcolumns = selectColumnString.split(",");
            for (int i = 0; i < oPcolumns.length; i ++) {
                String cName = oPcolumns[i].trim();
                if (!columnMap.containsKey(cName)) {
                    throw new RuntimeException("Colunm not found in the table schema");
                }
                ColunmRichInfo colunmRichInfo = columnMap.get(cName);
                selectedColumns.add(new SelectColumn(colunmRichInfo.getColumnName(), colunmRichInfo.getColunmIndexInSchema(), true));
            }
        }

        StarRocksFeHttpVisitor starRocksFeHttpVisitor = new StarRocksFeHttpVisitor(sourceOptions);
        QueryInfo queryInfo = null;
        try {
            queryInfo = starRocksFeHttpVisitor.getQueryInfo(sourceOptions);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get queryInfo:" + e.getMessage());
        
        }
        this.sourceOptions = sourceOptions;
        this.queryInfo = queryInfo;
        this.selectColumns = selectedColumns.toArray(new SelectColumn[0]);
        this.colunmRichInfos = colunmRichInfos;     
        this.beReaderList = new ArrayList<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        List<List<QueryBeXTablets>> lists = StarRocksDataSplitForParallelism.splitQueryBeXTablets(getRuntimeContext().getNumberOfParallelSubtasks(), queryInfo);
        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        lists.get(subTaskId).forEach(beXTablets -> {
            StarRocksSourceBeReader beReader = StarRocksDataSplitForParallelism.genBeReaderFromQueryBeXTablets(beXTablets, queryInfo, colunmRichInfos, selectColumns, sourceOptions);
            beReader.openScanner(beXTablets.getTabletIds(), queryInfo.getQueryPlan().getOpaqued_query_plan(), sourceOptions);
            this.beReaderList.add(beReader);
        });
    }

    @Override
    public void run(SourceContext<List<?>> sourceContext) {

        this.beReaderList.forEach(beReader -> {
            beReader.startToRead();
            while (beReader.hasNext()) {
                List<Object> row = beReader.getNext();
                sourceContext.collect(row);
            }
        });
    }

    @Override
    public void cancel() {
        this.beReaderList.forEach(beReader -> {
            if (beReader != null) {
                beReader.close();
            }
        });
    }

    @Override
    public TypeInformation<List<?>> getProducedType() {
        return TypeInformation.of(new TypeHint<List<?>>() {});
    }
}
