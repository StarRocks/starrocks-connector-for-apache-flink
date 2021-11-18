package com.starrocks.connector.flink.table;

import com.starrocks.connector.flink.manager.StarRocksFeHttpVisitor;
import com.starrocks.connector.flink.source.ColunmRichInfo;
import com.starrocks.connector.flink.source.QueryBeXTablets;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.source.SelectColumn;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarRocksDynamicSourceFunction extends RichParallelSourceFunction<List<?>> implements ResultTypeQueryable<List<?>> {

    private final StarRocksSourceOptions sourceOptions;
    private final QueryInfo queryInfo;
    private final SelectColumn[] selectColumns;
    private List<StarRocksSourceBeReader> beReaders;
    private final List<ColunmRichInfo> colunmRichInfos;
    

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
        this.beReaders = new ArrayList<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);

        int subTaskCount = getRuntimeContext().getNumberOfParallelSubtasks();
        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        int queryInfoBeXTabletsListCount = queryInfo.getBeXTablets().size();

        if (subTaskCount == queryInfoBeXTabletsListCount) {
            QueryBeXTablets queryBeXTablets = queryInfo.getBeXTablets().get(subTaskId);
            StarRocksSourceBeReader beReader = genBeReaderFromQueryBeXTablets(queryBeXTablets);
            this.beReaders.add(beReader);
            return;
        }

        if (subTaskCount < queryInfoBeXTabletsListCount) {
            for (int i = 0; i < queryInfoBeXTabletsListCount; i ++) {
                if (i%subTaskCount == subTaskId) {        
                    QueryBeXTablets queryBeXTablets = queryInfo.getBeXTablets().get(i);
                    StarRocksSourceBeReader beReader = genBeReaderFromQueryBeXTablets(queryBeXTablets);
                    this.beReaders.add(beReader);
                }
            }
            return;
        }

        if (subTaskCount > queryInfoBeXTabletsListCount) {

            List<QueryBeXTablets> totalTablets = new ArrayList<>();
            queryInfo.getBeXTablets().forEach(beXTablets -> {
                beXTablets.getTabletIds().forEach(tabletId -> {
                    QueryBeXTablets beXOnlyOneTablets = new QueryBeXTablets(beXTablets.getBeNode(), Arrays.asList(tabletId));
                    totalTablets.add(beXOnlyOneTablets);
                });
            });

            double x = (double)totalTablets.size()/subTaskCount;
            if (x <= 1) {
                if (subTaskId < totalTablets.size()) {
                    StarRocksSourceBeReader beReader = genBeReaderFromQueryBeXTablets(totalTablets.get(subTaskId));
                    this.beReaders.add(beReader);
                }
            } 
            if (x > 1) {
                long newx = Math.round(x);
                int start = (int)(subTaskId * newx);
                int end = start + (int)newx;
                List<QueryBeXTablets> curBxTs = new ArrayList<>();
                if (start >= totalTablets.size()) {
                    return;
                }
                if (end >= totalTablets.size()) {
                    end = totalTablets.size();
                }
                
                curBxTs = totalTablets.subList(start, end);
                Map<String, List<Long>> beXTabletsMap = new HashMap<>();
                curBxTs.forEach(curBxT -> {
                    List<Long> tablets = new ArrayList<>(); 
                    if (beXTabletsMap.containsKey(curBxT.getBeNode())) {
                        tablets = beXTabletsMap.get(curBxT.getBeNode());
                    } else {
                        tablets = new ArrayList<>();
                    }
                    tablets.add(curBxT.getTabletIds().get(0));
                    beXTabletsMap.put(curBxT.getBeNode(), tablets);
                });
                beXTabletsMap.forEach((beNode, tabletIds) -> {
                    QueryBeXTablets queryBeXTablets = new QueryBeXTablets(beNode, tabletIds);
                    StarRocksSourceBeReader beReader = genBeReaderFromQueryBeXTablets(queryBeXTablets);
                    this.beReaders.add(beReader);
                });
            }            
            return;
        }
        
    }

    private StarRocksSourceBeReader genBeReaderFromQueryBeXTablets(QueryBeXTablets qBeXTablets) {

        String beNode[] = qBeXTablets.getBeNode().split(":");
        String ip = beNode[0];
        int port = Integer.parseInt(beNode[1]);
        StarRocksSourceBeReader berReader = null;
        berReader = new StarRocksSourceBeReader(ip, port, colunmRichInfos, selectColumns, this.sourceOptions);
        berReader.openScanner(qBeXTablets.getTabletIds(), 
            this.queryInfo.getQueryPlan().getOpaqued_query_plan(), this.sourceOptions);
        return berReader;
    }

    @Override
    public void run(SourceContext<List<?>> sourceContext) {

        this.beReaders.forEach(beReader -> {
            beReader.startToRead();
            while (beReader.hasNext()) {
                List<Object> row = beReader.getNext();
                sourceContext.collect(row);
            }
        });
    }

    @Override
    public void cancel() {
        this.beReaders.forEach(beReader -> {
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
