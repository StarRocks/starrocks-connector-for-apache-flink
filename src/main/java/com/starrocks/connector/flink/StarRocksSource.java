package com.starrocks.connector.flink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.starrocks.connector.flink.exception.HttpException;
import com.starrocks.connector.flink.exception.StarRocksException;
import com.starrocks.connector.flink.manager.StarRocksFeHttpVisitor;
import com.starrocks.connector.flink.source.ColunmRichInfo;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.source.SelectColumn;
import com.starrocks.connector.flink.table.StarRocksDynamicSourceFunction;
import com.starrocks.connector.flink.table.StarRocksSourceOptions;


import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;


public class StarRocksSource {
    
    /**
     * Create a StarRocks DataStream source.
     *
     * @param sourceOptions     StarRocksSourceOptions as the document listed, such as http-nodes, load-url, batch size and maximum retries
     * @param queryInfo         QueryInfo includes some infomations, such as query_plan, be_nodes, you can get by use StarRocksSourceManager.getQueryInfo
     * @param flinkSchema       FlinkSchema
     */
    public static StarRocksDynamicSourceFunction source(StarRocksSourceOptions sourceOptions, TableSchema flinkSchema) {
        
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
        } catch (IOException | HttpException | StarRocksException e) {
            throw new RuntimeException("Failed to get queryInfo:" + e.getMessage());
        
        }
        return new StarRocksDynamicSourceFunction(sourceOptions, queryInfo, selectedColumns.toArray(new SelectColumn[0]), colunmRichInfos);
    }
}
