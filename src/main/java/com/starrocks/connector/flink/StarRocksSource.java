package com.starrocks.connector.flink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.starrocks.connector.flink.source.ColunmRichInfo;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.source.SelectColumn;
import com.starrocks.connector.flink.table.StarRocksDynamicSourceFunction;
import com.starrocks.connector.flink.table.StarRocksSourceOptions;

import org.apache.flink.table.api.DataTypes;
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
    public static StarRocksDynamicSourceFunction source(StarRocksSourceOptions sourceOptions, QueryInfo queryInfo, TableSchema flinkSchema) {
        
        List<ColunmRichInfo> colunmRichInfos = new ArrayList<>();
        List<TableColumn> columns = flinkSchema.getTableColumns();
        Map<String, ColunmRichInfo> columnMap = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            TableColumn column = columns.get(i);
            ColunmRichInfo colunmRichInfo = new ColunmRichInfo(column.getName(), i, column.getType());
            colunmRichInfos.add(colunmRichInfo);
            columnMap.put(column.getName(), colunmRichInfo);
        }

        List<SelectColumn> selectColumns = new ArrayList<>();
        String SelectColumnString = sourceOptions.getColumns();
        if (SelectColumnString.equals("")) {
            for (int i = 0; i < colunmRichInfos.size(); i ++ ) {
                selectColumns.add(new SelectColumn(colunmRichInfos.get(i).getColumnName(), i, true));
            }
        } else {
            String[] oPcolumns = SelectColumnString.split(",");
            for (int i = 0; i < oPcolumns.length; i ++) {
                String cName = oPcolumns[i];
                if (!columnMap.containsKey(cName)) {
                    throw new RuntimeException("Colunm not found in the table schema");
                }
                ColunmRichInfo colunmRichInfo = columnMap.get(cName);
                selectColumns.add(new SelectColumn(colunmRichInfo.getColumnName(), colunmRichInfo.getColunmIndexInSchema(), true));
            }
        }
        return new StarRocksDynamicSourceFunction(sourceOptions, queryInfo, flinkSchema, selectColumns.toArray(new SelectColumn[0]), colunmRichInfos);
    }
}
