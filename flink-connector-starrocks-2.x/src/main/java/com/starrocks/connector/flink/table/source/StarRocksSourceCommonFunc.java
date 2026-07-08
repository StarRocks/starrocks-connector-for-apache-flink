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

import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.manager.StarRocksQueryPlanVisitor;
import com.starrocks.connector.flink.manager.StarRocksQueryVisitor;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class StarRocksSourceCommonFunc {

    public static List<List<QueryBeXTablets>> splitQueryBeXTablets(int subTaskCount, QueryInfo queryInfo) {
        List<List<QueryBeXTablets>> curBeXTabletList = new ArrayList<>();
        for (int i = 0; i < subTaskCount; i ++) {
            curBeXTabletList.add(new ArrayList<>());
        }
        int beXTabletsListCount = queryInfo.getBeXTablets().size();
        if (subTaskCount == beXTabletsListCount) {
            for (int i = 0; i < beXTabletsListCount; i ++) {
                curBeXTabletList.set(i, Collections.singletonList(queryInfo.getBeXTablets().get(i)));
            }
            return curBeXTabletList;
        } 
        if (subTaskCount < beXTabletsListCount) {
            for (int i = 0; i < beXTabletsListCount; i ++) {
                List<QueryBeXTablets> tList = curBeXTabletList.get(i%subTaskCount);
                tList.add(queryInfo.getBeXTablets().get(i));
                curBeXTabletList.set(i%subTaskCount, tList);
            }
            return curBeXTabletList;
        } 
        List<QueryBeXTablets> beWithSingleTabletList = new ArrayList<>();
        queryInfo.getBeXTablets().forEach(beXTablets -> {
            beXTablets.getTabletIds().forEach(tabletId -> {
                QueryBeXTablets beXOnlyOneTablets = new QueryBeXTablets(beXTablets.getBeNode(), Collections.singletonList(tabletId));
                beWithSingleTabletList.add(beXOnlyOneTablets);
            });
        });
        double x = (double)beWithSingleTabletList.size()/subTaskCount;
        if (x <= 1) {
            for (int i = 0; i < beWithSingleTabletList.size(); i ++) {
                curBeXTabletList.set(i, Collections.singletonList(beWithSingleTabletList.get(i)));
            }
            return curBeXTabletList;
        } 
        long newx = Math.round(x);
        for (int i = 0; i < subTaskCount; i ++) {
            int start = (int)(i * newx);
            int end = start + (int)newx;
            List<QueryBeXTablets> curBxTs = new ArrayList<>();
            if (start >= beWithSingleTabletList.size()) {
                continue;
            }
            if (end >= beWithSingleTabletList.size()) {
                end = beWithSingleTabletList.size();
            }
            if (i == subTaskCount - 1) {
                end = beWithSingleTabletList.size();
            }
            curBxTs = beWithSingleTabletList.subList(start, end);
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
            List<QueryBeXTablets> tList = new ArrayList<>();
            beXTabletsMap.forEach((beNode, tabletIds) -> {
                QueryBeXTablets queryBeXTablets = new QueryBeXTablets(beNode, tabletIds);
                tList.add(queryBeXTablets);
            });
            curBeXTabletList.set(i, tList);
        }
        return curBeXTabletList;
    }

    // public static void validateTableStructure(StarRocksSourceOptions sourceOptions, ResolvedSchema flinkSchema) {

    //     StarRocksQueryVisitor starrocksQueryVisitor = genStarRocksQueryVisitor(sourceOptions);
    //     List<Map<String, Object>> rows = starrocksQueryVisitor.getTableColumnsMetaData();
    //     List<Column> flinkCols = flinkSchema.getColumns();
    //     if (flinkCols.size() != rows.size()) {
    //         throw new RuntimeException("Flink columns size not equal StarRocks columns");
    //     }
    // }


    public static Long getQueryCount(StarRocksSourceOptions sourceOptions, String SQL) {
        StarRocksJdbcConnectionOptions jdbcOptions = new StarRocksJdbcConnectionOptions(
            sourceOptions.getJdbcUrl(), sourceOptions.getUsername(), sourceOptions.getPassword()
        );
        StarRocksJdbcConnectionProvider jdbcConnProvider = new StarRocksJdbcConnectionProvider(jdbcOptions);
        StarRocksQueryVisitor starrocksQueryVisitor = new StarRocksQueryVisitor(
            jdbcConnProvider, sourceOptions.getDatabaseName(), sourceOptions.getTableName()
        );
        return starrocksQueryVisitor.getQueryCount(SQL);
    }

    public static Map<String, ColumnRichInfo> genColumnMap(ResolvedSchema flinkSchema) {
        Map<String, ColumnRichInfo> columnMap = new HashMap<>();
        List<Column> flinkColumns = flinkSchema.getColumns();
        int physicalIndex = 0;
        for (int i = 0; i < flinkColumns.size(); i++) {
            Column column = flinkColumns.get(i);
            // Only include physical columns — computed/virtual columns don't exist in StarRocks
            if (!column.isPhysical()) {
                continue;
            }
            ColumnRichInfo columnRichInfo = new ColumnRichInfo(column.getName(), physicalIndex, column.getDataType());
            columnMap.put(column.getName(), columnRichInfo);
            physicalIndex++;
        }
        return columnMap;
    }

    public static List<ColumnRichInfo> genColumnRichInfo(Map<String, ColumnRichInfo> columnMap) {
        return columnMap.values().stream().sorted(Comparator.comparing(ColumnRichInfo::getColumnIndexInSchema)).collect(Collectors.toList());
    }

    public static SelectColumn[] genSelectedColumns(Map<String, ColumnRichInfo> columnMap,
                                                    StarRocksSourceOptions sourceOptions, 
                                                    List<ColumnRichInfo> columnRichInfos) {
        List<SelectColumn> selectedColumns = new ArrayList<>();
        // user selected columns from sourceOptions
        String selectColumnString = sourceOptions.getColumns();
        if ("".equals(selectColumnString)) {
            // select *
            for (int i = 0; i < columnRichInfos.size(); i ++ ) {
                selectedColumns.add(new SelectColumn(columnRichInfos.get(i).getColumnName(), i));
            }
        } else {
            String[] oPColumns = selectColumnString.split(",");
            for (String oPColumn : oPColumns) {
                String cName = oPColumn.trim();
                if (!columnMap.containsKey(cName)) {
                    throw new RuntimeException("column not found in the table schema");
                }
                ColumnRichInfo columnRichInfo = columnMap.get(cName);
                selectedColumns.add(new SelectColumn(columnRichInfo.getColumnName(), columnRichInfo.getColumnIndexInSchema()));
            }
        }
        return selectedColumns.toArray(new SelectColumn[0]);
    }

    public static String buildSQL(
            StarRocksSourceQueryType queryType,
            SelectColumn[] selectColumns,
            String[] columnNames,
            StarRocksSourceOptions sourceOptions,
            String filter) {
        StringBuilder sqlSb = new StringBuilder("select ");
        if (queryType == StarRocksSourceQueryType.QueryCount) {
            sqlSb.append("count(*)");
        } else if (selectColumns != null && selectColumns.length > 0) {
            for (int i = 0; i < selectColumns.length; i++) {
                if (i > 0) {
                    sqlSb.append(",");
                }
                sqlSb.append("`").append(selectColumns[i].getColumnName()).append("`");
            }
        } else if (columnNames != null && columnNames.length > 0) {
            for (int i = 0; i < columnNames.length; i++) {
                if (i > 0) {
                    sqlSb.append(",");
                }
                sqlSb.append("`").append(columnNames[i]).append("`");
            }
        } else {
            sqlSb.append("*");
        }
        sqlSb.append(" from ");
        sqlSb.append("`").append(sourceOptions.getDatabaseName()).append("`");
        sqlSb.append(".");
        sqlSb.append("`").append(sourceOptions.getTableName()).append("`");
        if (filter != null && !filter.isEmpty()) {
            sqlSb.append(" where ").append(filter);
        }
        return sqlSb.toString();
    }

    public static QueryInfo getQueryInfo(StarRocksSourceOptions sourceOptions, String SQL) {
        StarRocksQueryPlanVisitor starRocksQueryPlanVisitor = new StarRocksQueryPlanVisitor(sourceOptions);
        QueryInfo queryInfo = null;
        try {
            queryInfo = starRocksQueryPlanVisitor.getQueryInfo(SQL);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get queryInfo:" + e.getMessage());
        }
        return queryInfo;
    }
}
