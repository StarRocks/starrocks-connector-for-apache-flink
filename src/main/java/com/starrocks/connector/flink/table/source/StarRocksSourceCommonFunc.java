package com.starrocks.connector.flink.table.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.manager.StarRocksQueryVisitor;
import com.starrocks.connector.flink.table.source.struct.Const;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.tools.DataUtil;

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;


public class StarRocksSourceCommonFunc {
    
    public static List<List<QueryBeXTablets>> splitQueryBeXTablets(int subTaskCount, QueryInfo queryInfo) {

        List<List<QueryBeXTablets>> curBeXTabletList = new ArrayList<>();
        for (int i = 0; i < subTaskCount; i ++) {
            curBeXTabletList.add(new ArrayList<>());
        }
        int beXTabletsListCount = queryInfo.getBeXTablets().size();
        if (subTaskCount == beXTabletsListCount) {
            for (int i = 0; i < beXTabletsListCount; i ++) {
                curBeXTabletList.set(i, Arrays.asList(queryInfo.getBeXTablets().get(i)));
            }
            return curBeXTabletList;
        } else if (subTaskCount < beXTabletsListCount) {
            for (int i = 0; i < beXTabletsListCount; i ++) {
                List<QueryBeXTablets> tList = curBeXTabletList.get(i%subTaskCount);
                tList.add(queryInfo.getBeXTablets().get(i));
                curBeXTabletList.set(i%subTaskCount, tList);
            }
            return curBeXTabletList;
        } else {
            List<QueryBeXTablets> beWithSingleTabletList = new ArrayList<>();
            queryInfo.getBeXTablets().forEach(beXTablets -> {
                beXTablets.getTabletIds().forEach(tabletId -> {
                    QueryBeXTablets beXOnlyOneTablets = new QueryBeXTablets(beXTablets.getBeNode(), Arrays.asList(tabletId));
                    beWithSingleTabletList.add(beXOnlyOneTablets);
                });
            });
            double x = (double)beWithSingleTabletList.size()/subTaskCount;
            if (x <= 1) {
                for (int i = 0; i < beWithSingleTabletList.size(); i ++) {
                    curBeXTabletList.set(i, Arrays.asList(beWithSingleTabletList.get(i)));
                }
            } 
            if (x > 1) {
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
            }
            return curBeXTabletList;
        }
    }


    private static StarRocksQueryVisitor genStarRocksQueryVisitor(StarRocksSourceOptions sourceOptions) {

        StarRocksJdbcConnectionProvider jdbcConnProvider;
        StarRocksQueryVisitor starrocksQueryVisitor;
        StarRocksJdbcConnectionOptions jdbcOptions = new StarRocksJdbcConnectionOptions(sourceOptions.getJdbcUrl(), sourceOptions.getUsername(), sourceOptions.getPassword());
        jdbcConnProvider = new StarRocksJdbcConnectionProvider(jdbcOptions);
        starrocksQueryVisitor = new StarRocksQueryVisitor(jdbcConnProvider, sourceOptions.getDatabaseName(), sourceOptions.getTableName());
        return starrocksQueryVisitor;
    }


    public static void validateTableStructure(StarRocksSourceOptions sourceOptions, TableSchema flinkSchema) {

        StarRocksQueryVisitor starrocksQueryVisitor = genStarRocksQueryVisitor(sourceOptions);
        List<Map<String, Object>> rows = starrocksQueryVisitor.getTableColumnsMetaData();
        List<TableColumn> flinkCols = flinkSchema.getTableColumns();
        if (flinkCols.size() != rows.size()) {
            throw new RuntimeException("Flink columns size not equal StarRocks columns");
        }
        for (int i = 0; i < rows.size(); i++) {
            String starrocksField = rows.get(i).get("COLUMN_NAME").toString();
            String starrocksType = rows.get(i).get("DATA_TYPE").toString().toUpperCase();
            String flinkField = flinkCols.get(i).getName();
            String flinkType = flinkCols.get(i).getType().toString();
            flinkType = DataUtil.ClearBracket(flinkType);
            starrocksType = DataUtil.ClearBracket(starrocksType);
            if (!starrocksField.equals(flinkField)) {
                throw new RuntimeException("Flink column name [" + flinkField + "] not equal StarRocks column name [" + starrocksField + "] at index " + i);
            }
            if (!Const.DataTypeRelationMap.containsKey(flinkType)) {
                throw new RuntimeException("FlinkType not found -> " + flinkType);
            }
            if (!Const.DataTypeRelationMap.get(flinkType).contains(starrocksType)) {
                throw new RuntimeException("Failed to convert " + starrocksType + " to " + flinkType);
            }
        }
    }

    public static int getQueryCount(StarRocksSourceOptions sourceOptions, String SQL) {
        StarRocksQueryVisitor starrocksQueryVisitor = genStarRocksQueryVisitor(sourceOptions);
        return starrocksQueryVisitor.getQueryCount(SQL);
    }

}
