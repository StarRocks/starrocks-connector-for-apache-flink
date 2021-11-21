package com.starrocks.connector.flink.table.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;

public class StarRocksDataSplitForParallelism {
    
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

    public static StarRocksSourceBeReader genBeReaderFromQueryBeXTablets(QueryBeXTablets qBeXTablets, QueryInfo queryInfo,
                                                                            List<ColunmRichInfo> colunmRichInfos, 
                                                                            SelectColumn[] selectColumns, 
                                                                            StarRocksSourceOptions sourceOptions) {

        StarRocksSourceBeReader berReader = new StarRocksSourceBeReader(qBeXTablets.getBeNode(), colunmRichInfos, selectColumns, sourceOptions);
        return berReader;
    }
}
