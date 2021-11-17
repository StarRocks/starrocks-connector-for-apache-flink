package com.starrocks.connector.flink.table;

import com.starrocks.connector.flink.exception.StarRocksException;
import com.starrocks.connector.flink.source.ColunmRichInfo;
import com.starrocks.connector.flink.source.QueryBeXTablets;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.source.SelectColumn;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

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
    

    public StarRocksDynamicSourceFunction(StarRocksSourceOptions sourceOptions, QueryInfo queryInfo,
                                            SelectColumn[] selectColumns, List<ColunmRichInfo> colunmRichInfos) {
        this.sourceOptions = sourceOptions;
        this.queryInfo = queryInfo;
        this.selectColumns = selectColumns;
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
                    StarRocksSourceBeReader beReader = null;
                    try {
                        beReader = genBeReaderFromQueryBeXTablets(queryBeXTablets);
                    } catch (StarRocksException e) {
                        throw new RuntimeException("Failed to create beReader:" + e.getMessage());
                    }
                    this.beReaders.add(beReader);
                });
            }            
            return;
        }
        
    }

    private StarRocksSourceBeReader genBeReaderFromQueryBeXTablets(QueryBeXTablets qBeXTablets) throws StarRocksException {

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
