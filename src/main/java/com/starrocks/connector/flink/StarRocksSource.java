package com.starrocks.connector.flink;

import com.starrocks.connector.flink.exception.StarRocksException;
import com.starrocks.connector.flink.related.DataReader;
import com.starrocks.connector.flink.related.Const;
import com.starrocks.connector.flink.related.QueryBeXTablets;
import com.starrocks.connector.flink.related.QueryInfo;
import com.starrocks.connector.flink.table.StarRocksSourceOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.List;

public class StarRocksSource extends RichParallelSourceFunction<List<?>> implements ResultTypeQueryable<List<?>> {

    private final StarRocksSourceOptions sourceOptions;
    private final QueryInfo queryInfo;
    private QueryBeXTablets queryBeXTablets;

    private DataReader dataReader;

    public StarRocksSource(StarRocksSourceOptions sourceOptions, QueryInfo queryInfo) {
        this.sourceOptions = sourceOptions;
        this.queryInfo = queryInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.queryBeXTablets = queryInfo.getBeXTablets().get(subTaskId);
        String beNode[] = this.queryBeXTablets.getBeNode().split(":");
        String ip = beNode[0];
        int port = Integer.parseInt(beNode[1]);
        int socketTimeout = this.sourceOptions.getBeSocketTimeout() != null ?
                Integer.parseInt(this.sourceOptions.getBeSocketTimeout()) : Const.DEFAULT_BE_SOCKET_TIMEOUT;
        int connectTimeout = this.sourceOptions.getBeConnectTimeout() != null ?
                Integer.parseInt(this.sourceOptions.getBeConnectTimeout()) : Const.DEFAULT_BE_CONNECT_TIMEOUT;
        this.dataReader = new DataReader(ip, port, socketTimeout, connectTimeout);

        int batchSize = this.sourceOptions.getBatchSize() != null ?
                Integer.parseInt(this.sourceOptions.getBatchSize()) : Const.DEFAULT_BATCH_SIZE;
        int queryTimeout = this.sourceOptions.getQueryTimeout() != null ?
                Integer.parseInt(this.sourceOptions.getQueryTimeout()) : Const.DEFAULT_QUERY_TIMEOUT;
        int memLimit = this.sourceOptions.getMemLimit() != null ?
                Integer.parseInt(this.sourceOptions.getMemLimit()) : Const.DEFAULT_MEM_LIMIT;

        this.dataReader.openScanner(
                this.queryBeXTablets.getTabletIds(),
                this.queryInfo.getQueryPlan().getOpaqued_query_plan(),
                sourceOptions.getDatabaseName(),
                sourceOptions.getTableName(),
                batchSize, queryTimeout, memLimit,
                sourceOptions.getUsername(),
                sourceOptions.getPassword());
    }

    @Override
    public void run(SourceContext<List<?>> sourceContext) throws Exception {
        boolean needRead = this.dataReader.startToRead();
        while (!this.dataReader.jobDone() && needRead) {
            List<Object> row = this.dataReader.getDataQueue().take();
            sourceContext.collect(row);
        }
    }

    @Override
    public void cancel() {
        try {
            this.dataReader.close();
        } catch (StarRocksException e) {
            e.printStackTrace();
        }
    }

    @Override
    public TypeInformation<List<?>> getProducedType() {
        return null;
    }
}
