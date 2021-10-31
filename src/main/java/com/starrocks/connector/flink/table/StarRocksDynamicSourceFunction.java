package com.starrocks.connector.flink.table;

import com.starrocks.connector.flink.exception.StarRocksException;
import com.starrocks.connector.flink.source.Const;
import com.starrocks.connector.flink.source.QueryBeXTablets;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.source.SelectColumn;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import java.util.List;

public class StarRocksDynamicSourceFunction extends RichParallelSourceFunction<List<?>> implements ResultTypeQueryable<List<?>> {

    private final StarRocksSourceOptions sourceOptions;
    private final QueryInfo queryInfo;
    private final DataType[] datatypes;
    private final SelectColumn[] selectColumns;
    private QueryBeXTablets queryBeXTablets;

    private StarRocksSourceDataReader dataReader;

    public StarRocksDynamicSourceFunction(StarRocksSourceOptions sourceOptions, QueryInfo queryInfo, TableSchema fSchema, SelectColumn[] selectColumns) {
        this.sourceOptions = sourceOptions;
        this.queryInfo = queryInfo;
        this.datatypes = fSchema.getFieldDataTypes();
        this.selectColumns = selectColumns;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.queryBeXTablets = queryInfo.getBeXTablets().get(subTaskId);
        String beNode[] = this.queryBeXTablets.getBeNode().split(":");
        String ip = beNode[0];
        int port = Integer.parseInt(beNode[1]);
        int socketTimeout = this.sourceOptions.getConnectTimeoutMs() != null ?
                Integer.parseInt(this.sourceOptions.getConnectTimeoutMs()) : Const.DEFAULT_BE_SOCKET_TIMEOUT;
        int connectTimeout = this.sourceOptions.getConnectTimeoutMs() != null ?
                Integer.parseInt(this.sourceOptions.getConnectTimeoutMs()) : Const.DEFAULT_BE_CONNECT_TIMEOUT;
        this.dataReader = new StarRocksSourceDataReader(ip, port, socketTimeout, connectTimeout, this.datatypes, selectColumns);

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
        this.dataReader.startToRead();
        while (this.dataReader.hasNext()) {
            List<Object> row = this.dataReader.getNext();
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
        return TypeInformation.of(new TypeHint<List<?>>() {});
    }
}
