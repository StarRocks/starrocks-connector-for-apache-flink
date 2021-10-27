package com.starrocks.connector.flink.table;

import com.starrocks.connector.flink.exception.StarRocksException;
import com.starrocks.connector.flink.source.Const;
import com.starrocks.connector.flink.source.QueryBeXTablets;
import com.starrocks.connector.flink.source.QueryInfo;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StarRocksRowDataInputFormat extends RichInputFormat<RowData, StarRocksTableInputSplit> implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksRowDataInputFormat.class);

    private final StarRocksSourceOptions sourceOptions;
    private final QueryInfo queryInfo;
    private StarRocksSourceDataReader dataReader;


    public StarRocksRowDataInputFormat(StarRocksSourceOptions sourceOptions, QueryInfo queryInfo) {
        this.sourceOptions = sourceOptions;
        this.queryInfo = queryInfo;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return baseStatistics;
    }

    @Override
    public StarRocksTableInputSplit[] createInputSplits(int i) throws IOException {

        List<StarRocksTableInputSplit> list = new ArrayList<>();
        for (int x = 0; x < this.queryInfo.getBeXTablets().size(); x ++) {
            list.add(new StarRocksTableInputSplit(x, this.queryInfo.getBeXTablets().get(x)));
        }
        return list.toArray(new StarRocksTableInputSplit[0]);
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(StarRocksTableInputSplit[] starRocksTableInputSplits) {

        return new DefaultInputSplitAssigner(starRocksTableInputSplits);
    }

    @Override
    public void open(StarRocksTableInputSplit starRocksTableInputSplit) {

        QueryBeXTablets queryBeXTablets = starRocksTableInputSplit.getQueryBeXTablets();
        String beNode[] = queryBeXTablets.getBeNode().split(":");
        String ip = beNode[0];
        int port = Integer.parseInt(beNode[1]);
        int socketTimeout = this.sourceOptions.getBeSocketTimeout() != null ?
                Integer.parseInt(this.sourceOptions.getBeSocketTimeout()) : Const.DEFAULT_BE_SOCKET_TIMEOUT;
        int connectTimeout = this.sourceOptions.getBeConnectTimeout() != null ?
                Integer.parseInt(this.sourceOptions.getBeConnectTimeout()) : Const.DEFAULT_BE_CONNECT_TIMEOUT;
        try {
            this.dataReader = new StarRocksSourceDataReader(ip, port, socketTimeout, connectTimeout);
        } catch (StarRocksException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
        int batchSize = this.sourceOptions.getBatchSize() != null ?
                Integer.parseInt(this.sourceOptions.getBatchSize()) : Const.DEFAULT_BATCH_SIZE;
        int queryTimeout = this.sourceOptions.getQueryTimeout() != null ?
                Integer.parseInt(this.sourceOptions.getQueryTimeout()) : Const.DEFAULT_QUERY_TIMEOUT;
        int memLimit = this.sourceOptions.getMemLimit() != null ?
                Integer.parseInt(this.sourceOptions.getMemLimit()) : Const.DEFAULT_MEM_LIMIT;

        try {
            this.dataReader.openScanner(
                    queryBeXTablets.getTabletIds(),
                    this.queryInfo.getQueryPlan().getOpaqued_query_plan(),
                    sourceOptions.getDatabaseName(),
                    sourceOptions.getTableName(),
                    batchSize, queryTimeout, memLimit,
                    sourceOptions.getUsername(),
                    sourceOptions.getPassword());
        } catch (StarRocksException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }

        try {
            this.dataReader.startToRead();
        } catch (StarRocksException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !this.dataReader.hasNext();
    }

    @Override
    public RowData nextRecord(RowData rowData) throws IOException {
        if (!this.dataReader.hasNext()) {
            return null;
        }
        List<Object> row = null;
        try {
            row = this.dataReader.getNext();
        } catch (StarRocksException e) {
            e.getMessage();
            LOG.error(e.getMessage());
        } 
        GenericRowData genericRowData = new GenericRowData(row.size());
        for (int i = 0; i < row.size(); i++) {
            genericRowData.setField(i, row.get(i));
        }
        return genericRowData;
    }

    @Override
    public void close() throws IOException {
        try {
            this.dataReader.close();
        } catch (StarRocksException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }
}
