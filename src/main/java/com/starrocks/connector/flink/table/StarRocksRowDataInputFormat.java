package com.starrocks.connector.flink.table;


import com.starrocks.connector.flink.exception.HttpException;
import com.starrocks.connector.flink.exception.StarRocksException;
import com.starrocks.connector.flink.manager.StarRocksSourceInfoVisitor;
import com.starrocks.connector.flink.source.Const;
import com.starrocks.connector.flink.source.QueryBeXTablets;
import com.starrocks.connector.flink.source.QueryInfo;
import com.starrocks.connector.flink.source.SelectColumn;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StarRocksRowDataInputFormat extends RichInputFormat<RowData, StarRocksTableInputSplit> implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksRowDataInputFormat.class);

    private final StarRocksSourceOptions sourceOptions;
    private final DataType[] flinkDataTypes;
    private final long limit;
    private final String filter;
    private final String columns;
    private final SelectColumn[] selectColumns;

    private StarRocksSourceDataReader dataReader;
    private StarRocksSourceInfoVisitor infoVisitor;

    private QueryInfo queryInfo;

    public StarRocksRowDataInputFormat(StarRocksSourceOptions sourceOptions, DataType[] flinkDataTypes, long limit, String filter, String columns, SelectColumn[] selectColumns) {

        this.sourceOptions = sourceOptions;
        this.flinkDataTypes = flinkDataTypes;
        this.limit = limit;
        this.filter = filter;
        this.columns = columns;
        this.selectColumns = selectColumns;

        this.infoVisitor = new StarRocksSourceInfoVisitor(sourceOptions);
    }

    @Override
    public void configure(Configuration configuration) {}

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return baseStatistics;
    }

    @Override
    public StarRocksTableInputSplit[] createInputSplits(int i) throws IOException {

        try {
            this.queryInfo = infoVisitor.getQueryInfo(this.columns, this.filter, this.limit);
        } catch (HttpException | StarRocksException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
        List<StarRocksTableInputSplit> list = new ArrayList<>();
        for (int x = 0; x < queryInfo.getBeXTablets().size(); x ++) {
            list.add(new StarRocksTableInputSplit(x, queryInfo));
        }
        return list.toArray(new StarRocksTableInputSplit[0]);
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(StarRocksTableInputSplit[] starRocksTableInputSplits) {

        return new DefaultInputSplitAssigner(starRocksTableInputSplits);
    }

    @Override
    public void open(StarRocksTableInputSplit starRocksTableInputSplit) {

        QueryBeXTablets queryBeXTablets = starRocksTableInputSplit.getBeXTablets();
        String beNode[] = queryBeXTablets.getBeNode().split(":");
        String ip = beNode[0];
        int port = Integer.parseInt(beNode[1]);
        int socketTimeout = this.sourceOptions.getConnectTimeoutMs() != null ?
                Integer.parseInt(this.sourceOptions.getConnectTimeoutMs()) : Const.DEFAULT_BE_SOCKET_TIMEOUT;
        int connectTimeout = this.sourceOptions.getConnectTimeoutMs() != null ?
                Integer.parseInt(this.sourceOptions.getConnectTimeoutMs()) : Const.DEFAULT_BE_CONNECT_TIMEOUT;
        try {
            this.dataReader = new StarRocksSourceDataReader(ip, port, socketTimeout, connectTimeout, flinkDataTypes, selectColumns);
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
                    starRocksTableInputSplit.getQueryInfo().getQueryPlan().getOpaqued_query_plan(),
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

    /** Builder for {@link StarRocksRowDataInputFormat}. */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private StarRocksSourceOptions sourceOptions;
        private DataType[] flinkDataTypes;
        private long limit;
        private String columns;
        private SelectColumn[] selectColumns;
        private String filter;
        
        public Builder setSourceOptions(StarRocksSourceOptions sourceOptions) {
            this.sourceOptions = sourceOptions;
            return this;
        }
        public Builder setFlinkDataTypes(DataType[] flinkDataTypes) {
            this.flinkDataTypes = flinkDataTypes;
            return this;
        }
        public Builder setLimit(long limit) {
            this.limit = limit;
            return this;
        }
        public Builder setColumns(String columns) {
            this.columns = columns;
            return this;
        }
        public Builder setSelectColumns(SelectColumn[] selectColumns) {
            this.selectColumns = selectColumns;
            return this;
        }
        public Builder setFilter(String filter) {
            this.filter = filter;
            return this;
        }
        

        public Builder() {}

        public StarRocksRowDataInputFormat build() {
            if (this.sourceOptions == null) {
                throw new NullPointerException("No query supplied");
            }
            return new StarRocksRowDataInputFormat(this.sourceOptions, this.flinkDataTypes, this.limit, this.filter, this.columns, this.selectColumns);
        }
    }
}
