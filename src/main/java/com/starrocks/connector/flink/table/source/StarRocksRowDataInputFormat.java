package com.starrocks.connector.flink.table.source;


import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.manager.StarRocksFeHttpVisitor;
import com.starrocks.connector.flink.manager.StarRocksQueryVisitor;
import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;

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
import java.util.Map;



public class StarRocksRowDataInputFormat extends RichInputFormat<RowData, StarRocksTableInputSplit> implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksRowDataInputFormat.class);

    private final StarRocksSourceOptions sourceOptions;
    
    private final long limit;
    private final String filter;
    private StarRocksSourceQueryType queryType;
    private final List<ColunmRichInfo> colunmRichInfos;

    private String columns;
    private SelectColumn[] selectColumns;

    private StarRocksSourceDataReader dataReader;
    private StarRocksFeHttpVisitor feHttpVisitor;

    private final StarRocksJdbcConnectionProvider jdbcConnProvider;
    private final StarRocksQueryVisitor starrocksQueryVisitor;
    
    private QueryInfo queryInfo;
    

    public StarRocksRowDataInputFormat(StarRocksSourceOptions sourceOptions, List<ColunmRichInfo> colunmRichInfos,
                                        long limit, String filter, String columns, SelectColumn[] selectColumns, StarRocksSourceQueryType queryType) {

        this.sourceOptions = sourceOptions;

        this.limit = limit;
        this.filter = filter;
        this.columns = columns;
        this.colunmRichInfos = colunmRichInfos;
        this.selectColumns = selectColumns;
        this.queryType = queryType;

        StarRocksJdbcConnectionOptions jdbcOptions = new StarRocksJdbcConnectionOptions(sourceOptions.getJdbcUrl(), sourceOptions.getUsername(), sourceOptions.getPassword());
        this.jdbcConnProvider = new StarRocksJdbcConnectionProvider(jdbcOptions);
        this.starrocksQueryVisitor = new StarRocksQueryVisitor(jdbcConnProvider, sourceOptions.getDatabaseName(), sourceOptions.getTableName());

        this.feHttpVisitor = new StarRocksFeHttpVisitor(sourceOptions);

        if (queryType == null) {
            queryType = StarRocksSourceQueryType.QueryAllColumns;
        }

        if (queryType == StarRocksSourceQueryType.QueryAllColumns){
            List<SelectColumn> nColumns = new ArrayList<>();
            this.colunmRichInfos.forEach(richInfo -> {
                nColumns.add(new SelectColumn(richInfo.getColumnName(), richInfo.getColunmIndexInSchema(), true));
            });
            this.selectColumns = nColumns.toArray(new SelectColumn[0]);
        }
        // validateTableStructure(this.colunmRichInfos);
    }

    @Override
    public void configure(Configuration configuration) {}

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return baseStatistics;
    }

    @Override
    public StarRocksTableInputSplit[] createInputSplits(int i) throws IOException {

        String SQL = genSQL();

        List<StarRocksTableInputSplit> list = new ArrayList<>();
        if (this.queryType == StarRocksSourceQueryType.QueryCount) {
            int dataCount = this.starrocksQueryVisitor.getQueryCount(SQL);
            list.add(new StarRocksTableInputSplit(0, null, null, true, dataCount));
            return list.toArray(new StarRocksTableInputSplit[0]);
        }
        this.queryInfo = feHttpVisitor.getQueryInfo(SQL);
        for (int x = 0; x < queryInfo.getBeXTablets().size(); x ++) {
            list.add(new StarRocksTableInputSplit(x, queryInfo, this.selectColumns, false, 0));
        }
        return list.toArray(new StarRocksTableInputSplit[0]);
    }

    private String genSQL() {

        if (queryType == null) {
            queryType = StarRocksSourceQueryType.QueryAllColumns;
        }

        String SQL = "select";
        switch (this.queryType) {
        case QueryCount:
            SQL = SQL + " count(*) ";
            break;
        case QueryAllColumns:
            SQL = SQL + " * ";
            break;
        case QuerySomeColumns:
            SQL = SQL + " " + columns + " ";
            break;
        }
        SQL = SQL + " from " + sourceOptions.getDatabaseName() + "." + sourceOptions.getTableName();
        if (!(filter == null || filter.equals(""))) {
            SQL = SQL + " where " + filter;
        }
        if (limit > 0) {
            // querySQL = querySQL + " limit " + limit;
        }
        return SQL;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(StarRocksTableInputSplit[] starRocksTableInputSplits) {

        return new DefaultInputSplitAssigner(starRocksTableInputSplits);
    }

    @Override
    public void open(StarRocksTableInputSplit starRocksTableInputSplit) {

        if (starRocksTableInputSplit.isQueryCount()) {
            StarRocksSourceTrickReader reader = new StarRocksSourceTrickReader(starRocksTableInputSplit.getDataCount());
            this.dataReader = reader;
        } else {
            QueryBeXTablets queryBeXTablets = starRocksTableInputSplit.getBeXTablets();
            String beNode[] = queryBeXTablets.getBeNode().split(":");
            String ip = beNode[0];
            int port = Integer.parseInt(beNode[1]);
            StarRocksSourceBeReader beReader = new StarRocksSourceBeReader(ip, port, colunmRichInfos, starRocksTableInputSplit.getSelectColumn(), this.sourceOptions);
            this.dataReader = beReader;
            beReader.openScanner(
                        queryBeXTablets.getTabletIds(),
                        starRocksTableInputSplit.getQueryInfo().getQueryPlan().getOpaqued_query_plan(),
                        this.sourceOptions);
            beReader.startToRead();            
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
        row = this.dataReader.getNext();
        GenericRowData genericRowData = new GenericRowData(row.size());
        for (int i = 0; i < row.size(); i++) {
            genericRowData.setField(i, row.get(i));
        }
        return genericRowData;
    }

    @Override
    public void close() throws IOException {
        if (this.dataReader != null) {
            this.dataReader.close();
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }

    private void validateTableStructure(List<ColunmRichInfo> colunmRichInfos) {
        if (null == colunmRichInfos) {
            return;
        }
        List<Map<String, Object>> rows = starrocksQueryVisitor.getTableColumnsMetaData();
        if (null == rows) {
            throw new IllegalArgumentException("Couldn't get the sink table's column info.");
        }
        // validate primary keys
        if (colunmRichInfos.size() != rows.size()) {
            throw new IllegalArgumentException("Fields count mismatch.");
        }
        for (int i = 0; i < rows.size(); i++) {
            String starrocksField = rows.get(i).get("COLUMN_NAME").toString().toLowerCase();
            String starrocksType = rows.get(i).get("DATA_TYPE").toString().toLowerCase();
            // TODO:
            // validateTableStructure
        }
    }

    /** Builder for {@link StarRocksRowDataInputFormat}. */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private StarRocksSourceOptions sourceOptions;
        private long limit;
        private String columns;
        private List<ColunmRichInfo> colunmRichInfos;
        private SelectColumn[] selectColumns;
        private String filter;
        private StarRocksSourceQueryType queryType;

        public Builder setSourceOptions(StarRocksSourceOptions sourceOptions) {
            this.sourceOptions = sourceOptions;
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
        public Builder setColunmRichInfos(List<ColunmRichInfo> colunmRichInfos) {
            this.colunmRichInfos = colunmRichInfos;
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
        public Builder setQueryType(StarRocksSourceQueryType queryType) {
            this.queryType = queryType;
            return this;
        }
        
        public Builder() {}

        public StarRocksRowDataInputFormat build() {
            if (this.sourceOptions == null) {
                throw new NullPointerException("No query supplied");
            }
            return new StarRocksRowDataInputFormat(this.sourceOptions, this.colunmRichInfos, this.limit, this.filter, this.columns, this.selectColumns, this.queryType);
        }
    }
}
