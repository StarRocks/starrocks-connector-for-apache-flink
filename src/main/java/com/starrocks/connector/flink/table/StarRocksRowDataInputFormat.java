package com.starrocks.connector.flink.table;


import com.starrocks.connector.flink.exception.HttpException;
import com.starrocks.connector.flink.exception.StarRocksException;
import com.starrocks.connector.flink.manager.StarRocksSourceInfoVisitor;
import com.starrocks.connector.flink.source.ColunmRichInfo;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class StarRocksRowDataInputFormat extends RichInputFormat<RowData, StarRocksTableInputSplit> implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksRowDataInputFormat.class);

    private final StarRocksSourceOptions sourceOptions;
    
    private final long limit;
    private final String filter;
    private String columns;
    private final List<ColunmRichInfo> colunmRichInfos;
    private SelectColumn[] selectColumns;

    private StarRocksSourceDataReader dataReader;
    private StarRocksSourceInfoVisitor infoVisitor;

    private QueryInfo queryInfo;

    public StarRocksRowDataInputFormat(StarRocksSourceOptions sourceOptions, List<ColunmRichInfo> colunmRichInfos,
                                        long limit, String filter, String columns, SelectColumn[] selectColumns) {

        this.sourceOptions = sourceOptions;
        this.limit = limit;
        this.filter = filter;
        this.columns = columns;
        this.colunmRichInfos = colunmRichInfos;
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
            this.queryInfo = infoVisitor.getQueryInfo(genSQL());
        } catch (HttpException | StarRocksException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
        List<StarRocksTableInputSplit> list = new ArrayList<>();
        for (int x = 0; x < queryInfo.getBeXTablets().size(); x ++) {
            list.add(new StarRocksTableInputSplit(x, queryInfo, this.selectColumns));
        }
        return list.toArray(new StarRocksTableInputSplit[0]);
    }

    private String genSQL() throws StarRocksException {

        if (columns == null) {
            columns = "*";
            List<SelectColumn> nColumns = new ArrayList<>();
            this.colunmRichInfos.forEach(richInfo -> {
                nColumns.add(new SelectColumn(richInfo.getColumnName(), richInfo.getColunmIndexInSchema(), true));
            });
            this.selectColumns = nColumns.toArray(new SelectColumn[0]);
        } else {
            String tmpFilter = filter.replace("(", "").replace(")","");
            String[] express = tmpFilter.split(" ");

            Set<String> selectedSet = new HashSet<>();
            for (int i = 0; i < selectColumns.length; i ++) {
                selectedSet.add(selectColumns[i].getColumnName());
            }

            List<String> expresstionCol = new ArrayList<>();
            List<SelectColumn> addSelectColumns = new ArrayList<>();
            for (int i = 0; i < express.length; i ++) {
                if (express[i].equals("=") && !selectedSet.contains(express[i - 1])) {
                    String columnName = express[i - 1];
                    expresstionCol.add(columnName);
                    int colunmIndex = -1;
                    for (int y = 0; y < this.colunmRichInfos.size(); y ++) {
                        if (this.colunmRichInfos.get(y).getColumnName().equals(columnName)) {
                            colunmIndex = y;
                        }
                    }
                    if (colunmIndex == -1) {
                        throw new StarRocksException("could not found column when gen SQL");
                    }
                    addSelectColumns.add(new SelectColumn(columnName, colunmIndex, false));
                }
            }

            SelectColumn[] newSelected = addSelectColumns.toArray(new SelectColumn[0]);

            SelectColumn[] realSelectColumns = new SelectColumn[selectColumns.length + newSelected.length];
            System.arraycopy(selectColumns, 0, realSelectColumns, 0, selectColumns.length);
            System.arraycopy(newSelected, 0, realSelectColumns, selectColumns.length, newSelected.length);

            this.selectColumns = realSelectColumns;

            if (expresstionCol.size() > 0) {
                if (columns.equals("")) {
                    columns = String.join(", ", expresstionCol);
                } else {
                    columns = columns + ", " + String.join(", ", expresstionCol);
                }
            }
        }

        String querySQL = "select " + columns + " from " + sourceOptions.getDatabaseName() + "." + sourceOptions.getTableName();
        if (!(filter == null || filter.equals(""))) {
            querySQL = querySQL + " where " + filter;
        }
        if (limit > 0) {
            // querySQL = querySQL + " limit " + limit;
        }

        return querySQL;
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
        
        try {
            this.dataReader = new StarRocksSourceDataReader(ip, port, colunmRichInfos, starRocksTableInputSplit.getSelectColumn(), this.sourceOptions);
        } catch (StarRocksException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }

        try {
            this.dataReader.openScanner(
                    queryBeXTablets.getTabletIds(),
                    starRocksTableInputSplit.getQueryInfo().getQueryPlan().getOpaqued_query_plan(),
                    this.sourceOptions);
        } catch (StarRocksException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
        }

        try {
            this.dataReader.startToRead();
        } catch (StarRocksException | IOException e) {
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
        private long limit;
        private String columns;
        private List<ColunmRichInfo> colunmRichInfos;
        private SelectColumn[] selectColumns;
        private String filter;
        
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
        

        public Builder() {}

        public StarRocksRowDataInputFormat build() {
            if (this.sourceOptions == null) {
                throw new NullPointerException("No query supplied");
            }
            return new StarRocksRowDataInputFormat(this.sourceOptions, this.colunmRichInfos, this.limit, this.filter, this.columns, this.selectColumns);
        }
    }
}
