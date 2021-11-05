package com.starrocks.connector.flink.table;

import com.starrocks.connector.flink.source.ColunmRichInfo;
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

    private StarRocksSourceBeReader dataReader;
    private final List<ColunmRichInfo> colunmRichInfos;
    

    public StarRocksDynamicSourceFunction(StarRocksSourceOptions sourceOptions, QueryInfo queryInfo, TableSchema fSchema, 
                                            SelectColumn[] selectColumns, List<ColunmRichInfo> colunmRichInfos) {
        this.sourceOptions = sourceOptions;
        this.queryInfo = queryInfo;
        this.datatypes = fSchema.getFieldDataTypes();
        this.selectColumns = selectColumns;
        this.colunmRichInfos = colunmRichInfos;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        super.open(parameters);
        int subTaskId = getRuntimeContext().getIndexOfThisSubtask();
        this.queryBeXTablets = queryInfo.getBeXTablets().get(subTaskId);
        String beNode[] = this.queryBeXTablets.getBeNode().split(":");
        String ip = beNode[0];
        int port = Integer.parseInt(beNode[1]);
        this.dataReader = new StarRocksSourceBeReader(ip, port, colunmRichInfos, selectColumns, this.sourceOptions);
        this.dataReader.openScanner(
                this.queryBeXTablets.getTabletIds(),
                this.queryInfo.getQueryPlan().getOpaqued_query_plan(),
                this.sourceOptions);
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

        this.dataReader.close();
    }

    @Override
    public TypeInformation<List<?>> getProducedType() {
        return TypeInformation.of(new TypeHint<List<?>>() {});
    }
}
