package com.starrocks.connector.flink.table;

import com.starrocks.connector.flink.exception.HttpException;
import com.starrocks.connector.flink.manager.StarRocksSourceManager;
import com.starrocks.connector.flink.source.QueryInfo;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;

import java.io.IOException;

public class StarRocksDynamicTableSource implements ScanTableSource, LookupTableSource {

    private transient TableSchema flinkSchema;
    private final StarRocksSourceOptions options;

    private StarRocksSourceManager manager;

    public StarRocksDynamicTableSource(StarRocksSourceOptions options, TableSchema schema) {
        this.options = options;
        this.flinkSchema = schema;
        manager = new StarRocksSourceManager(options);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {

        manager = new StarRocksSourceManager(options);
        QueryInfo queryInfo;
        try {
            queryInfo = manager.getQueryInfo();
        } catch (IOException | HttpException e) {
            throw new RuntimeException(e.getMessage());
        }
        StarRocksRowDataInputFormat inputFormat = new StarRocksRowDataInputFormat(this.options, queryInfo);
        return InputFormatProvider.of(inputFormat);
    }

    @Override
    public DynamicTableSource copy() {
        return new StarRocksDynamicTableSource(this.options, this.flinkSchema);
    }

    @Override
    public String asSummaryString() {
        return "StarRocks Table Source";
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext lookupContext) {
        return null;
    }
}
