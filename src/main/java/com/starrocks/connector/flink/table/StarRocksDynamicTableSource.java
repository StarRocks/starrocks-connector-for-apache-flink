package com.starrocks.connector.flink.table;

import com.starrocks.connector.flink.exception.HttpException;
import com.starrocks.connector.flink.manager.StarRocksSourceManager;
import com.starrocks.connector.flink.related.QueryInfo;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;

import java.io.IOException;

public class StarRocksDynamicTableSource implements ScanTableSource, LookupTableSource {

    private static final long serialVersionUID = 1L;
    private final StarRocksSourceOptions options;

    private StarRocksSourceManager manager;

    public StarRocksDynamicTableSource(StarRocksSourceOptions options) {

        manager = new StarRocksSourceManager(options);
        this.options = options;
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
        return new StarRocksDynamicTableSource(this.options);
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
