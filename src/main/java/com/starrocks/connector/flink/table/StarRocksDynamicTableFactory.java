package com.starrocks.connector.flink.table;

import com.starrocks.connector.flink.table.sink.StarRocksDynamicTableSinkFactory;
import com.starrocks.connector.flink.table.source.StarRocksDynamicTableSourceFactory;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.starrocks.connector.flink.table.StarRocksOptions.*;
import static com.starrocks.connector.flink.table.StarRocksOptions.JDBC_URL;

/**
 * The {@link StarRocksDynamicTableFactory} translates the catalog table to a table source.
 *
 * <p>Because the table source requires a decoding format, we are discovering the format using the
 * provided {@link FactoryUtil} for convenience.
 */
public final class StarRocksDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private StarRocksDynamicTableSourceFactory dynamicTableSourceFactory = new StarRocksDynamicTableSourceFactory();

    private StarRocksDynamicTableSinkFactory dynamicTableSinkFactory = new StarRocksDynamicTableSinkFactory();

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER; // used for matching to `connector = '...'`
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return dynamicTableSinkFactory.createDynamicTableSink(context);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return dynamicTableSourceFactory.createDynamicTableSource(context);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(TABLE_NAME);
        options.add(DATABASE_NAME);
        options.add(FE_NODES);
        options.add(JDBC_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(StarRocksSourceOptions.SCAN_CONNECT_TIMEOUT);
        options.add(StarRocksSourceOptions.SCAN_BATCH_ROWS);
        options.add(StarRocksSourceOptions.SCAN_PROPERTIES);
        // options.add(StarRocksSourceOptions.SCAN_LIMIT);
        options.add(StarRocksSourceOptions.SCAN_KEEP_ALIVE_MIN);
        options.add(StarRocksSourceOptions.SCAN_QUERTY_TIMEOUT_S);
        options.add(StarRocksSourceOptions.SCAN_MEM_LIMIT);
        options.add(StarRocksSourceOptions.SCAN_MAX_RETRIES);
        options.add(StarRocksSourceOptions.SCAN_BE_HOST_MAPPING_LIST);
        options.add(StarRocksSourceOptions.LOOKUP_CACHE_TTL_MS);
        options.add(StarRocksSourceOptions.LOOKUP_CACHE_MAX_ROWS);
        options.add(StarRocksSourceOptions.LOOKUP_MAX_RETRIES);


        return options;
    }
}