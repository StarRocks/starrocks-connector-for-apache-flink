package com.starrocks.connector.flink.table.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

import com.starrocks.connector.flink.table.source.struct.PushDownHolder;


public final class StarRocksDynamicTableSourceFactory implements DynamicTableSourceFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(StarRocksSourceOptions.SOURCE_PROPERTIES_PREFIX);
        ReadableConfig options = helper.getOptions();
        // validate some special properties
        StarRocksSourceOptions sourceOptions = new StarRocksSourceOptions(options, context.getCatalogTable().getOptions());
        TableSchema flinkSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        PushDownHolder pushDownHolder = new PushDownHolder();
        return new StarRocksDynamicTableSource(sourceOptions, flinkSchema, pushDownHolder);
    }


    @Override
    public String factoryIdentifier() {
        return "starrocks";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {

        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(StarRocksSourceOptions.USERNAME);
        options.add(StarRocksSourceOptions.PASSWORD);
        options.add(StarRocksSourceOptions.TABLE_NAME);
        options.add(StarRocksSourceOptions.DATABASE_NAME);
        options.add(StarRocksSourceOptions.SCAN_URL);
        options.add(StarRocksSourceOptions.JDBC_URL);
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
        return options;
    }
}
