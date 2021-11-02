package com.starrocks.connector.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

public final class StarRocksDynamicTableSourceFactory implements DynamicTableSourceFactory {

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(StarRocksSourceOptions.SOURCE_PROPERTIES_PREFIX);
        ReadableConfig options = helper.getOptions();
        // validate some special properties
        StarRocksSourceOptions sourceOptions = new StarRocksSourceOptions(options, context.getCatalogTable().getOptions());
        TableSchema flinkSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        StarRocksRowDataInputFormat.Builder builder = StarRocksRowDataInputFormat.builder();
        return new StarRocksDynamicTableSource(sourceOptions, flinkSchema, builder);
    }


    @Override
    public String factoryIdentifier() {
        return "starrocks-source";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {

        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(StarRocksSourceOptions.USERNAME);
        options.add(StarRocksSourceOptions.PASSWORD);
        options.add(StarRocksSourceOptions.TABLE_NAME);
        options.add(StarRocksSourceOptions.DATABASE_NAME);
        options.add(StarRocksSourceOptions.SCAN_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {

        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(StarRocksSourceOptions.SOURCE_CONNECT_TIMEOUT);
        options.add(StarRocksSourceOptions.SCAN_BATCH_SIZE);
        options.add(StarRocksSourceOptions.SCAN_PROPERTIES);
        options.add(StarRocksSourceOptions.SCAN_LIMIT);
        options.add(StarRocksSourceOptions.SCAN_KEEP_ALIVE_MIN);
        options.add(StarRocksSourceOptions.SCAN_QUERTY_TIMEOUT);
        options.add(StarRocksSourceOptions.SCAN_MEM_LIMIT);
        options.add(StarRocksSourceOptions.SOURCE_MAX_RETRIES);
        return options;
    }
}
