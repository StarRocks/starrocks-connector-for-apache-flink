package com.starrocks.connector.flink.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.Set;

public class StarRocksDynamicTableSourceFactory implements DynamicTableSourceFactory {

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("username")
            .stringType()
            .noDefaultValue()
            .withDescription("StarRocks username.");

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password")
            .stringType()
            .noDefaultValue()
            .withDescription("StarRocks password.");

    public static final ConfigOption<String> FILTER = ConfigOptions
            .key("filter")
            .stringType()
            .noDefaultValue()
            .withDescription("filter");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        final FactoryUtil.TableFactoryHelper factoryHelper = FactoryUtil.createTableFactoryHelper(this, context);
        factoryHelper.validate();
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new StarRocksDynamicTableSource(getStarRocksSourceOptions(factoryHelper.getOptions()));
    }

    private StarRocksSourceOptions getStarRocksSourceOptions(ReadableConfig config) {

        StarRocksSourceOptions options = StarRocksSourceOptions.builder()
                .withProperty("jdbc-url", config.get(null))
                .build();
        // todo: params
        return options;
    }

    @Override
    public String factoryIdentifier() {
        return "starrocks";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        // todo: add params
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FILTER);
        // todo: add params
        return options;
    }
}
