package com.starrocks.connector.flink.catalog;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static com.starrocks.connector.flink.catalog.StarRocksCatalogOptions.DEFAULT_DATABASE;
import static com.starrocks.connector.flink.table.StarRocksOptions.DATABASE_NAME;
import static com.starrocks.connector.flink.table.StarRocksOptions.IDENTIFIER;
import static com.starrocks.connector.flink.table.StarRocksOptions.JDBC_URL;
import static com.starrocks.connector.flink.table.StarRocksOptions.PASSWORD;
import static com.starrocks.connector.flink.table.StarRocksOptions.TABLE_NAME;
import static com.starrocks.connector.flink.table.StarRocksOptions.USERNAME;

/**
 * Factory for {@link StarRocksCatalog}.
 */
public class StarRocksCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(TABLE_NAME);
        options.add(DATABASE_NAME);
        options.add(JDBC_URL);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        //sink
        options.add(StarRocksSinkOptions.SINK_VERSION);
        options.add(StarRocksSinkOptions.SINK_BATCH_MAX_SIZE);
        options.add(StarRocksSinkOptions.SINK_BATCH_MAX_ROWS);
        options.add(StarRocksSinkOptions.SINK_BATCH_FLUSH_INTERVAL);
        options.add(StarRocksSinkOptions.SINK_MAX_RETRIES);
        options.add(StarRocksSinkOptions.SINK_SEMANTIC);
        options.add(StarRocksSinkOptions.SINK_BATCH_OFFER_TIMEOUT);
        options.add(StarRocksSinkOptions.SINK_PARALLELISM);
        options.add(StarRocksSinkOptions.SINK_LABEL_PREFIX);
        options.add(StarRocksSinkOptions.SINK_CONNECT_TIMEOUT);
        options.add(StarRocksSinkOptions.SINK_IO_THREAD_COUNT);
        options.add(StarRocksSinkOptions.SINK_CHUNK_LIMIT);
        options.add(StarRocksSinkOptions.SINK_SCAN_FREQUENCY);

        //source
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

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(StarRocksSinkOptions.SINK_PROPERTIES_PREFIX);

        return new StarRocksCatalog(
                context.getName(),
                helper.getOptions().get(JDBC_URL),
                helper.getOptions().get(DEFAULT_DATABASE),
                helper.getOptions().get(USERNAME),
                helper.getOptions().get(PASSWORD),
                ((Configuration) helper.getOptions()).toMap());
    }
}
