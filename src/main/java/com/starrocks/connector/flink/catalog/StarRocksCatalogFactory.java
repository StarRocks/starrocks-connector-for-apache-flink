/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import static com.starrocks.connector.flink.catalog.CatalogOptions.IDENTIFIER;
import static com.starrocks.connector.flink.catalog.ConfigUtils.getPrefixConfigs;

public class StarRocksCatalogFactory implements CatalogFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Catalog createCatalog(Context context) {
        final FactoryUtil.CatalogFactoryHelper helper =
                FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validateExcept(
                StarRocksSourceOptions.SOURCE_PROPERTIES_PREFIX,
                StarRocksSinkOptions.SINK_PROPERTIES_PREFIX,
                CatalogOptions.TABLE_PROPERTIES_PREFIX);

        Configuration sourceBaseConfig =
                Configuration.fromMap(
                        getPrefixConfigs("scan.", context.getOptions(), false));
        Configuration sinkBaseConfig =
                Configuration.fromMap(
                        getPrefixConfigs("sink.", context.getOptions(), false));
        Configuration tableBaseConfig =
                Configuration.fromMap(
                        getPrefixConfigs("table.", context.getOptions(), false));
        return new StarRocksCatalog(
                context.getName(),
                helper.getOptions().get(CatalogOptions.FE_JDBC_URL),
                helper.getOptions().get(CatalogOptions.FE_HTTP_URL),
                helper.getOptions().get(CatalogOptions.USERNAME),
                helper.getOptions().get(CatalogOptions.PASSWORD),
                helper.getOptions().get(CatalogOptions.DEFAULT_DATABASE),
                sourceBaseConfig,
                sinkBaseConfig,
                tableBaseConfig,
                context.getClassLoader()
            );
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CatalogOptions.FE_JDBC_URL);
        options.add(CatalogOptions.FE_HTTP_URL);
        options.add(CatalogOptions.USERNAME);
        options.add(CatalogOptions.PASSWORD);
        options.add(CatalogOptions.DEFAULT_DATABASE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        StarRocksDynamicTableFactory factory = new StarRocksDynamicTableFactory();
        options.addAll(factory.optionalOptions());
        options.add(CatalogOptions.TABLE_NUM_BUCKETS);
        return options;
    }
}
