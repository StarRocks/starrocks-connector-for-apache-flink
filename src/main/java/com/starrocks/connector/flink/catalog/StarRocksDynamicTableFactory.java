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

import com.starrocks.connector.flink.table.sink.StarRocksDynamicTableSinkFactory;
import com.starrocks.connector.flink.table.source.StarRocksDynamicTableSourceFactory;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Note this factory is only used for catalog currently, and maybe unify it with
 * {@link StarRocksDynamicTableSourceFactory} and {@link StarRocksDynamicTableSinkFactory}
 * in the future.
 */
public class StarRocksDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    public static final String IDENTIFIER = "starrocks";

    private final StarRocksDynamicTableSourceFactory sourceFactory;
    private final StarRocksDynamicTableSinkFactory sinkFactory;

    public StarRocksDynamicTableFactory() {
        this.sourceFactory = new StarRocksDynamicTableSourceFactory();
        this.sinkFactory = new StarRocksDynamicTableSinkFactory();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        return sourceFactory.createDynamicTableSource(context);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        return sinkFactory.createDynamicTableSink(context);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> configOptions = new HashSet<>();
        configOptions.addAll(sourceFactory.requiredOptions());
        configOptions.addAll(sinkFactory.requiredOptions());
        return configOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> configOptions = new HashSet<>();
        configOptions.addAll(sourceFactory.optionalOptions());
        configOptions.addAll(sinkFactory.optionalOptions());
        return configOptions;
    }
}
