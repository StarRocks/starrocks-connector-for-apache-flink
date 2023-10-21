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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

public class CatalogOptions {

    public static final String IDENTIFIER = "starrocks";

    public static ConfigOption<String> FE_JDBC_URL =
            ConfigOptions.key("jdbc-url")
                .stringType()
                .noDefaultValue()
                .withDescription("StarRocks JDBC url like: `jdbc:mysql://fe_ip1:query_port,fe_ip2:query_port...`.");

    public static ConfigOption<String> FE_HTTP_URL =
            ConfigOptions.key("http-url")
                .stringType()
                .noDefaultValue()
                .withDescription("StarRocks FE http url like: `fe_ip1:http_port,http://fe_ip2:http_port`.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                .stringType()
                .noDefaultValue()
                .withDescription("StarRocks user name.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                .stringType()
                .noDefaultValue()
                .withDescription("StarRocks user password.");

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
                .stringType()
                .noDefaultValue()
                .withDescription("The default database.");

    // ------ options for create table ------

    public static final String TABLE_PROPERTIES_PREFIX = "table.properties";

    public static final ConfigOption<Integer> TABLE_NUM_BUCKETS =
            ConfigOptions.key("table.num-buckets")
                .intType()
                .noDefaultValue()
                .withDescription("Number of buckets for creating StarRocks table.");
}
