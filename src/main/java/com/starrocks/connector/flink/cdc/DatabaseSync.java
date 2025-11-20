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
package com.starrocks.connector.flink.cdc;

import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.catalog.StarRocksCatalog;
import com.starrocks.connector.flink.catalog.StarRocksTable;
import com.starrocks.connector.flink.cdc.json.DebeziumJsonFilter;
import com.starrocks.connector.flink.cdc.json.DebeziumJsonProcess;
import com.starrocks.connector.flink.cdc.json.DebeziumJsonSerializer;
import com.starrocks.connector.flink.cdc.mysql.ParsingProcessFunction;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public abstract class DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseSync.class);
    private static final String FAST_SCHEMA_EVOLUTION = "fast_schema_evolution";
    protected Configuration config;
    protected String database;
    protected TableNameConverter converter;
    protected Pattern includingPattern;
    protected Pattern excludingPattern;
    protected Map<String, String> tableConfig;
    protected Configuration sinkConfig;
    public StreamExecutionEnvironment env;
    private Map<String, String> tableMapping = new HashMap<>();
    private Boolean isFastSchemaEvolution;

    public abstract Connection getConnection() throws SQLException;

    public abstract List<SourceSchema> getSchemaList() throws Exception;

    public abstract DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env);


    public void create(StreamExecutionEnvironment env, String database, Configuration config,
                       String tablePrefix, String tableSuffix, String includingTables,
                       String excludingTables, Configuration sinkConfig, Map<String, String> tableConfig) {
        this.env = env;
        this.config = config;
        this.database = database;
        this.converter = new TableNameConverter(tablePrefix, tableSuffix);
        this.includingPattern = includingTables == null ? null : Pattern.compile(includingTables);
        this.excludingPattern = excludingTables == null ? null : Pattern.compile(excludingTables);
        this.sinkConfig = sinkConfig;
        this.tableConfig = tableConfig == null ? new HashMap<>() : tableConfig;
        this.isFastSchemaEvolution = checkFastSchemaEvolution();
    }

    public void build() throws Exception {
        StarRocksJdbcConnectionOptions options = getStarRocksConnectionOptions();
        StarRocksCatalog starRocksCatalog = new StarRocksCatalog(options.getDbURL(), options.getUsername().get(), options.getPassword().get());
        starRocksCatalog.open();

        List<SourceSchema> schemaList = getSchemaList();
        if (!starRocksCatalog.databaseExists(database)) {
            LOG.info("database {} not exist, created", database);
            starRocksCatalog.createDatabase(database, true);
        }

        List<String> syncTables = new ArrayList<>();
        List<String> starRocksTables = new ArrayList<>();
        for (SourceSchema schema : schemaList) {
            syncTables.add(schema.getTableName());
            String starRocksTable = converter.convert(schema.getTableName());

            tableMapping.put(schema.getTableIdentifier(), String.format("%s.%s", database, starRocksTable));
            if (!starRocksCatalog.tableExists(database, starRocksTable)) {
                StarRocksTable starRocksSchema = schema.convertStarRocksTable(tableConfig);

                starRocksCatalog.createTable(starRocksSchema, true);
            }
            if(!starRocksTables.contains(starRocksTable)){
                starRocksTables.add(starRocksTable);
            }
        }
        starRocksCatalog.close();

        Preconditions.checkState(!syncTables.isEmpty(), "No tables to be synchronized.");
        config.set(MySqlSourceOptions.TABLE_NAME, "(" + String.join("|", syncTables) + ")");

        DataStreamSource<String> streamSource = buildCdcSource(env);
        SingleOutputStreamOperator<Void> parsedStream = streamSource.process(new ParsingProcessFunction(converter));
        for (String table : starRocksTables) {
            OutputTag<String> recordOutputTag = ParsingProcessFunction.createRecordOutputTag(table);
            DataStream<String> sideOutput = parsedStream.getSideOutput(recordOutputTag);

            int sinkParallel = sinkConfig.get(StarRocksSinkOptions.SINK_PARALLELISM, sideOutput.getParallelism());

            StarRocksSinkOptions starRocksSinkOptions = getStarRocksSinkOptions(table);
            SinkFunction<String> starRockSink = StarRocksSink.sink(starRocksSinkOptions);
            sideOutput.map(new DebeziumJsonProcess(getSerializer(table))).filter(new DebeziumJsonFilter()).addSink(starRockSink).setParallelism(sinkParallel).name(table);
        }

    }

    private boolean checkFastSchemaEvolution() {
        String tableProperty = tableConfig.get(FAST_SCHEMA_EVOLUTION);
        return tableProperty != null && tableProperty.equalsIgnoreCase("true");
    }

    private DebeziumJsonSerializer getSerializer(String table) {
        String user = sinkConfig.get(StarRocksSinkOptions.USERNAME);
        String passwd = sinkConfig.get(StarRocksSinkOptions.PASSWORD, "");
        String jdbcUrl = sinkConfig.get(StarRocksSinkOptions.JDBC_URL);

        StarRocksOptions.Builder starRocksBuilder = StarRocksOptions.builder();
        starRocksBuilder.setTableIdentifier(database + "." + table)
                .setUsername(user)
                .setPassword(passwd)
                .setJdbcUrl(jdbcUrl)
                .setFastSchemaEvolution(isFastSchemaEvolution);

        return DebeziumJsonSerializer.builder().setStarRocksOptions(starRocksBuilder.build()).build();
    }

    /**
     * load-url :<fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>
     * @param table
     * @return
     */
    private StarRocksSinkOptions getStarRocksSinkOptions(String table) {
        String jdbcUrl = sinkConfig.get(StarRocksSinkOptions.JDBC_URL);
        String loadUrl = String.join(";", sinkConfig.get(StarRocksSinkOptions.LOAD_URL));
        String user = sinkConfig.get(StarRocksSinkOptions.USERNAME);
        String passwd = sinkConfig.get(StarRocksSinkOptions.PASSWORD, "");
        String labelPrefix = sinkConfig.get(StarRocksSinkOptions.SINK_LABEL_PREFIX);

        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", jdbcUrl)
                .withProperty("load-url", loadUrl)
                .withProperty("database-name", database)
                .withProperty("table-name", table)
                .withProperty("username", user)
                .withProperty("password", passwd)
                .withProperty("sink.semantic", "exactly-once")
                .withProperty("sink.version", "V2")
                .withProperty("sink.label-prefix", labelPrefix)
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .withProperty("sink.properties.ignore_json_size", "true")
                .withProperty("sink.socket.timeout-ms", "60000")
                .build();

        options.enableUpsertDelete();
        return options;
    }

    private StarRocksJdbcConnectionOptions getStarRocksConnectionOptions() {
        String user = sinkConfig.get(StarRocksSinkOptions.USERNAME);
        String passwd = sinkConfig.get(StarRocksSinkOptions.PASSWORD, "");
        String jdbcUrl = sinkConfig.get(StarRocksSinkOptions.JDBC_URL);
        Preconditions.checkNotNull(user, "username is empty in sink-conf");
        Preconditions.checkNotNull(jdbcUrl, "jdbcurl is empty in sink-conf");

        return new StarRocksJdbcConnectionOptions(jdbcUrl, user, passwd);
    }

    /**
     * Filter table that need to be synchronized
     */
    protected boolean isSyncNeeded(String tableName) {
        boolean sync = true;
        if (includingPattern != null) {
            sync = includingPattern.matcher(tableName).matches();
        }
        if (excludingPattern != null) {
            sync = sync && !excludingPattern.matcher(tableName).matches();
        }
        LOG.debug("table {} is synchronized? {}", tableName, sync);
        return sync;
    }

    public static class TableNameConverter implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String prefix;
        private final String suffix;

        TableNameConverter(){
            this("","");
        }

        TableNameConverter(String prefix, String suffix) {
            this.prefix = prefix == null ? "" : prefix;
            this.suffix = suffix == null ? "" : suffix;
        }

        public String convert(String tableName) {
            return prefix + tableName + suffix;
        }
    }
}
