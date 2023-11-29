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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TemporaryClassLoaderContext;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import org.apache.commons.compress.utils.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.connector.flink.catalog.JdbcUtils.verifyJdbcDriver;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Flink catalog for StarRocks. */
public class FlinkCatalog extends AbstractCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCatalog.class);

    private final String jdbcUrl;
    private final String httpUrl;
    private final String username;
    private final String password;
    private final Configuration sourceBaseConfig;
    private final Configuration sinkBaseConfig;
    private final Configuration tableBaseConfig;
    private final ClassLoader userClassLoader;
    private final StarRocksCatalog starRocksCatalog;

    public FlinkCatalog(
            String name,
            String jdbcUrl,
            String httpUrl,
            String username,
            String password,
            String defaultDatabase,
            Configuration sourceBaseConfig,
            Configuration sinkBaseConfig,
            Configuration tableBaseConfig,
            ClassLoader userClassLoader) {
        super(name, defaultDatabase);
        this.jdbcUrl = jdbcUrl;
        this.httpUrl = httpUrl;
        this.username = username;
        this.password = password;
        this.sourceBaseConfig = sourceBaseConfig;
        this.sinkBaseConfig = sinkBaseConfig;
        this.tableBaseConfig = tableBaseConfig;
        this.userClassLoader = userClassLoader;
        this.starRocksCatalog = new StarRocksCatalog(jdbcUrl, username, password);
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new StarRocksDynamicTableFactory());
    }

    @Override
    public void open() throws CatalogException {
        // Refer to flink-connector-jdbc's AbstractJdbcCatalog
        // load the Driver use userClassLoader explicitly, see FLINK-15635 for more detail
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(userClassLoader)) {
            verifyJdbcDriver();
            // test connection, fail early if we cannot connect to database
            try (Connection conn = getConnection()) {
            } catch (SQLException e) {
                throw new ValidationException(
                        String.format("Failed to connect StarRocks via JDBC: %s.", jdbcUrl), e);
            }
        }
        LOG.info("Open flink catalog for StarRocks {}", getName());
    }

    @Override
    public void close() throws CatalogException {
        LOG.info("Close flink catalog for StarRocks {}", getName());
    }

    // ------ databases ------

    @Override
    public List<String> listDatabases() throws CatalogException {
        return executeSingleColumnStatement(
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;");
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        if (databaseExists(databaseName)) {
            return new CatalogDatabaseImpl(Collections.emptyMap(), null);
        } else {
            throw new DatabaseNotExistException(getName(), databaseName);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        try {
            return starRocksCatalog.databaseExists(databaseName);
        } catch (Exception e) {
            throw new CatalogException("Failed to check database exist", e);
        }
    }

    @Override
    public void createDatabase(String databaseName, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }
            throw new DatabaseAlreadyExistException(getName(), databaseName);
        }

        try {
            starRocksCatalog.createDatabase(databaseName, ignoreIfExists);
        } catch (StarRocksCatalogException e) {
            throw new CatalogException(
                    String.format("Failed to create database %s, ignoreIfExists: %s",
                            databaseName, ignoreIfExists),
                    e);
        }
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------ tables ------

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "database name cannot be null or empty.");
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(getName(), databaseName);
        }

        return executeSingleColumnStatement(
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                databaseName);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        List<String> tableList = executeSingleColumnStatement(
                "SELECT TABLE_NAME FROM information_schema.`TABLES` " +
                        "WHERE TABLE_SCHEMA=? and TABLE_NAME=?",
                tablePath.getDatabaseName(),
                tablePath.getObjectName()
            );
        return !tableList.isEmpty();
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        try {
            StarRocksTable starRocksTable = starRocksCatalog.getTable(
                    tablePath.getDatabaseName(), tablePath.getObjectName()).orElse(null);
            if (starRocksTable == null) {
                throw new TableNotExistException(getName(), tablePath);
            }
            Schema.Builder flinkSchemaBuilder = Schema.newBuilder();
            Set<String> primaryKeys = new HashSet<>();
            if (starRocksTable.getTableType() == StarRocksTable.TableType.PRIMARY_KEY &&
                    starRocksTable.getTableKeys().isPresent()) {
                flinkSchemaBuilder.primaryKey(starRocksTable.getTableKeys().get());
                primaryKeys.addAll(starRocksTable.getTableKeys().get());
            }
            for (StarRocksColumn column : starRocksTable.getColumns()) {
                flinkSchemaBuilder.column(
                        column.getColumnName(),
                        TypeUtils.toFlinkType(
                            column.getDataType(),
                            column.getColumnSize().orElse(null),
                            column.getDecimalDigits().orElse(null),
                            column.isNullable()
                        )
                    );
            }
            Schema flinkSchema = flinkSchemaBuilder.build();
            Map<String, String> properties = new HashMap<>();
            properties.put(CONNECTOR.key(), CatalogOptions.IDENTIFIER);
            properties.putAll(getSourceConfig(tablePath.getDatabaseName(), tablePath.getObjectName()));
            properties.putAll(getSinkConfig(tablePath.getDatabaseName(), tablePath.getObjectName()));

            return CatalogTable.of(flinkSchema, starRocksTable.getComment().orElse(null), Lists.newArrayList(), properties);
        } catch (StarRocksCatalogException e) {
            throw new CatalogException(
                    String.format("Failed to get table %s in catalog %s", tablePath.getFullName(), getName()), e);
        }
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(getName(), tablePath.getDatabaseName());
        }

        if (tableExists(tablePath)) {
           if (ignoreIfExists) {
               return;
           }
           throw new TableAlreadyExistException(getName(), tablePath);
        }

        StarRocksTable starRocksTable = StarRocksUtils.toStarRocksTable(getName(), tablePath, tableBaseConfig, table);
        try {
            starRocksCatalog.createTable(starRocksTable, ignoreIfExists);
            LOG.info("Success to create table {} in catalog {}", tablePath.getFullName(), getName());
        } catch (StarRocksCatalogException e) {
            LOG.error("Failed to create table {} in catalog {}", tablePath.getFullName(), getName(), e);
            throw new CatalogException(
                    String.format("Failed to create table %s in catalog %s",
                            tablePath.getFullName(), getName()), e);
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(getName(), tablePath);
        }

        try {
            String dropSql = String.format(
                    "DROP TABLE `%s`.`%s`;", tablePath.getDatabaseName(), tablePath.getObjectName());
            executeUpdateStatement(dropSql);
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed to drop table %s in catalog %s",
                    tablePath.getFullName(), getName()), e);
        }
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        // TODO Flink supports to add/drop column since 1.17. Implement it in the future if needed.
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    private Map<String, String> getSourceConfig(String database, String table) {
        Map<String, String> sourceConfig = new HashMap<>(sourceBaseConfig.toMap());
        setIfNotExist(sourceConfig, StarRocksSourceOptions.JDBC_URL, jdbcUrl);
        setIfNotExist(sourceConfig, StarRocksSourceOptions.SCAN_URL, httpUrl);
        setIfNotExist(sourceConfig, StarRocksSourceOptions.USERNAME, username);
        setIfNotExist(sourceConfig, StarRocksSourceOptions.PASSWORD, password);
        sourceConfig.put(StarRocksSourceOptions.DATABASE_NAME.key(), database);
        sourceConfig.put(StarRocksSourceOptions.TABLE_NAME.key(), table);
        return sourceConfig;
    }

    private Map<String, String> getSinkConfig(String database, String table) {
        Map<String, String> sinkConfig = new HashMap<>(sinkBaseConfig.toMap());
        setIfNotExist(sinkConfig, StarRocksSinkOptions.JDBC_URL, jdbcUrl);
        setIfNotExist(sinkConfig, StarRocksSinkOptions.LOAD_URL, httpUrl);
        setIfNotExist(sinkConfig, StarRocksSinkOptions.USERNAME, username);
        setIfNotExist(sinkConfig, StarRocksSinkOptions.PASSWORD, password);
        sinkConfig.put(StarRocksSinkOptions.DATABASE_NAME.key(), database);
        sinkConfig.put(StarRocksSinkOptions.TABLE_NAME.key(), table);
        if (sinkConfig.containsKey(StarRocksSinkOptions.SINK_LABEL_PREFIX.key())) {
            String rawLabelPrefix = sinkConfig.get(StarRocksSinkOptions.SINK_LABEL_PREFIX.key());
            String labelPrefix = String.join("_", rawLabelPrefix, database, table);
            sinkConfig.put(StarRocksSinkOptions.SINK_LABEL_PREFIX.key(), labelPrefix);
        }
        return sinkConfig;
    }

    private void setIfNotExist(Map<String, String> config, ConfigOption<?> option, String value) {
        if (!config.containsKey(option.key())) {
            config.put(option.key(), value);
        }
    }

    private int executeUpdateStatement(String sql) throws SQLException {
        try (Connection connection = getConnection();
                Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sql);
        }
    }

    private List<String> executeSingleColumnStatement(String sql, Object... params) {
        try (Connection conn = getConnection();
                PreparedStatement statement = conn.prepareStatement(sql)) {
            List<String> columnValues = Lists.newArrayList();
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }
            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String columnValue = rs.getString(1);
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed to execute sql: %s", sql), e);
        }
    }

    private Connection getConnection() throws SQLException {
        // TODO reuse the connection
        return DriverManager.getConnection(jdbcUrl, username, password);
    }

    // ------ views ------

    @Override
    public List<String> listViews(String databaseName) throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------ partitions ------

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException,
            PartitionSpecInvalidException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> filters)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new PartitionNotExistException(getName(), tablePath, partitionSpec);
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        return false;
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
            PartitionSpecInvalidException, PartitionAlreadyExistsException,
            CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------ functions ------

    @Override
    public List<String> listFunctions(String dbName) throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        throw new FunctionNotExistException(getName(), functionPath);
    }


    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    // ------ statistics ------

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    String getJdbcUrl() {
        return jdbcUrl;
    }

    @VisibleForTesting
    String getHttpUrl() {
        return httpUrl;
    }

    @VisibleForTesting
    String getUsername() {
        return username;
    }

    @VisibleForTesting
    String getPassword() {
        return password;
    }

    @VisibleForTesting
    Configuration getSourceBaseConfig() {
        return sourceBaseConfig;
    }

    @VisibleForTesting
    Configuration getSinkBaseConfig() {
        return sinkBaseConfig;
    }

    @VisibleForTesting
    Configuration getTableBaseConfig() {
        return tableBaseConfig;
    }
}
