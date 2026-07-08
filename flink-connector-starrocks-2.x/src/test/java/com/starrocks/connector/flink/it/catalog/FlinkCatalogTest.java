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

package com.starrocks.connector.flink.it.catalog;

import com.starrocks.connector.flink.catalog.FlinkCatalog;
import com.starrocks.connector.flink.it.StarRocksITTestBase;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.starrocks.connector.flink.catalog.TypeUtils.MAX_VARCHAR_SIZE;
import static com.starrocks.connector.flink.catalog.TypeUtils.STRING_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FlinkCatalogTest extends StarRocksITTestBase {

    @Test
    public void testDatabase() throws Exception {
        FlinkCatalog catalog = createCatalog("test_db_catalog");
        String db1 = "sr_test_" + genRandomUuid();
        String db2 = "sr_test_" + genRandomUuid();
        String db3 = "sr_test_" + genRandomUuid();
        createDatabase(db1);
        createDatabase(db2);

        List<String> expectDbList = listDatabase();
        expectDbList.sort(String::compareTo);
        List<String> actualDbList = catalog.listDatabases();
        actualDbList.sort(String::compareTo);
        assertEquals(expectDbList, actualDbList);

        assertNotNull(catalog.getDatabase(db1));
        assertNotNull(catalog.getDatabase(db2));
        try {
            catalog.getDatabase(db3);
            fail("Should fail because the db does not exist");
        } catch (Exception e) {
            assertTrue(e instanceof DatabaseNotExistException);
        }

        assertTrue(catalog.databaseExists(db1));
        assertTrue(catalog.databaseExists(db2));
        assertFalse(catalog.databaseExists(db3));

        DATABASE_SET_TO_CLEAN.add(db3);
        catalog.createDatabase(db3, new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
        assertTrue(catalog.databaseExists(db3));
        // test ignoreIfExists is true for already existed db
        catalog.createDatabase(db3, new CatalogDatabaseImpl(Collections.emptyMap(), null), true);
        try {
            catalog.createDatabase(db3, new CatalogDatabaseImpl(Collections.emptyMap(), null), false);
            fail("Should fail because the db already exists");
        } catch (Exception e) {
            assertTrue(e instanceof DatabaseAlreadyExistException);
        }
    }

    @Test
    public void testListAndExistTable() throws Exception {
        FlinkCatalog catalog = createCatalog("test_table_catalog");

        String db = "sr_test_" + genRandomUuid();
        DATABASE_SET_TO_CLEAN.add(db);
        createDatabase(db);
        assertTrue(catalog.databaseExists(db));

        String tbl1 = createAllTypesTable(db, "tbl1");
        String tbl2 = createAllTypesTable(db, "tbl2");
        String tbl3 = "tbl3_" + genRandomUuid();

        List<String> expectTables = Arrays.asList(tbl1, tbl2);
        expectTables.sort(String::compareTo);
        List<String> actualTables = catalog.listTables(db);
        actualTables.sort(String::compareTo);
        assertEquals(expectTables, actualTables);
        String dbNotExist = "sr_test_" + genRandomUuid();
        try {
            catalog.listTables(dbNotExist);
            fail("Should failed because the db doest not exist");
        } catch (Exception e) {
            assertTrue(e instanceof DatabaseNotExistException);
        }

        assertTrue(catalog.tableExists(new ObjectPath(db, tbl1)));
        assertTrue(catalog.tableExists(new ObjectPath(db, tbl2)));
        assertFalse(catalog.tableExists(new ObjectPath(db, tbl3)));
        assertFalse(catalog.tableExists(new ObjectPath(dbNotExist, tbl1)));
    }

    @Test
    public void testGetTable() throws Exception {
        FlinkCatalog catalog = createCatalog("test_table_catalog");

        String db = "sr_test_" + genRandomUuid();
        DATABASE_SET_TO_CLEAN.add(db);
        catalog.createDatabase(db, new CatalogDatabaseImpl(Collections.emptyMap(), null), true);
        assertTrue(catalog.databaseExists(db));

        String tbl = createAllTypesTable(db, "tbl");
        Schema flinkSchema = Schema.newBuilder().fromResolvedSchema(createAllTypesFlinkSchema()).build();

        assertTrue(catalog.tableExists(new ObjectPath(db, tbl)));
        CatalogBaseTable table = catalog.getTable(new ObjectPath(db, tbl));
        assertNotNull(table);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (int i = 0; i < 11; i++) {
            Schema.UnresolvedPhysicalColumn column = (Schema.UnresolvedPhysicalColumn) flinkSchema.getColumns().get(i);
            schemaBuilder.column(column.getName(), column.getDataType());
        }
        schemaBuilder.column("c11", DataTypes.VARCHAR(STRING_SIZE));
        for (int i = 12; i < 15; i++) {
            Schema.UnresolvedPhysicalColumn column = (Schema.UnresolvedPhysicalColumn) flinkSchema.getColumns().get(i);
            schemaBuilder.column(column.getName(), column.getDataType());
        }
        schemaBuilder.primaryKey(flinkSchema.getPrimaryKey().get().getColumnNames());
        Schema expectFlinkSchema = schemaBuilder.build();
        Schema actualFlinkSchema = table.getUnresolvedSchema();

        assertEquals(expectFlinkSchema.getColumns(), actualFlinkSchema.getColumns());
        assertTrue(actualFlinkSchema.getPrimaryKey().isPresent());
        assertEquals(expectFlinkSchema.getPrimaryKey().get().getColumnNames(),
                actualFlinkSchema.getPrimaryKey().get().getColumnNames());

        String tblNotExist = "tbl_" + genRandomUuid();
        try {
            catalog.getTable(new ObjectPath(db, tblNotExist));
            fail("Should fail because the table does not exist");
        } catch (Exception e) {
            assertTrue(e instanceof TableNotExistException);
        }
    }

    @Test
    public void testCreateTable() throws Exception {
        Configuration tableConf = new Configuration();
        tableConf.setString("table.properties.replication_num", "1");
        FlinkCatalog catalog = createCatalog("test_table_catalog", tableConf);

        String db = "sr_test_" + genRandomUuid();
        DATABASE_SET_TO_CLEAN.add(db);
        catalog.createDatabase(db, new CatalogDatabaseImpl(Collections.emptyMap(), null), true);
        assertTrue(catalog.databaseExists(db));
        String tbl = "tbl_" + genRandomUuid();
        ObjectPath objectPath = new ObjectPath(db, tbl);
        ResolvedSchema flinkSchema = createAllTypesFlinkSchema();
        CatalogTable catalogTable = CatalogTable.newBuilder()
                .schema(org.apache.flink.table.api.Schema.newBuilder()
                        .fromResolvedSchema(flinkSchema)
                        .build())
                .options(Collections.emptyMap())
                .build();
        ResolvedCatalogTable resolvedCatalogTable = new ResolvedCatalogTable(catalogTable, flinkSchema);
        assertFalse(catalog.tableExists(objectPath));
        catalog.createTable(objectPath, resolvedCatalogTable, false);
        assertTrue(catalog.tableExists(objectPath));
        CatalogBaseTable actualCatalogTable = catalog.getTable(objectPath);
        List<Column> tableColumns = new ArrayList<>(flinkSchema.getColumns());
        tableColumns.set(5, Column.physical(tableColumns.get(5).getName(), DataTypes.VARCHAR(MAX_VARCHAR_SIZE)));
        tableColumns.set(11, Column.physical(tableColumns.get(11).getName(), DataTypes.VARCHAR(MAX_VARCHAR_SIZE)));
        tableColumns.set(14, Column.physical(tableColumns.get(14).getName(), DataTypes.VARCHAR(MAX_VARCHAR_SIZE)));
        ResolvedSchema expectedResolved = new ResolvedSchema(
            tableColumns,
            Collections.<WatermarkSpec>emptyList(),
            flinkSchema.getPrimaryKey().orElse(null));
        Schema expectFlinkSchema = Schema.newBuilder().fromResolvedSchema(expectedResolved).build();
        Schema actualFlinkSchema = actualCatalogTable.getUnresolvedSchema();
        assertEquals(expectFlinkSchema.getColumns(), actualFlinkSchema.getColumns());
        assertTrue(actualFlinkSchema.getPrimaryKey().isPresent());
        assertEquals(expectFlinkSchema.getPrimaryKey().get().getColumnNames(),
                actualFlinkSchema.getPrimaryKey().get().getColumnNames());

        try {
            catalog.createTable(objectPath, resolvedCatalogTable, false);
            fail("Should fail because the table exists");
        } catch (Exception e) {
            assertTrue(e instanceof TableAlreadyExistException);
        }

        String dbNotExist = "sr_test_" + genRandomUuid();
        ObjectPath objectPathNotExist = new ObjectPath(dbNotExist, tbl);
        try {
            catalog.createTable(objectPathNotExist, resolvedCatalogTable, false);
            fail("Should fail because the database does not exist");
        } catch (Exception e) {
            assertTrue(e instanceof DatabaseNotExistException);
        }
    }

    @Test
    public void testDropTable() throws Exception {
        FlinkCatalog catalog = createCatalog("test_table_catalog");

        String db = "sr_test_" + genRandomUuid();
        DATABASE_SET_TO_CLEAN.add(db);
        catalog.createDatabase(db, new CatalogDatabaseImpl(Collections.emptyMap(), null), true);
        assertTrue(catalog.databaseExists(db));
        String tbl = createAllTypesTable(db, "tbl");

        ObjectPath objectPath = new ObjectPath(db, tbl);
        assertTrue(catalog.tableExists(objectPath));
        catalog.dropTable(objectPath, false);
        assertFalse(catalog.tableExists(objectPath));
        // test ignoreIfNotExists
        catalog.dropTable(objectPath, true);
        try {
            catalog.dropTable(objectPath, false);
            fail("Should fail because the table does not exist");
        } catch (Exception e) {
            assertTrue(e instanceof TableNotExistException);
        }
    }

    private String createAllTypesTable(String database, String tablePrefix) throws Exception {
        String tableName = tablePrefix + "_" + genRandomUuid();
        String createSql = String.format(
                "CREATE TABLE `%s`.`%s` (" +
                    "c0 BOOLEAN," +
                    "c1 TINYINT," +
                    "c2 SMALLINT," +
                    "c3 INT," +
                    "c4 BIGINT," +
                    "c5 LARGEINT," +
                    "c6 FLOAT," +
                    "c7 DOUBLE," +
                    "c8 DECIMAL(23,5)," +
                    "c9 CHAR(7)," +
                    "c10 VARCHAR(109)," +
                    "c11 STRING," +
                    "c12 DATE," +
                    "c13 DATETIME," +
                    "c14 JSON" +
                ") ENGINE = OLAP " +
                "PRIMARY KEY(c0,c1,c2,c3) " +
                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                "PROPERTIES (" +
                    "\"replication_num\" = \"1\"" +
                ")",
                database, tableName);
        executeSrSQL(createSql);
        return tableName;
    }

    private ResolvedSchema createAllTypesFlinkSchema() {
        return new ResolvedSchema(
            Arrays.asList(
                Column.physical("c0", DataTypes.BOOLEAN().notNull()),
                Column.physical("c1", DataTypes.TINYINT().notNull()),
                Column.physical("c2", DataTypes.SMALLINT().notNull()),
                Column.physical("c3", DataTypes.INT().notNull()),
                Column.physical("c4", DataTypes.BIGINT()),
                Column.physical("c5", DataTypes.STRING()),
                Column.physical("c6", DataTypes.FLOAT()),
                Column.physical("c7", DataTypes.DOUBLE()),
                Column.physical("c8", DataTypes.DECIMAL(23, 5)),
                Column.physical("c9", DataTypes.CHAR(7)),
                Column.physical("c10", DataTypes.VARCHAR(109)),
                Column.physical("c11", DataTypes.STRING()),
                Column.physical("c12", DataTypes.DATE()),
                Column.physical("c13", DataTypes.TIMESTAMP(0)),
                Column.physical("c14", DataTypes.STRING())),
            Collections.<WatermarkSpec>emptyList(),
            UniqueConstraint.primaryKey("pk", Arrays.asList("c0", "c1", "c2", "c3")));
    }

    private FlinkCatalog createCatalog(String catalogName) {
        return createCatalog(catalogName, new Configuration());
    }

    private FlinkCatalog createCatalog(String catalogName, Configuration tableConf) {
        return new FlinkCatalog(
                catalogName,
                getJdbcUrl(),
                getHttpUrls(),
                "root",
                "",
                DB_NAME,
                new Configuration(),
                new Configuration(),
                tableConf,
                Thread.currentThread().getContextClassLoader()
        );
    }

    @Test
    public void testInsertAndSelect() throws Exception {
        String tableName = createPkTable("test_read_write");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        String catalogName = "test_catalog";
        String createCatalogSql = String.format(
                "CREATE CATALOG %s WITH (" +
                        "       'type' = 'starrocks',\n" +
                        "       'jdbc-url' = '%s',\n" +
                        "       'http-url' = '%s',\n" +
                        "       'username' = '%s',\n" +
                        "       'password' = '%s',\n" +
                        "       'default-database' = '%s'" +
                        ");",
                catalogName, getJdbcUrl(), getHttpUrls(), "root", "", "default-db");
        tEnv.executeSql(createCatalogSql);
        tEnv.executeSql(String.format("USE CATALOG %s;", catalogName));
        tEnv.executeSql(String.format("USE %s;", DB_NAME));
        tEnv.executeSql(
                String.format(
                    "INSERT INTO %s VALUES (1, '100'), (2, '200');",
                    tableName
                ))
            .await();

        List<Row> results =
            CollectionUtil.iteratorToList(
                tEnv.sqlQuery(
                    String.format(
                        "SELECT * FROM %s",
                        tableName))
                .execute()
                .collect());
        results.sort(Comparator.comparingInt(row -> (int) row.getField(0)));

        List<Row> expectRows = Arrays.asList(
                Row.ofKind(RowKind.INSERT, 1, "100"),
                Row.ofKind(RowKind.INSERT, 2, "200")
            );

       assertThat(results).isEqualTo(expectRows);
    }

    private String createPkTable(String tablePrefix) throws Exception {
        String tableName = tablePrefix + "_" + genRandomUuid();
        String createStarRocksTable =
                String.format(
                        "CREATE TABLE `%s`.`%s` (" +
                                "c0 INT," +
                                "c1 STRING" +
                                ") ENGINE = OLAP " +
                                "PRIMARY KEY(c0) " +
                                "DISTRIBUTED BY HASH (c0) BUCKETS 8 " +
                                "PROPERTIES (" +
                                "\"replication_num\" = \"1\"" +
                                ")",
                        DB_NAME, tableName);
        executeSrSQL(createStarRocksTable);
        return tableName;
    }
}
