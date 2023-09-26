/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.table.source;

import static org.apache.flink.util.Preconditions.checkArgument;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.converter.JdbcRowConverter;
import com.starrocks.connector.flink.dialect.MySqlDialect;
import com.starrocks.connector.flink.statement.FieldNamedPreparedStatement;
import com.starrocks.connector.flink.tools.EnvUtils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksDynamicLookupFunction extends LookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicLookupFunction.class);

    private final StarRocksSourceOptions sourceOptions;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    // cache for lookup data
    private final String[] keyNames;

    private final String query;

    private final transient StarRocksJdbcConnectionProvider connectionProvider;

    private transient FieldNamedPreparedStatement statement;

    private final JdbcRowConverter lookupKeyRowConverter;

    private final JdbcRowConverter jdbcRowConverter;

    public StarRocksDynamicLookupFunction(StarRocksSourceOptions sourceOptions,
        String[] fieldNames,
        DataType[] fieldTypes,
        String[] keyNames,
        RowType rowType
    ) {
        this.sourceOptions = sourceOptions;

        this.cacheMaxSize = sourceOptions.getLookupCacheMaxRows();
        this.cacheExpireMs = sourceOptions.getLookupCacheTTL();
        this.maxRetryTimes = sourceOptions.getLookupMaxRetries();

        this.keyNames = keyNames;
        MySqlDialect mySqlDialect = new MySqlDialect();
        this.query = mySqlDialect.getSelectFromStatement(sourceOptions.getTableName(), fieldNames,
            keyNames);
        connectionProvider = new StarRocksJdbcConnectionProvider(
            new StarRocksJdbcConnectionOptions(
                sourceOptions.getJdbcUrl(), sourceOptions.getUsername(),
                sourceOptions.getPassword()));
        this.jdbcRowConverter = mySqlDialect.getRowConverter(rowType);
        List<String> nameList = Arrays.asList(fieldNames);
        DataType[] keyTypes =
            Arrays.stream(keyNames)
                .map(
                    s -> {
                        checkArgument(
                            nameList.contains(s),
                            "keyName %s can't find in fieldNames %s.",
                            s,
                            nameList);
                        return fieldTypes[nameList.indexOf(s)];
                    })
                .toArray(DataType[]::new);

        this.lookupKeyRowConverter = mySqlDialect.getRowConverter(RowType.of(
            Arrays.stream(keyTypes)
                .map(DataType::getLogicalType)
                .toArray(LogicalType[]::new)));

    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        establishConnectionAndStatement();
        LOG.info("Open lookup function. {}", EnvUtils.getGitInformation());
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        for (int retry = 0; retry <= maxRetryTimes; retry++) {
            try {
                statement.clearParameters();
                statement = lookupKeyRowConverter.toExternal(keyRow, statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    ArrayList<RowData> rows = new ArrayList<>();
                    while (resultSet.next()) {
                        RowData row = jdbcRowConverter.toInternal(resultSet);
                        rows.add(row);
                    }
                    rows.trimToSize();
                    return rows;
                }
            } catch (SQLException e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }

                try {
                    if (!connectionProvider.isConnectionValid()) {
                        statement.close();
                        connectionProvider.close();
                        establishConnectionAndStatement();
                    }
                } catch (SQLException | ClassNotFoundException exception) {
                    LOG.error(
                        "JDBC connection is not valid, and reestablish connection failed",
                        exception);
                    throw new RuntimeException("Reestablish JDBC connection failed", exception);
                }

                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
        return Collections.emptyList();
    }


    @Override
    public void close() throws Exception {
        connectionProvider.close();
        super.close();
    }

    private void establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
        Connection dbConn = connectionProvider.getConnection();
        statement = FieldNamedPreparedStatement.prepareStatement(dbConn, query, keyNames);
    }
}
