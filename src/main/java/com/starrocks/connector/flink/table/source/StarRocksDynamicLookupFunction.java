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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.converter.JdbcRowConverter;
import com.starrocks.connector.flink.dialect.MySqlDialect;
import com.starrocks.connector.flink.statement.FieldNamedPreparedStatement;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.tools.EnvUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksDynamicLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicLookupFunction.class);

    private final StarRocksSourceOptions sourceOptions;
    private QueryInfo queryInfo;
    private final SelectColumn[] selectColumns;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;

    // cache for lookup data
    private final Cache<RowData, List<RowData>> cacheMap;

    private final String[] keyNames;

    private final String query;

    private final transient StarRocksJdbcConnectionProvider connectionProvider;

    private transient FieldNamedPreparedStatement statement;

    private final JdbcRowConverter lookupKeyRowConverter;

    private final JdbcRowConverter jdbcRowConverter;

    public StarRocksDynamicLookupFunction(StarRocksSourceOptions sourceOptions,
        SelectColumn[] selectColumns
    ) {
        this.sourceOptions = sourceOptions;
        this.selectColumns = selectColumns;

        this.cacheMaxSize = sourceOptions.getLookupCacheMaxRows();
        this.cacheExpireMs = sourceOptions.getLookupCacheTTL();
        this.maxRetryTimes = sourceOptions.getLookupMaxRetries();

        this.cacheMap = CacheBuilder.newBuilder()
            .maximumSize(cacheMaxSize)
            .expireAfterWrite(Duration.ofMillis(cacheExpireMs))
            .build();
        this.keyNames = null;
        MySqlDialect mySqlDialect = new MySqlDialect();
        this.query = mySqlDialect.getSelectFromStatement(sourceOptions.getTableName(), null,
            null);
        connectionProvider = new StarRocksJdbcConnectionProvider(
            new StarRocksJdbcConnectionOptions(
                sourceOptions.getJdbcUrl(), sourceOptions.getUsername(),
                sourceOptions.getPassword()));
        this.jdbcRowConverter = mySqlDialect.getRowConverter(null);
        this.lookupKeyRowConverter = mySqlDialect.getRowConverter(null);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        establishConnectionAndStatement();
        LOG.info("Open lookup function. {}", EnvUtils.getGitInformation());
    }

    public void eval(Object... keys) {
        GenericRowData keyRow = GenericRowData.of(keys);
        if (cacheMap != null) {

            List<RowData> curList = cacheMap.getIfPresent(keyRow);
            if (curList != null) {
                curList.parallelStream().forEach(this::collect);
                return;
            }
        }
        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            // query
            try {
                statement.clearParameters();
                statement = lookupKeyRowConverter.toExternal(keyRow, statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    ArrayList<RowData> rows = new ArrayList<>();
                    while (resultSet.next()) {
                        RowData rowData = jdbcRowConverter.toInternal(resultSet);
                        rows.add(rowData);
                    }
                    rows.trimToSize();
                    rows.parallelStream().forEach(this::collect);
                    break;
                }
            } catch (SQLException e) {
                LOG.error("query StarRocks error", e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Exception of StarRocks lookup failed.", e);
                }
                try {
                    if (!connectionProvider.getConnection().isValid(10)) {
                        statement.close();
                        connectionProvider.close();
                        establishConnectionAndStatement();
                    }
                } catch (SQLException | ClassNotFoundException exception) {
                    LOG.error("Jdbc Connection is not valid, and reestablish connection failed",
                        exception);
                    throw new RuntimeException("Reestablish JDBC connection failed", exception);
                }
                try {
                    Thread.sleep(1000L * retry);
                } catch (InterruptedException ie) {
                    throw new RuntimeException(ie);
                }
            }

        }
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
