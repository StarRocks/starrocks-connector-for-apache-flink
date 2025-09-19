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

import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.tools.EnvUtils;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class StarRocksDynamicLookupFunction extends LookupFunction {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicLookupFunction.class);

    private final int maxRetryTimes;
    private final ColumnRichInfo[] filterRichInfos;
    private final List<ColumnRichInfo> columnRichInfos;
    private final StarRocksSourceOptions sourceOptions;
    private final SelectColumn[] selectColumns;
    private final StarRocksJdbcConnectionProvider jdbcConnProvider;
    private final String query;

    private PreparedStatement statement;

    public StarRocksDynamicLookupFunction(StarRocksSourceOptions sourceOptions,
                                          ColumnRichInfo[] filterRichInfos,
                                          List<ColumnRichInfo> columnRichInfos,
                                          SelectColumn[] selectColumns,
                                          StarRocksJdbcConnectionProvider jdbcConnProvider) {
        this.maxRetryTimes = sourceOptions.getScanMaxRetries();
        this.sourceOptions = sourceOptions;
        this.filterRichInfos = filterRichInfos;
        this.columnRichInfos = columnRichInfos;
        this.selectColumns = selectColumns;
        this.jdbcConnProvider = jdbcConnProvider;
        this.query = genPreparedSQL();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        statement = jdbcConnProvider.getConnection().prepareStatement(query);
        LOG.info("Open lookup function. {}", EnvUtils.getGitInformation());
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (statement != null) {
            statement.close();
        }
    }

    @Override
    public Collection<RowData> lookup(RowData rowData) {
        if (rowData.getArity() != filterRichInfos.length) {
            throw new IllegalArgumentException(String.format(
                    "RowData contains different size of columns from filterRichInfos, expect %d, actual %d.",
                    filterRichInfos.length, rowData.getArity()));
        }
        for (int retry = 0; retry <= maxRetryTimes; ++retry) {
            try {
                statement.clearParameters();
                for (int index = 0; index < rowData.getArity(); index++) {
                    serialize(filterRichInfos[index].getDataType(), rowData, index, statement);
                }
                try (ResultSet resultSet = statement.executeQuery()) {
                    ArrayList<RowData> rows = new ArrayList<>();
                    while (resultSet.next()) {
                        RowData row = buildRowData(resultSet);
                        rows.add(row);
                    }
                    rows.trimToSize();
                    return rows;
                }
            } catch (SQLException e) {
                LOG.error("JDBC executeBatch error, retry times = {}", retry, e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }

                try {
                    statement.close();
                    Connection conn = jdbcConnProvider.reestablishConnection();
                    statement = conn.prepareStatement(query);
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

    private String genPreparedSQL() {
        String columns = Arrays.stream(selectColumns)
                .map(col -> "`" + col.getColumnName() + "`")
                .collect(Collectors.joining(","));
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("select %s from `%s`.`%s`", columns,
                sourceOptions.getDatabaseName(), sourceOptions.getTableName()));
        if (filterRichInfos != null && filterRichInfos.length > 0) {
            sb.append(" where ").append(Arrays.stream(filterRichInfos)
                    .map(info -> String.format("%s = ?", info.getColumnName()))
                    .collect(Collectors.joining(" and ")));
        }
        String sql = sb.toString();
        LOG.info("LookUpFunction SQL [{}]", sql);
        return sql;
    }

    private void serialize(DataType dataType, RowData val, int index, PreparedStatement statement) throws SQLException {
        LogicalType type = dataType.getLogicalType();
        int pos = index + 1;
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                statement.setBoolean(pos, val.getBoolean(index));
                break;
            case TINYINT:
                statement.setByte(pos, val.getByte(index));
                break;
            case SMALLINT:
                statement.setShort(pos, val.getShort(index));
                break;
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                statement.setInt(pos, val.getInt(index));
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                statement.setLong(pos, val.getLong(index));
                break;
            case FLOAT:
                statement.setFloat(pos, val.getFloat(index));
                break;
            case DOUBLE:
                statement.setDouble(pos, val.getDouble(index));
                break;
            case CHAR:
            case VARCHAR:
                // value is BinaryString
                statement.setString(pos, val.getString(index).toString());
                break;
            case BINARY:
            case VARBINARY:
                statement.setBytes(pos, val.getBinary(index));
                break;
            case DATE:
                statement.setDate(pos, Date.valueOf(LocalDate.ofEpochDay(val.getInt(index))));
                break;
            case TIME_WITHOUT_TIME_ZONE:
                statement.setTime(
                        pos,
                        Time.valueOf(LocalTime.ofNanoOfDay(val.getInt(index) * 1_000_000L)));
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                final int timestampPrecision = ((TimestampType) type).getPrecision();
                statement.setTimestamp(
                        pos, val.getTimestamp(index, timestampPrecision).toTimestamp());
                break;
            case DECIMAL:
                final int decimalPrecision = ((DecimalType) type).getPrecision();
                final int decimalScale = ((DecimalType) type).getScale();
                statement.setBigDecimal(
                        pos,
                        val.getDecimal(index, decimalPrecision, decimalScale).toBigDecimal());
                break;
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private Object deserialize(DataType dataType, Object field) {
        LogicalType type = dataType.getLogicalType();
        switch (type.getTypeRoot()) {
            case NULL:
                return null;
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
            case INTEGER:
            case BIGINT:
            case BINARY:
            case VARBINARY:
                return field;
            case TINYINT:
                return ((Integer) field).byteValue();
            case SMALLINT:
                // Converter for small type that casts value to int and then return short value,
                // since
                // JDBC 1.0 use int type for small values.
                return field instanceof Integer ? ((Integer) field).shortValue() : field;
            case DECIMAL:
                final int precision = ((DecimalType) type).getPrecision();
                final int scale = ((DecimalType) type).getScale();
                // using decimal(20, 0) to support db type bigint unsigned, user should define
                // decimal(20, 0) in SQL,
                // but other precision like decimal(30, 0) can work too from lenient consideration.
                return field instanceof BigInteger
                                ? DecimalData.fromBigDecimal(
                                new BigDecimal((BigInteger) field, 0), precision, scale)
                                : DecimalData.fromBigDecimal((BigDecimal) field, precision, scale);
            case DATE:
                return (int) (((Date) field).toLocalDate().toEpochDay());
            case TIME_WITHOUT_TIME_ZONE:
                return (int) (((Time) field).toLocalTime().toNanoOfDay() / 1_000_000L);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return field instanceof LocalDateTime
                        ? TimestampData.fromLocalDateTime((LocalDateTime) field)
                        : TimestampData.fromTimestamp((Timestamp) field);
            case CHAR:
            case VARCHAR:
                return StringData.fromString((String) field);
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    private RowData buildRowData(ResultSet resultSet) throws SQLException {
        GenericRowData genericRowData = new GenericRowData(selectColumns.length);
        for (int col = 0; col < selectColumns.length; ++col) {
            Object field = resultSet.getObject(col + 1);
            int colIndex = selectColumns[col].getColumnIndexInFlinkTable();
            if (colIndex < 0 || colIndex > columnRichInfos.size()) {
                throw new RuntimeException(String.format(
                        "Cannot get ColumnRichInfo from selected column, since its index %d is invalid.", colIndex));
            }
            ColumnRichInfo colInfo = columnRichInfos.get(colIndex);
            genericRowData.setField(col, deserialize(colInfo.getDataType(), field));
        }
        return genericRowData;
    }
}
