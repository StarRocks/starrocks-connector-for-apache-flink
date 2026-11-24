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

package com.starrocks.connector.flink.row.sink;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.LogicalType;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions.UnavailableValueOnInsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Decides which columns a row of a flexible partial update writes. In this mode a key missing from
 * the JSON row keeps the column's current value and an explicit null writes NULL, so a column must
 * be left out exactly when the row has no value to write for it:
 *
 * <ul>
 *   <li>a non key column holding the CDC source's unavailable value, such as an unchanged PostgreSQL
 *       TOAST column;
 *   <li>every non key column of a delete, which the source may send as null. The server validates
 *       delete rows too, so a null would get the row filtered when the column is NOT NULL.
 * </ul>
 */
public final class FlexibleRowProjection implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(FlexibleRowProjection.class);

    /** Stands in for the value of a column that the JSON row leaves out. */
    public static final Object ABSENT = new Object() {
        @Override
        public String toString() {
            return "<absent>";
        }
    };

    static final String COUNTER_UNAVAILABLE_VALUE_INSERT_ROWS = "totalUnavailableValueInsertRows";
    private static final long WARN_INTERVAL_MS = 60_000L;

    private final String tableName;
    private final String[] fieldNames;
    private final boolean[] keyColumn;
    @Nullable
    private final String unavailableValue;
    @Nullable
    private final byte[] unavailableBytes;
    private final UnavailableValueOnInsert onInsert;
    private final boolean sanitizeLog;

    private transient BinaryStringData unavailableString;
    private transient Counter unavailableInsertRows;
    private transient long lastWarnTimeMs;
    private transient long rowsSinceLastWarn;

    private FlexibleRowProjection(String tableName, String[] fieldNames, boolean[] keyColumn,
                                  @Nullable String unavailableValue, UnavailableValueOnInsert onInsert,
                                  boolean sanitizeLog) {
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.keyColumn = keyColumn;
        this.unavailableValue = unavailableValue;
        // Debezium's placeholder for a binary column is the bytes of the same string.
        this.unavailableBytes = unavailableValue == null ? null : unavailableValue.getBytes(StandardCharsets.UTF_8);
        this.onInsert = onInsert;
        this.sanitizeLog = sanitizeLog;
    }

    public static FlexibleRowProjection create(StarRocksSinkOptions sinkOptions, TableSchema schema) {
        String[] fieldNames = schema.getFieldNames();
        Set<String> keys = new HashSet<>(schema.getPrimaryKey()
                .map(UniqueConstraint::getColumns)
                .orElse(Collections.emptyList()));
        boolean[] keyColumn = new boolean[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            keyColumn[i] = keys.contains(fieldNames[i]);
        }
        return new FlexibleRowProjection(
                sinkOptions.getDatabaseName() + "." + sinkOptions.getTableName(),
                fieldNames,
                keyColumn,
                sinkOptions.getFlexiblePartialUpdateUnavailableValue(),
                sinkOptions.getFlexiblePartialUpdateUnavailableValueOnInsert(),
                sinkOptions.isSanitizeErrorLog());
    }

    /**
     * Refuses non key columns whose unchanged TOAST value cannot be told from NULL. Debezium 1.9 has no
     * placeholder for an array, map or row: converting one fails, it logs the error and delivers NULL,
     * and the sink would then overwrite the stored value with it.
     *
     * <p>A numeric value has the same problem, but PostgreSQL only stores one out of line as a last
     * resort for a row that does not fit in a page. Refusing DECIMAL would rule out most tables for that
     * rare case, so these columns are allowed and reported in the log instead.
     */
    public static void checkColumnTypes(TableSchema schema) {
        Set<String> keys = new HashSet<>(schema.getPrimaryKey()
                .map(UniqueConstraint::getColumns)
                .orElse(Collections.emptyList()));
        List<String> refused = new ArrayList<>();
        List<String> decimals = new ArrayList<>();
        for (TableColumn column : schema.getTableColumns()) {
            if (keys.contains(column.getName())) {
                continue;
            }
            switch (column.getType().getLogicalType().getTypeRoot()) {
                case ARRAY:
                case MAP:
                case MULTISET:
                case ROW:
                    refused.add(column.getName());
                    break;
                case DECIMAL:
                    decimals.add(column.getName());
                    break;
                default:
                    break;
            }
        }
        if (!decimals.isEmpty()) {
            LOG.warn("'{}' cannot protect DECIMAL columns {}: if PostgreSQL stored a numeric value out of line, which "
                    + "it only does for a row that does not fit in a page, Debezium sends NULL instead of a "
                    + "placeholder when an UPDATE leaves the value unchanged, and the sink writes that NULL.",
                    StarRocksSinkOptions.SINK_FLEXIBLE_PARTIAL_UPDATE_UNAVAILABLE_VALUE.key(), decimals);
        }
        if (!refused.isEmpty()) {
            throw new IllegalArgumentException("'"
                    + StarRocksSinkOptions.SINK_FLEXIBLE_PARTIAL_UPDATE_UNAVAILABLE_VALUE.key()
                    + "' cannot protect columns " + refused + ": Debezium sends NULL instead of a placeholder for "
                    + "an unchanged TOAST value of an array, map or row, and the sink would overwrite the stored "
                    + "value with it. Remove these columns from the Flink table so they keep their values, or set "
                    + "REPLICA IDENTITY FULL on the source table and remove this option.");
        }
    }

    public void open(@Nullable MetricGroup metricGroup) {
        if (metricGroup != null) {
            this.unavailableInsertRows = metricGroup.counter(COUNTER_UNAVAILABLE_VALUE_INSERT_ROWS);
        }
    }

    /** Whether the column of a {@link RowData} is left out. Reads the raw value, before any json is parsed. */
    boolean leavesOut(LogicalType type, RowData row, int pos, boolean deleteRow) {
        if (keyColumn[pos]) {
            return false;
        }
        if (deleteRow) {
            return true;
        }
        if (unavailableValue == null || row.isNullAt(pos)) {
            return false;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return isUnavailable(row.getString(pos));
            case BINARY:
            case VARBINARY:
                return Arrays.equals(unavailableBytes, row.getBinary(pos));
            default:
                return false;
        }
    }

    /** Marks the left out columns of a row whose values were already built by the user's row builder. */
    void project(Object[] values, boolean supportUpsertDelete) {
        boolean deleteRow = supportUpsertDelete && isDeleteOp(values[values.length - 1]);
        for (int i = 0; i < keyColumn.length; i++) {
            if (!keyColumn[i] && (deleteRow || isUnavailable(values[i]))) {
                values[i] = ABSENT;
            }
        }
    }

    /**
     * Handles an INSERT that carries the unavailable value. It is one of two things the sink cannot tell
     * apart: the first change of a key the job has not seen, which ChangelogNormalize emits as an INSERT
     * and which leaves the column's value in place, or the new row of an UPDATE that changed the primary
     * key, where no stored value exists and the column takes its default.
     */
    void checkInsert(Object[] values) {
        if (unavailableValue == null) {
            return;
        }
        List<String> columns = null;
        for (int i = 0; i < keyColumn.length; i++) {
            if (values[i] == ABSENT) {
                if (columns == null) {
                    columns = new ArrayList<>();
                }
                columns.add(fieldNames[i]);
            }
        }
        if (columns == null) {
            return;
        }
        String row = "An INSERT into " + tableName + describeKey(values) + " carries the unavailable value in "
                + "columns " + columns + ".";
        if (onInsert == UnavailableValueOnInsert.FAIL) {
            throw new IllegalStateException(row + " Either the source changed this row's primary key, so the new "
                    + "row has no stored value for them, or this is the first change the job sees for this key. "
                    + "Set REPLICA IDENTITY FULL on the source table, or set '"
                    + StarRocksSinkOptions.SINK_FLEXIBLE_PARTIAL_UPDATE_UNAVAILABLE_VALUE_ON_INSERT.key()
                    + "=log' to write the row without these columns.");
        }
        if (unavailableInsertRows != null) {
            unavailableInsertRows.inc();
        }
        rowsSinceLastWarn++;
        long now = System.currentTimeMillis();
        if (lastWarnTimeMs == 0 || now - lastWarnTimeMs >= WARN_INTERVAL_MS) {
            LOG.warn("{} The row is written without them, so they keep their current value, or take their default "
                    + "if the source changed the row's primary key. {} such rows since the last report.",
                    row, rowsSinceLastWarn);
            lastWarnTimeMs = now;
            rowsSinceLastWarn = 0;
        }
    }

    private String describeKey(Object[] values) {
        if (sanitizeLog) {
            return "";
        }
        StringBuilder key = new StringBuilder(" with key [");
        boolean first = true;
        for (int i = 0; i < keyColumn.length; i++) {
            if (keyColumn[i]) {
                key.append(first ? "" : ", ").append(fieldNames[i]).append('=').append(values[i]);
                first = false;
            }
        }
        return key.append(']').toString();
    }

    private boolean isUnavailable(StringData value) {
        if (unavailableString == null) {
            unavailableString = BinaryStringData.fromString(unavailableValue);
        }
        return unavailableString.equals(value);
    }

    private boolean isUnavailable(@Nullable Object value) {
        if (unavailableValue == null || value == null) {
            return false;
        }
        if (value instanceof StringData) {
            return isUnavailable((StringData) value);
        }
        if (value instanceof CharSequence) {
            return unavailableValue.contentEquals((CharSequence) value);
        }
        if (value instanceof byte[]) {
            return Arrays.equals(unavailableBytes, (byte[]) value);
        }
        return false;
    }

    private static boolean isDeleteOp(@Nullable Object op) {
        if (op instanceof Number) {
            return ((Number) op).intValue() == StarRocksSinkOP.DELETE.ordinal();
        }
        return op != null && String.valueOf(StarRocksSinkOP.DELETE.ordinal()).equals(op.toString().trim());
    }
}
