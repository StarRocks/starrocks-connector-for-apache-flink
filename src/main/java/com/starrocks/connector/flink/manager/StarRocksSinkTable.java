package com.starrocks.connector.flink.manager;

import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionOptions;
import com.starrocks.connector.flink.connection.StarRocksJdbcConnectionProvider;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StarRocksSinkTable {

    private final String database;
    private final String table;

    private final StarRocksQueryVisitor starRocksQueryVisitor;

    private StarRocksSinkTable(Builder builder) {
        this.database = builder.database;
        this.table = builder.table;

        StarRocksJdbcConnectionOptions options =
                new StarRocksJdbcConnectionOptions(builder.jdbcUrl, builder.username, builder.password);
        StarRocksJdbcConnectionProvider jdbcConnectionProvider = new StarRocksJdbcConnectionProvider(options);
        this.starRocksQueryVisitor = new StarRocksQueryVisitor(jdbcConnectionProvider, builder.database, builder.table);
    }

    public static StarRocksSinkTable.Builder builder() {
        return new StarRocksSinkTable.Builder();
    }

    public String getVersion() {
        return starRocksQueryVisitor.getStarRocksVersion();
    }

    public void validateTableStructure(StarRocksSinkOptions sinkOptions, TableSchema flinkSchema) {
        if (flinkSchema == null) {
            return;
        }

        Optional<UniqueConstraint> constraint = flinkSchema.getPrimaryKey();
        List<Map<String, Object>> rows = starRocksQueryVisitor.getTableColumnsMetaData();
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("Couldn't get the sink table's column info.");
        }
        // validate primary keys
        List<String> primaryKeys = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            String keysType = row.get("COLUMN_KEY").toString();
            if (!"PRI".equals(keysType)) {
                continue;
            }
            primaryKeys.add(row.get("COLUMN_NAME").toString().toLowerCase());
        }
        if (!primaryKeys.isEmpty()) {
            if (!constraint.isPresent()) {
                throw new IllegalArgumentException("Primary keys not defined in the sink `TableSchema`.");
            }
            if (constraint.get().getColumns().size() != primaryKeys.size() ||
                    !constraint.get().getColumns().stream().allMatch(col -> primaryKeys.contains(col.toLowerCase()))) {
                throw new IllegalArgumentException("Primary keys of the flink `TableSchema` do not match with the ones from starrocks table.");
            }
            sinkOptions.enableUpsertDelete();
        }

        if (sinkOptions.hasColumnMappingProperty()) {
            return;
        }
        if (flinkSchema.getFieldCount() != rows.size()) {
            throw new IllegalArgumentException("Fields count of "+ sinkOptions.getTableName() + " mismatch. \nflinkSchema["
                    +flinkSchema.getFieldNames().length+"]:"
                    + String.join(",", flinkSchema.getFieldNames())
                    +"\n realTab["+rows.size()+"]:"
                    +rows.stream().map((r)-> String.valueOf(r.get("COLUMN_NAME"))).collect(Collectors.joining(",")));
        }
        List<TableColumn> flinkCols = flinkSchema.getTableColumns();
        for (Map<String, Object> row : rows) {
            String starrocksField = row.get("COLUMN_NAME").toString().toLowerCase();
            String starrocksType = row.get("DATA_TYPE").toString().toLowerCase();
            List<TableColumn> matchedFlinkCols = flinkCols.stream()
                    .filter(col -> col.getName().toLowerCase().equals(starrocksField) && (!typesMap.containsKey(starrocksType) || typesMap.get(starrocksType).contains(col.getType().getLogicalType().getTypeRoot())))
                    .collect(Collectors.toList());
            if (matchedFlinkCols.isEmpty()) {
                throw new IllegalArgumentException("Fields name or type mismatch for:" + starrocksField);
            }
        }
    }

    public static class Builder {
        private String database;
        private String table;
        private String jdbcUrl;
        private String username;
        private String password;

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder table(String table) {
            this.table = table;
            return this;
        }

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder sinkOptions(StarRocksSinkOptions sinkOptions) {
            this.jdbcUrl = sinkOptions.getJdbcUrl();
            this.username = sinkOptions.getUsername();
            this.password = sinkOptions.getPassword();
            this.database = sinkOptions.getDatabaseName();
            this.table = sinkOptions.getTableName();
            return this;
        }

        public StarRocksSinkTable build() {
            return new StarRocksSinkTable(this);
        }
    }

    private static final Map<String, List<LogicalTypeRoot>> typesMap = new HashMap<>();

    static {
        typesMap.put("bigint", Arrays.asList(LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("largeint", Arrays.asList(LogicalTypeRoot.DECIMAL, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("char", Arrays.asList(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR));
        typesMap.put("date", Arrays.asList(LogicalTypeRoot.DATE, LogicalTypeRoot.VARCHAR));
        typesMap.put("datetime", Arrays.asList(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE, LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE, LogicalTypeRoot.VARCHAR));
        typesMap.put("decimal", Arrays.asList(LogicalTypeRoot.DECIMAL, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.DOUBLE, LogicalTypeRoot.FLOAT));
        typesMap.put("double", Arrays.asList(LogicalTypeRoot.DOUBLE, LogicalTypeRoot.BIGINT, LogicalTypeRoot.INTEGER));
        typesMap.put("float", Arrays.asList(LogicalTypeRoot.FLOAT, LogicalTypeRoot.INTEGER));
        typesMap.put("int", Arrays.asList(LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("tinyint", Arrays.asList(LogicalTypeRoot.TINYINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY, LogicalTypeRoot.BOOLEAN));
        typesMap.put("smallint", Arrays.asList(LogicalTypeRoot.SMALLINT, LogicalTypeRoot.INTEGER, LogicalTypeRoot.BINARY));
        typesMap.put("varchar", Arrays.asList(LogicalTypeRoot.VARCHAR, LogicalTypeRoot.ARRAY, LogicalTypeRoot.MAP, LogicalTypeRoot.ROW));
        typesMap.put("string", Arrays.asList(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR, LogicalTypeRoot.ARRAY, LogicalTypeRoot.MAP, LogicalTypeRoot.ROW));
    }

}
