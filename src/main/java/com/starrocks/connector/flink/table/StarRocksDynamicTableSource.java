package com.starrocks.connector.flink.table;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.starrocks.connector.flink.source.SelectColumn;

public class StarRocksDynamicTableSource implements ScanTableSource, SupportsLimitPushDown, SupportsFilterPushDown, SupportsProjectionPushDown {

    private final TableSchema flinkSchema;
    private final StarRocksSourceOptions options;
    private long limit;
    private String filter;
    private String columns;
    private int[] projectedFields;
    private StarRocksRowDataInputFormat.Builder builder = StarRocksRowDataInputFormat.builder();

    public StarRocksDynamicTableSource(StarRocksSourceOptions options, TableSchema schema, StarRocksRowDataInputFormat.Builder builder) {
        this.options = options;
        this.flinkSchema = schema;
        this.builder = builder;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        
        builder.setFlinkDataTypes(this.flinkSchema.getFieldDataTypes()).
                setSourceOptions(options);
        StarRocksRowDataInputFormat format = builder.build();
        return InputFormatProvider.of(format);
    }

    @Override
    public DynamicTableSource copy() {
        return new StarRocksDynamicTableSource(this.options, this.flinkSchema, this.builder);
    }

    @Override
    public String asSummaryString() {
        return "StarRocks Table Source";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.projectedFields = Arrays.stream(projectedFields).mapToInt(value -> value[0]).toArray();
        ArrayList<String> columnList = new ArrayList<>();
        ArrayList<SelectColumn> selectColumns = new ArrayList<SelectColumn>(); 
        for (int index : this.projectedFields) {
            String columnName = flinkSchema.getFieldName(index).get();
            columnList.add(columnName);
            selectColumns.add(new SelectColumn(columnName, index));
        }
        String columns = String.join(", ", columnList);
        this.columns = columns;
        builder.setColumns(columns);
        builder.setSelectColumns(selectColumns.toArray(new SelectColumn[selectColumns.size()]));
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filtersExpressions) {

        List<String> filters = new ArrayList<>();
        StarRocksExpressionExtractor extractor = new StarRocksExpressionExtractor();
        for (ResolvedExpression expression : filtersExpressions) {
            String str = expression.accept(extractor);
            if (str == null) {
                continue;
            }
            filters.add(str);
        }
        Optional<String> filter = Optional.of(String.join(" and ", filters));
        this.filter = filter.get();
        builder.setFilter(filter.get());
        return Result.of(filtersExpressions, new ArrayList<>());
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
        builder.setLimit(limit);
    }
}
