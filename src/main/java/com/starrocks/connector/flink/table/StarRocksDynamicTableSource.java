package com.starrocks.connector.flink.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
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

import com.starrocks.connector.flink.source.ColunmRichInfo;
import com.starrocks.connector.flink.source.SelectColumn;

public class StarRocksDynamicTableSource implements ScanTableSource, SupportsLimitPushDown, SupportsFilterPushDown, SupportsProjectionPushDown {

    private final TableSchema flinkSchema;
    private final StarRocksSourceOptions options;
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
        
        builder.setSourceOptions(options);
        
        List<ColunmRichInfo> colunmRichInfos = new ArrayList<>();
        List<TableColumn> tableColumns = this.flinkSchema.getTableColumns();
        for (int i = 0; i < tableColumns.size(); i ++) {
            TableColumn column = tableColumns.get(i);
            colunmRichInfos.add(new ColunmRichInfo(column.getName(), i, column.getType()));
        }
        builder.setColunmRichInfos(colunmRichInfos);
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

        // if columns = "*", this func will not be called, so 'selectColumns' will be null
        this.projectedFields = Arrays.stream(projectedFields).mapToInt(value -> value[0]).toArray();
        if (this.projectedFields.length == 0 ) {
            builder.setQueryType(StarRocksSourceQueryType.QueryCount);
            return;
        }

        builder.setQueryType(StarRocksSourceQueryType.QuerySomeColumns);

        ArrayList<String> columnList = new ArrayList<>();
        ArrayList<SelectColumn> selectColumns = new ArrayList<SelectColumn>(); 
        for (int index : this.projectedFields) {
            String columnName = flinkSchema.getFieldName(index).get();
            columnList.add(columnName);
            selectColumns.add(new SelectColumn(columnName, index, true));
        }
        String columns = String.join(", ", columnList);
        builder.setColumns(columns);
        builder.setSelectColumns(selectColumns.toArray(new SelectColumn[selectColumns.size()]));
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filtersExpressions) {

        List<String> filters = new ArrayList<>();
        StarRocksExpressionExtractor extractor = new StarRocksExpressionExtractor();
        for (ResolvedExpression expression : filtersExpressions) {
            if (expression.getOutputDataType().equals(DataTypes.BOOLEAN()) && expression.getChildren().size() == 0) {
                filters.add(expression.accept(extractor) + " = true");
                continue;
            }
            String str = expression.accept(extractor);
            if (str == null) {
                continue;
            }
            filters.add(str);
        }
        Optional<String> filter = Optional.of(String.join(" and ", filters));
        builder.setFilter(filter.get());
        return Result.of(filtersExpressions, new ArrayList<>());
    }

    @Override
    public void applyLimit(long limit) {
        builder.setLimit(limit);
    }
}
