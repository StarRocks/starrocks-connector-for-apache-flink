package com.starrocks.connector.flink.table;

import com.google.common.base.Strings;
import com.starrocks.connector.flink.manager.StarRocksSinkBufferEntity;
import com.starrocks.connector.flink.manager.StarRocksSinkManagerV2;
import com.starrocks.connector.flink.manager.StarRocksSinkTable;
import com.starrocks.connector.flink.row.StarRocksIRowTransformer;
import com.starrocks.connector.flink.row.StarRocksISerializer;
import com.starrocks.connector.flink.row.StarRocksSerializerFactory;
import com.starrocks.connector.flink.table.data.StarRocksRowData;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.truncate.Truncate;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.NestedRowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class StarRocksDynamicSinkFunctionV2<T> extends StarRocksDynamicSinkFunctionBase<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksDynamicSinkFunctionV2.class);

    private static final int NESTED_ROW_DATA_HEADER_SIZE = 256;

    private final StarRocksSinkOptions sinkOptions;
    private final StarRocksSinkManagerV2 sinkManager;
    private final StarRocksISerializer serializer;
    private final StarRocksIRowTransformer<T> rowTransformer;

    private transient volatile ListState<Tuple2<Long, StreamLoadSnapshot>> snapshotStates;
    private final Map<Long, StreamLoadSnapshot> snapshotMap = new ConcurrentHashMap<>();

    private transient ListState<Map<String, StarRocksSinkBufferEntity>> legacyState;
    private transient List<StarRocksSinkBufferEntity> legacyData;

    public StarRocksDynamicSinkFunctionV2(StarRocksSinkOptions sinkOptions,
                                          TableSchema schema,
                                          StarRocksIRowTransformer<T> rowTransformer) {
        this.sinkOptions = sinkOptions;
        this.rowTransformer = rowTransformer;
        rowTransformer.setTableSchema(schema);
        this.serializer = StarRocksSerializerFactory.createSerializer(sinkOptions, schema.getFieldNames());
        StarRocksSinkTable sinkTable = StarRocksSinkTable.builder()
                .sinkOptions(sinkOptions)
                .build();
        sinkTable.validateTableStructure(sinkOptions, schema);
        this.sinkManager = new StarRocksSinkManagerV2(sinkOptions.getProperties());
    }

    public StarRocksDynamicSinkFunctionV2(StarRocksSinkOptions sinkOptions) {
        this.sinkOptions = sinkOptions;
        this.sinkManager = new StarRocksSinkManagerV2(sinkOptions.getProperties());
        this.serializer = null;
        this.rowTransformer = null;
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (serializer == null) {
            if (value instanceof StarRocksSinkRowDataWithMeta) {
                StarRocksSinkRowDataWithMeta data = (StarRocksSinkRowDataWithMeta) value;
                if (Strings.isNullOrEmpty(data.getDatabase())
                        || Strings.isNullOrEmpty(data.getTable())
                        || data.getDataRows() == null) {
                    LOG.warn(String.format("json row data not fulfilled. {database: %s, table: %s, dataRows: %s}",
                            data.getDatabase(), data.getTable(), Arrays.toString(data.getDataRows())));
                    return;
                }
                sinkManager.write(null, data.getDatabase(), data.getTable(), data.getDataRows());
                return;
            } else if (value instanceof StarRocksRowData) {
                StarRocksRowData data = (StarRocksRowData) value;
                if (Strings.isNullOrEmpty(data.getDatabase())
                        || Strings.isNullOrEmpty(data.getTable())
                        || data.getRow() == null) {
                    LOG.warn(String.format("json row data not fulfilled. {database: %s, table: %s, dataRows: %s}",
                            data.getDatabase(), data.getTable(), data.getRow()));
                    return;
                }
                sinkManager.write(data.getUniqueKey(), data.getDatabase(), data.getTable(), data.getRow());
                return;
            }
            // raw data sink
            sinkManager.write(null, sinkOptions.getDatabaseName(), sinkOptions.getTableName(), value.toString());
            return;
        }

        if (value instanceof NestedRowData) {
            NestedRowData ddlData = (NestedRowData) value;
            if (ddlData.getSegments().length != 1 || ddlData.getSegments()[0].size() < NESTED_ROW_DATA_HEADER_SIZE) {
                return;
            }

            int totalSize = ddlData.getSegments()[0].size();
            byte[] data = new byte[totalSize - NESTED_ROW_DATA_HEADER_SIZE];
            ddlData.getSegments()[0].get(NESTED_ROW_DATA_HEADER_SIZE, data);
            Map<String, String> ddlMap = InstantiationUtil.deserializeObject(data, HashMap.class.getClassLoader());
            if (ddlMap == null
                    || "true".equals(ddlMap.get("snapshot"))
                    || Strings.isNullOrEmpty(ddlMap.get("ddl"))
                    || Strings.isNullOrEmpty(ddlMap.get("databaseName"))) {
                return;
            }
            Statement statement = CCJSqlParserUtil.parse(ddlMap.get("ddl"));
            if (statement instanceof Truncate) {
                Truncate truncate = (Truncate) statement;
                if (!sinkOptions.getTableName().equalsIgnoreCase(truncate.getTable().getName())) {
                    return;
                }
                // TODO: add ddl to queue
            } else if (statement instanceof Alter) {

            }
        }
        if (value instanceof RowData) {
            if (RowKind.UPDATE_BEFORE.equals(((RowData)value).getRowKind())) {
                // do not need update_before, cauz an update action happened on the primary keys will be separated into `delete` and `create`
                return;
            }
            if (!sinkOptions.supportUpsertDelete() && RowKind.DELETE.equals(((RowData)value).getRowKind())) {
                // let go the UPDATE_AFTER and INSERT rows for tables who have a group of `unique` or `duplicate` keys.
                return;
            }
        }
        flushLegacyData();
        sinkManager.write(
                null,
                sinkOptions.getDatabaseName(),
                sinkOptions.getTableName(),
                serializer.serialize(rowTransformer.transform(value, sinkOptions.supportUpsertDelete()))
        );
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        sinkManager.init();
        sinkManager.setRuntimeContext(getRuntimeContext(), sinkOptions);
        if (rowTransformer != null) {
            rowTransformer.setRuntimeContext(getRuntimeContext());
        }

        for (Tuple2<Long, StreamLoadSnapshot> tuple : snapshotStates.get()) {
            if (!sinkManager.commit(tuple.f1)) {
                snapshotMap.put(tuple.f0, tuple.f1);
            }
        }

        if (!snapshotMap.isEmpty()) {
            String labels = snapshotMap.values().stream()
                    .map(StreamLoadSnapshot::getTransactions)
                    .flatMap(Collection::stream)
                    .map(tx -> tx.getDatabase() + "-" + tx.getLabel())
                    .collect(Collectors.joining(","));

            throw new RuntimeException(String.format("Open failed, some labels are not commit (%s)", labels));
        }

        LOG.info("Open sink function v2");
    }

    public void finish() {
        sinkManager.flush();
        StreamLoadSnapshot snapshot = sinkManager.snapshot();
        sinkManager.commit(snapshot);
    }

    @Override
    public void close() {
        sinkManager.flush();
        sinkManager.close();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        sinkManager.flush();
        StreamLoadSnapshot snapshot = sinkManager.snapshot();

        if (sinkManager.prepare(snapshot)) {
            snapshotMap.put(functionSnapshotContext.getCheckpointId(), snapshot);

            snapshotStates.clear();
            snapshotStates.add(Tuple2.of(functionSnapshotContext.getCheckpointId(), snapshot));
        } else {
            throw new RuntimeException("Snapshot state failed by prepare");
        }

        if (legacyState != null) {
            legacyState.clear();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Tuple2<Long, StreamLoadSnapshot>> descriptor =
                new ListStateDescriptor<>(
                        "starrocks-sink-transaction",
                        TypeInformation.of(new TypeHint<Tuple2<Long, StreamLoadSnapshot>>() {})
                );
        snapshotStates = functionInitializationContext.getOperatorStateStore().getListState(descriptor);

        if (sinkOptions.getSemantic() == StarRocksSinkSemantic.EXACTLY_ONCE) {
            ListStateDescriptor<Map<String, StarRocksSinkBufferEntity>> legacyDescriptor =
                    new ListStateDescriptor<>(
                            "buffered-rows",
                            TypeInformation.of(new TypeHint<Map<String, StarRocksSinkBufferEntity>>(){})
                    );
            legacyState = functionInitializationContext.getOperatorStateStore().getListState(legacyDescriptor);
            legacyData = new ArrayList<>();
            for (Map<String, StarRocksSinkBufferEntity> entry : legacyState.get()) {
                legacyData.addAll(entry.values());
            }
            LOG.info("There are {} items from legacy state", legacyData.size());
        }
    }

    private void flushLegacyData() {
        if (legacyData.isEmpty()) {
            return;
        }

        for (StarRocksSinkBufferEntity entity : legacyData) {
            for (byte[] data : entity.getBuffer()) {
                sinkManager.write(null, entity.getDatabase(), entity.getTable(), new String(data, StandardCharsets.UTF_8));
            }
            LOG.info("Write {} legacy records from table '{}' of database '{}'",
                    entity.getBuffer().size(), entity.getDatabase(), entity.getTable());
        }
        legacyData.clear();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        StreamLoadSnapshot snapshot = snapshotMap.get(checkpointId);
        if (snapshot == null) {
            LOG.warn("Unknown checkpoint id : {}", checkpointId);
            return;
        }

        if (sinkManager.commit(snapshot)) {
            snapshotMap.remove(checkpointId);
        } else {
            LOG.error("checkpoint complete failed, id : {}", checkpointId);
            throw new RuntimeException("checkpoint complete failed, id : " + checkpointId);
        }

        // set legacyState to null to avoid clear it in latter snapshotState
        legacyState = null;
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        StreamLoadSnapshot snapshot = snapshotMap.get(checkpointId);
        if (snapshot == null) {
            LOG.warn("Unknown checkpoint id : {}", checkpointId);
            return;
        }

        sinkManager.abort(snapshot);
        snapshotMap.remove(checkpointId);
    }
}
