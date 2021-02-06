package com.dorisdb.table;

import com.dorisdb.row.DorisIRowTransformer;
import com.dorisdb.manager.DorisSinkManager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;

import java.io.IOException;

public class DorisDynamicSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private DorisSinkManager sinkManager;
    private DorisIRowTransformer<T> rowTransformer;
 
    public DorisDynamicSinkFunction(DorisSinkOptions sinkOptions, TableSchema schema, DorisIRowTransformer<T> rowTransformer) {
		rowTransformer.setTableSchema(schema);
        this.rowTransformer = rowTransformer;
		this.sinkManager = new DorisSinkManager(sinkOptions, schema);
    }
 
    @Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
        rowTransformer.setRuntimeContext(getRuntimeContext());
		sinkManager.startScheduler();
	}

	@Override
	public void invoke(T value, Context context) throws IOException {
        sinkManager.writeRecord(rowTransformer.transform(value));
	}

	@Override
	public void initializeState(FunctionInitializationContext context) {
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		sinkManager.flush();
	}

	@Override
	public void close() throws Exception {
        sinkManager.close();
		super.close();
    }
}