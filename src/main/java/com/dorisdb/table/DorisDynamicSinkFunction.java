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

package com.dorisdb.table;

import com.dorisdb.row.DorisIRowTransformer;
import com.dorisdb.manager.DorisSinkManager;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;

import java.util.List;

public class DorisDynamicSinkFunction<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private DorisSinkManager sinkManager;
    private DorisIRowTransformer<T> rowTransformer;
    private DorisSinkOptions sinkOptions;

    // state only works with `DorisSinkSemantic.EXACTLY_ONCE`
    private transient ListState<Tuple2<String, List<String>>> checkpointedState;
 
    public DorisDynamicSinkFunction(DorisSinkOptions sinkOptions, TableSchema schema, DorisIRowTransformer<T> rowTransformer) {
        rowTransformer.setTableSchema(schema);
        this.rowTransformer = rowTransformer;
        this.sinkOptions = sinkOptions;
        this.sinkManager = new DorisSinkManager(sinkOptions, schema);
    }
 
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        rowTransformer.setRuntimeContext(getRuntimeContext());
        sinkManager.startScheduler();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (DorisSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            // flush the batch saved at last checkpoint state first    
            for (Tuple2<String, List<String>> state : checkpointedState.get()) {
                sinkManager.setBufferedBatchList(state.f1);
                sinkManager.flush(state.f0);
            }
            checkpointedState.clear();
        }
        sinkManager.writeRecord(rowTransformer.transform(value));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        if (!DorisSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            return;
        }
        ListStateDescriptor<Tuple2<String, List<String>>> descriptor =
            new ListStateDescriptor<>(
                "buffered-rows",
                TypeInformation.of(new TypeHint<Tuple2<String, List<String>>>(){})
            );
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (DorisSinkSemantic.EXACTLY_ONCE.equals(sinkOptions.getSemantic())) {
            // save state
            checkpointedState.clear();
            checkpointedState.add(new Tuple2<>(sinkManager.createBatchLabel(), sinkManager.getBufferedBatchList()));
            return;
        }
        sinkManager.flush(sinkManager.createBatchLabel());
    }

    @Override
    public void close() throws Exception {
        sinkManager.close();
        super.close();
    }
}