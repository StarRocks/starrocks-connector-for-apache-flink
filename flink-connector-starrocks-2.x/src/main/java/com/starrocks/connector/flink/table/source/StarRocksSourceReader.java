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

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryPlan;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/** FLIP-27 SourceReader that reads data from StarRocks BE nodes, one split at a time. */
public class StarRocksSourceReader extends SingleThreadMultiplexSourceReaderBase<
        GenericRowData, RowData, StarRocksSourceSplit, StarRocksSourceSplit> {

    public StarRocksSourceReader(
            StarRocksSourceOptions sourceOptions,
            List<ColumnRichInfo> columnRichInfos,
            SelectColumn[] selectColumns,
            Callable<QueryPlan> planner,
            SourceReaderContext readerContext) {
        super(
                () -> new StarRocksSplitReader(sourceOptions, columnRichInfos, selectColumns, planner),
                new StarRocksRecordEmitter(),
                readerContext.getConfiguration(),
                readerContext);
    }

    @Override
    public void start() {
        // a reader restored with a split finishes it before asking for another
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, StarRocksSourceSplit> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected StarRocksSourceSplit initializedState(StarRocksSourceSplit split) {
        return split;
    }

    @Override
    protected StarRocksSourceSplit toSplitType(String splitId, StarRocksSourceSplit splitState) {
        return splitState;
    }
}
