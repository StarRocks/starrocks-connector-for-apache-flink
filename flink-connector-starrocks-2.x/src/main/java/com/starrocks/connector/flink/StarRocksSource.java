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

package com.starrocks.connector.flink;

import com.starrocks.connector.flink.table.source.StarRocksFlinkSource;
import com.starrocks.connector.flink.table.source.StarRocksSourceCommonFunc;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;

import java.util.Map;


public class StarRocksSource {

    /**
     * Create a StarRocks DataStream source.
     *
     * @param flinkSchema       FlinkSchema
     * @param sourceOptions     StarRocksSourceOptions as the document listed, such as http-nodes, load-url, batch size and maximum retries
     * @return Source           FLIP-27 Source for use with env.fromSource()
     */
    public static Source<RowData, ?, ?> source(ResolvedSchema flinkSchema, StarRocksSourceOptions sourceOptions) {
        // Mirror the 1.x DataStream entry: scan.filter and scan.columns come from the options
        String filter = sourceOptions.getFilter().isEmpty() ? null : sourceOptions.getFilter();
        SelectColumn[] selectColumns = null;
        if (!sourceOptions.getColumns().trim().toLowerCase().startsWith("count(")) {
            Map<String, ColumnRichInfo> columnMap = StarRocksSourceCommonFunc.genColumnMap(flinkSchema);
            selectColumns = StarRocksSourceCommonFunc.genSelectedColumns(
                    columnMap, sourceOptions, StarRocksSourceCommonFunc.genColumnRichInfo(columnMap));
        }
        return new StarRocksFlinkSource(sourceOptions, flinkSchema, filter, -1, selectColumns, null);
    }
}
