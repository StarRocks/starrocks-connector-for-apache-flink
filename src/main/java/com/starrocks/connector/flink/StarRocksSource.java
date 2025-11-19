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

import com.starrocks.connector.flink.table.source.StarRocksDynamicSourceFunction;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import org.apache.flink.table.legacy.api.TableSchema;


public class StarRocksSource {
    
    /**
     * Create a StarRocks DataStream source.
     *
     * @param sourceOptions     StarRocksSourceOptions as the document listed, such as http-nodes, load-url, batch size and maximum retries
     * @param flinkSchema       FlinkSchema
     * @return StarRocksDynamicSourceFunction Function of RichParallelSourceFunction
     */
    public static StarRocksDynamicSourceFunction source(TableSchema flinkSchema, StarRocksSourceOptions sourceOptions) {
        
        return new StarRocksDynamicSourceFunction(flinkSchema, sourceOptions);
    }
}
