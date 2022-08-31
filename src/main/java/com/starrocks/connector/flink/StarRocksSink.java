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

import com.starrocks.connector.flink.row.StarRocksGenericRowTransformer;
import com.starrocks.connector.flink.row.StarRocksIRowTransformer;
import com.starrocks.connector.flink.row.StarRocksSinkRowBuilder;
import com.starrocks.connector.flink.table.StarRocksDynamicSinkFunctionV2;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;

public class StarRocksSink {

    /**
     * Create a StarRocks DataStream sink.
     * <p>
     * Note: the objects passed to the return sink can be processed in batch and retried.
     * Therefore, objects can not be {@link org.apache.flink.api.common.ExecutionConfig#enableObjectReuse() reused}.
     * </p>
     *
     * @param flinkTableSchema     TableSchema of the all columns with DataType
     * @param sinkOptions          StarRocksSinkOptions as the document listed, such as jdbc-url, load-url, batch size and maximum retries
     * @param rowDataTransformer   StarRocksSinkRowBuilder which would be used to transform the upstream record.
     * @param <T>                  type of data in {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord StreamRecord}.
     * @return SinkFunction        SinkFunction that could be add to a stream.
     */
    public static <T> SinkFunction<T> sink(
            TableSchema flinkTableSchema,
            StarRocksSinkOptions sinkOptions,
            StarRocksSinkRowBuilder<T> rowDataTransformer) {
        StarRocksIRowTransformer<T> rowTransformer =
                new StarRocksGenericRowTransformer<>(rowDataTransformer);
        return new StarRocksDynamicSinkFunctionV2<>(sinkOptions, flinkTableSchema, rowTransformer);
    }

    /**
     * Create a StarRocks DataStream sink, stream elements could only be String.
     * <p>
     * Note: the objects passed to the return sink can be processed in batch and retried.
     * Therefore, objects can not be {@link org.apache.flink.api.common.ExecutionConfig#enableObjectReuse() reused}.
     * </p>
     *
     * @param sinkOptions            StarRocksSinkOptions as the document listed, such as jdbc-url, load-url, batch size and maximum retries
     * @return SinkFunction          SinkFunction that could be add to a stream.
     */
    public static SinkFunction<String> sink(StarRocksSinkOptions sinkOptions) {
        return new StarRocksDynamicSinkFunctionV2<>(sinkOptions);
    }

    private StarRocksSink() {}
}
