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

package com.dorisdb.connector.flink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;

import com.dorisdb.connector.flink.row.DorisGenericRowTransformer;
import com.dorisdb.connector.flink.row.DorisSinkRowBuilder;
import com.dorisdb.connector.flink.table.DorisDynamicSinkFunction;
import com.dorisdb.connector.flink.table.DorisSinkOptions;

public class DorisSink {

    /**
     * Create a Doris DataStream sink.
     * <p>
     * Note: the objects passed to the return sink can be processed in batch and retried.
     * Therefore, objects can not be {@link org.apache.flink.api.common.ExecutionConfig#enableObjectReuse() reused}.
     * </p>
     *
     * @param flinkTableSchema  TableSchema of the all columns with DataType
     * @param sinkOptions          DorisSinkOptions as the document listed, such as jdbc-url, load-url, batch size and maximum retries
     * @param <T>               type of data in {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord StreamRecord}.
     */
    public static <T> SinkFunction<T> sink(
        TableSchema flinkTableSchema,
        DorisSinkOptions sinkOptions,
        DorisSinkRowBuilder<T> rowDataTransformer) {
            return new DorisDynamicSinkFunction<>(
                sinkOptions,
                flinkTableSchema,
                new DorisGenericRowTransformer<>(rowDataTransformer)
            );
    }

    private DorisSink() {}
}
