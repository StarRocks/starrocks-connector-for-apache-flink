package com.dorisdb;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;

import com.dorisdb.row.DorisGenericRowTransformer;
import com.dorisdb.table.DorisDynamicSinkFunction;
import com.dorisdb.table.DorisSinkOptions;

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
        DorisGenericRowTransformer.RowConsumer<T> rowDataTransformer) {
            return new DorisDynamicSinkFunction<>(
                sinkOptions,
                flinkTableSchema,
                new DorisGenericRowTransformer<>(rowDataTransformer)
            );
    }

    private DorisSink() {}
}
