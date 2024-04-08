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

package com.starrocks.connector.flink.row.source;

import org.apache.flink.table.data.GenericRowData;

import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.thrift.TScanBatchResult;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StarRocksSourceFlinkRows {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSourceFlinkRows.class);
    private int offsetOfBatchForRead;
    private int flinkRowsCount;

    private final List<GenericRowData> sourceFlinkRows = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private VectorSchemaRoot root;
    private final RootAllocator rootAllocator;
    private final List<ColumnRichInfo> columnRichInfos;
    private final SelectColumn[] selectedColumns;

    public StarRocksSourceFlinkRows(TScanBatchResult nextResult,
                                    List<ColumnRichInfo> columnRichInfos,
                                    SelectColumn[] selectColumns) {
        this.columnRichInfos = columnRichInfos;
        this.selectedColumns = selectColumns;
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        byte[] bytes = nextResult.getRows();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        this.arrowStreamReader = new ArrowStreamReader(byteArrayInputStream, rootAllocator);
        this.offsetOfBatchForRead = 0;
    }

    public void init(List<ArrowFieldConverter> fieldConverters) throws IOException {
        this.root = arrowStreamReader.getVectorSchemaRoot();
        initFiledConverters(fieldConverters);
        while (arrowStreamReader.loadNextBatch()) {
            List<FieldVector> fieldVectors = root.getFieldVectors();
            if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                continue;
            }
            int rowCountOfBatch = root.getRowCount();
            for (int i = 0; i < rowCountOfBatch; i ++) {
                sourceFlinkRows.add(new GenericRowData(this.selectedColumns.length));
            }
            for (int i = 0; i < fieldVectors.size(); i++) {
                FieldVector fieldVector = fieldVectors.get(i);
                ArrowFieldConverter converter = fieldConverters.get(i);
                for (int rowIndex = 0; rowIndex < rowCountOfBatch; rowIndex++) {
                    try {
                        Object fieldValue = converter.convert(fieldVector, rowIndex);
                        sourceFlinkRows.get(flinkRowsCount + rowIndex).setField(i, fieldValue);
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to convert arrow data for field " + fieldVector.getField().getName(), e);
                    }
                }
            }
            flinkRowsCount += rowCountOfBatch;
        }
    }

    private void initFiledConverters(List<ArrowFieldConverter> fieldConverters) {
        if (!fieldConverters.isEmpty()) {
            return;
        }

        Schema schema = root.getSchema();
        for (int i = 0; i < schema.getFields().size(); i++) {
            Field field = schema.getFields().get(i);
            ColumnRichInfo flinkColumn = columnRichInfos.get(selectedColumns[i].getColumnIndexInFlinkTable());
            ArrowFieldConverter converter = ArrowFieldConverter.createConverter(
                    flinkColumn.getDataType().getLogicalType(), field);
            fieldConverters.add(converter);
        }
    }

    public boolean hasNext() {
        if (offsetOfBatchForRead < flinkRowsCount) {
            return true;
        }
        this.close();
        return false;
    }

    public GenericRowData next() {
        if (!hasNext()) {
            LOG.error("offset larger than flinksRowsCount");
            throw new RuntimeException("read offset larger than flinksRowsCount");
        }
        return sourceFlinkRows.get(offsetOfBatchForRead ++);
    }

    public int getReadRowCount() {
        return flinkRowsCount;
    }

    private void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close StarRocksSourceFlinkRows:" + e.getMessage());
            throw new RuntimeException("Failed to close StarRocksSourceFlinkRows:" + e.getMessage());
        }
    }
}