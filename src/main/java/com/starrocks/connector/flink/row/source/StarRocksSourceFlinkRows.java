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

import com.starrocks.connector.flink.table.source.struct.Column;
import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.Const;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.table.source.struct.StarRocksSchema;
import com.starrocks.connector.flink.tools.DataTypeUtils;
import com.starrocks.connector.flink.tools.DataUtil;
import com.starrocks.thrift.TScanBatchResult;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

public class StarRocksSourceFlinkRows {

    private static Logger LOG = LoggerFactory.getLogger(StarRocksSourceFlinkRows.class);
    private int offsetOfBatchForRead;
    private int rowCountOfBatch;
    private int flinkRowsCount;

    private List<GenericRowData> sourceFlinkRows = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;
    private List<ColumnRichInfo> columnRichInfos;
    private final SelectColumn[] selectedColumns;
    private final StarRocksSchema starRocksSchema;

    public List<GenericRowData> getFlinkRows() {
        return sourceFlinkRows;
    }

    public StarRocksSourceFlinkRows(TScanBatchResult nextResult,
                                    List<ColumnRichInfo> columnRichInfos,
                                    StarRocksSchema srSchema,
                                    SelectColumn[] selectColumns) {
        this.columnRichInfos = columnRichInfos;
        this.selectedColumns = selectColumns;
        this.starRocksSchema = srSchema;
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        byte[] bytes = nextResult.getRows();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        this.arrowStreamReader = new ArrowStreamReader(byteArrayInputStream, rootAllocator);
        this.offsetOfBatchForRead = 0;
    }

    public StarRocksSourceFlinkRows genFlinkRowsFromArrow() throws IOException {
        this.root = arrowStreamReader.getVectorSchemaRoot();
        while (arrowStreamReader.loadNextBatch()) {
            fieldVectors = root.getFieldVectors();
            if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                continue;
            }
            rowCountOfBatch = root.getRowCount();
            for (int i = 0; i < rowCountOfBatch; i ++) {
                sourceFlinkRows.add(new GenericRowData(this.selectedColumns.length));
            }
            this.genFlinkRows();
            flinkRowsCount += root.getRowCount();
        }
        return this;
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
    
    private void setValueToFlinkRows(int rowIndex, int column, Object obj) {
        if (rowIndex > rowCountOfBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " + rowCountOfBatch;
            LOG.error("Get row offset: {} larger than row size: {}", rowIndex, rowCountOfBatch);
            throw new NoSuchElementException(errMsg);
        }
        sourceFlinkRows.get(rowIndex).setField(column, obj);
    }
    
    private void genFlinkRows() {
        for (int i = 0; i < fieldVectors.size(); i++) {
            FieldVector fieldVector = fieldVectors.get(i);
            StarRocksToFlinkTrans translators = null;
            Column column = starRocksSchema.getColumn(i);
            boolean nullable = true;
            if (column != null) {
                ColumnRichInfo richInfo = columnRichInfos.get(selectedColumns[i].getColumnIndexInFlinkTable());
                nullable = richInfo.getDataType().getLogicalType().isNullable();
                LogicalTypeRoot flinkTypeRoot = richInfo.getDataType().getLogicalType().getTypeRoot();
                String srType = DataUtil.clearBracket(column.getType());
                if (Const.DataTypeRelationMap.containsKey(flinkTypeRoot)
                        && Const.DataTypeRelationMap.get(flinkTypeRoot).containsKey(srType)) {
                    translators = Const.DataTypeRelationMap.get(flinkTypeRoot).get(srType);
                }
            }

            // TODO make sure what's going on here
            if (translators == null) {
                DataType dataType = DataTypeUtils.map(fieldVector.getMinorType());
                if (dataType == null) {
                    throw new RuntimeException(
                            "Flink type not support when convert data from starrocks to flink, " +
                                    "type is -> [" + fieldVector.getMinorType().toString() + "]"
                    );
                }
                HashMap<String, StarRocksToFlinkTrans> map = Const.DataTypeRelationMap.get(dataType.getLogicalType().getTypeRoot());
                translators = map.values().stream().findFirst().orElse(null);
            }

            if (translators != null) {
                Object[] result = translators.transToFlinkData(fieldVector.getMinorType(), fieldVector, rowCountOfBatch, i, nullable);
                for (int ri = 0; ri < result.length; ri ++) {
                    setValueToFlinkRows(ri, i, result[ri]);
                }
            }
        }
    }

}