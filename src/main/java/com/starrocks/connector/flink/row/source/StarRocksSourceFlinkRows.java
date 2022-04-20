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
import com.starrocks.connector.flink.table.source.struct.ColunmRichInfo;
import com.starrocks.connector.flink.table.source.struct.Const;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.table.source.struct.StarRocksSchema;
import com.starrocks.thrift.TScanBatchResult;
import com.starrocks.connector.flink.tools.DataUtil;

import org.apache.arrow.memory.RootAllocator;


import org.apache.arrow.vector.FieldVector;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;

import org.apache.flink.table.data.GenericRowData;

import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class StarRocksSourceFlinkRows {

    private static Logger LOG = LoggerFactory.getLogger(StarRocksSourceFlinkRows.class);
    private int offsetOfBatchForRead;
    private int rowCountOfBatch;
    private int flinkRowsCount;

    private List<GenericRowData> sourceFlinkRows = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private VectorSchemaRoot root;
    private Map<String, FieldVector> fieldVectorMap;
    private RootAllocator rootAllocator;
    private final List<ColunmRichInfo> colunmRichInfos;
    private final SelectColumn[] selectedColumns;
    private final StarRocksSchema starRocksSchema;

    public List<GenericRowData> getFlinkRows() {
        return sourceFlinkRows;
    }

    public StarRocksSourceFlinkRows(TScanBatchResult nextResult, List<ColunmRichInfo> colunmRichInfos, 
                                    StarRocksSchema srSchema, SelectColumn[] selectColumns) {
        this.colunmRichInfos = colunmRichInfos;
        this.selectedColumns = selectColumns;
        this.starRocksSchema = srSchema;
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        byte[] bytes = nextResult.getRows();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        this.arrowStreamReader = new ArrowStreamReader(byteArrayInputStream, rootAllocator);
        this.offsetOfBatchForRead = 0;
        this.fieldVectorMap = new HashMap<>();
    }

    public StarRocksSourceFlinkRows genFlinkRowsFromArrow() throws IOException {
        this.root = arrowStreamReader.getVectorSchemaRoot();
        while (arrowStreamReader.loadNextBatch()) {
            List<FieldVector> fieldVectors = root.getFieldVectors();
            fieldVectors.forEach(vector -> {
                fieldVectorMap.put(vector.getName(), vector);
            });
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
    
    private void setValueToFlinkRows(int rowIndex, int colunm, Object obj) {
        if (rowIndex > rowCountOfBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " + rowCountOfBatch;
            LOG.error("Get row offset: {} larger than row size: {}", rowIndex, rowCountOfBatch);
            throw new NoSuchElementException(errMsg);
        }
        sourceFlinkRows.get(rowIndex).setField(colunm, obj);
    }

    private void genFlinkRows() {
        final AtomicInteger index = new AtomicInteger(0);
        List<SelectColumn> selectedColumnsList = Arrays.asList(selectedColumns);
        selectedColumnsList.stream().map(column -> new Object[]{column, index.getAndAdd(1)})
        .collect(Collectors.toList())
        .parallelStream().forEach(columnAndIndex -> {
            SelectColumn column = (SelectColumn) columnAndIndex[0];
            int colIndex = (int) columnAndIndex[1];
            FieldVector columnVector = fieldVectorMap.get(column.getColumnName());
            if (null == columnVector) {
                throw new RuntimeException(
                    "Can not find StarRocks column data " +
                    "column name is -> [" + column.getColumnName() + "]"
                );
            } 
            Types.MinorType beShowDataType = columnVector.getMinorType();
            Column srColumn = starRocksSchema.get(column.getColumnName());
            if (null == srColumn) {
                throw new RuntimeException(
                    "Can not find StarRocks column info from open_scan result, " +
                    "column name is -> [" + column.getColumnName() + "]"
                );
            }
            String starrocksType = srColumn.getType(); 
            ColunmRichInfo richInfo = colunmRichInfos.get(column.getColumnIndexInFlinkTable());
            boolean nullable = richInfo.getDataType().getLogicalType().isNullable();
            LogicalTypeRoot flinkTypeRoot = richInfo.getDataType().getLogicalType().getTypeRoot();
            // starrocksType -> flinkType
            starrocksType = DataUtil.ClearBracket(starrocksType);
            if (!Const.DataTypeRelationMap.containsKey(flinkTypeRoot)) {
                throw new RuntimeException(
                    "Flink type not support when convert data from starrocks to flink, " +
                    "type is -> [" + flinkTypeRoot.toString() + "]"
                );
            }
            if (!Const.DataTypeRelationMap.get(flinkTypeRoot).containsKey(starrocksType)) {
                throw new RuntimeException(
                    "StarRocks type can not convert to flink type, " +
                    "starrocks type is -> [" + starrocksType + "] " + 
                    "flink type is -> [" + flinkTypeRoot.toString() + "]"
                );
            }
            StarRocksToFlinkTrans translators = Const.DataTypeRelationMap.get(flinkTypeRoot).get(starrocksType);
            Object[] result = translators.transToFlinkData(beShowDataType, columnVector, rowCountOfBatch, colIndex, nullable);
            for (int i = 0; i < result.length; i ++) {
                setValueToFlinkRows(i, colIndex, result[i]);
            }
        });
    }
}