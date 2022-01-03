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

package com.starrocks.connector.flink.table.source.struct;

import java.io.Serializable;

import org.apache.flink.table.types.DataType;

public class ColunmRichInfo implements Serializable {

    private final String columnName;
    private final int colunmIndexInSchema;
    private final DataType dataType;


    public ColunmRichInfo(String columnName, int colunmIndexInSchema, DataType dataType) {
        this.columnName = columnName;
        this.colunmIndexInSchema = colunmIndexInSchema;
        this.dataType = dataType;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public int getColunmIndexInSchema() {
        return this.colunmIndexInSchema;
    }

    public DataType getDataType() {
        return this.dataType;
    }
}
