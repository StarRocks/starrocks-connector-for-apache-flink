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

import com.starrocks.connector.flink.table.source.StarRocksSourceQueryType;

public class PushDownHolder {

    private String filter = "";
    private long limit;
    private SelectColumn[] selectColumns; 
    private String columns;
    private StarRocksSourceQueryType queryType;

    public String getFilter() {
        return filter;
    }
    public void setFilter(String filter) {
        this.filter = filter;
    }
    public long getLimit() {
        return limit;
    }
    public void setLimit(long limit) {
        this.limit = limit;
    }
    public SelectColumn[] getSelectColumns() {
        return selectColumns;
    }
    public void setSelectColumns(SelectColumn[] selectColumns) {
        this.selectColumns = selectColumns;
    }
    public String getColumns() {
        return columns;
    }
    public void setColumns(String columns) {
        this.columns = columns;
    }
    public StarRocksSourceQueryType getQueryType() {
        return queryType;
    }
    public void setQueryType(StarRocksSourceQueryType queryType) {
        this.queryType = queryType;
    }
}
