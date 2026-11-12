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

import com.starrocks.connector.flink.table.source.StarRocksSourceQueryType;

public class PushDownHolder implements Serializable {

    private static final long serialVersionUID = 1L;

    private String filter = "";
    private long limit;
    private SelectColumn[] selectColumns; 
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
    public StarRocksSourceQueryType getQueryType() {
        return queryType;
    }
    public void setQueryType(StarRocksSourceQueryType queryType) {
        this.queryType = queryType;
    }

    public PushDownHolder copy() {
        PushDownHolder holder = new PushDownHolder();
        holder.setLimit(limit);
        holder.setFilter(filter);
        if (selectColumns != null) {
            holder.setSelectColumns(selectColumns.clone());
        }
        if (queryType != null) {
            holder.setQueryType(queryType);
        }
        return holder;
    }
}
