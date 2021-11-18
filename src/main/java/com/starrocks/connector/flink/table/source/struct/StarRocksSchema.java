// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.flink.table.source.struct;

import com.starrocks.connector.flink.thrift.TScanColumnDesc;

import java.util.ArrayList;
import java.util.List;

public class StarRocksSchema {
    private int status = 0;
    private List<Column> properties;

    public StarRocksSchema() {
        properties = new ArrayList<>();
    }

    public StarRocksSchema(int fieldCount) {
        properties = new ArrayList<>(fieldCount);
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public List<Column> getProperties() {
        return properties;
    }

    public void setProperties(List<Column> properties) {
        this.properties = properties;
    }

    public void put(String name, String type, String comment, int scale, int precision) {
        properties.add(new Column(name, type, comment, scale, precision));
    }

    public void put(Column f) {
        properties.add(f);
    }

    public Column get(int index) {
        if (index >= properties.size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Fields size：" + properties.size());
        }
        return properties.get(index);
    }

    public int size() {
        return properties.size();
    }

    public static StarRocksSchema genSchema(List<TScanColumnDesc> tscanColumnDescs) {
        StarRocksSchema schema = new StarRocksSchema(tscanColumnDescs.size());
        tscanColumnDescs.stream().forEach(desc -> schema.put(new Column(desc.getName(), desc.getType().name(), "", 0, 0)));
        return schema;
    }
}
