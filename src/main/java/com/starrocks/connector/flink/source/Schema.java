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

package com.starrocks.connector.flink.source;

import com.starrocks.connector.flink.thrift.TScanColumnDesc;

import java.util.ArrayList;
import java.util.List;

public class Schema {
    private int status = 0;
    private List<Field> properties;

    public Schema() {
        properties = new ArrayList<>();
    }

    public Schema(int fieldCount) {
        properties = new ArrayList<>(fieldCount);
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public List<Field> getProperties() {
        return properties;
    }

    public void setProperties(List<Field> properties) {
        this.properties = properties;
    }

    public void put(String name, String type, String comment, int scale, int precision) {
        properties.add(new Field(name, type, comment, scale, precision));
    }

    public void put(Field f) {
        properties.add(f);
    }

    public Field get(int index) {
        if (index >= properties.size()) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Fields size：" + properties.size());
        }
        return properties.get(index);
    }

    public int size() {
        return properties.size();
    }

    public static Schema genSchema(List<TScanColumnDesc> tscanColumnDescs) {
        Schema schema = new Schema(tscanColumnDescs.size());
        tscanColumnDescs.stream().forEach(desc -> schema.put(new Field(desc.getName(), desc.getType().name(), "", 0, 0)));
        return schema;
    }
}
