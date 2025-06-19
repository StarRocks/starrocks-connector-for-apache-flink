/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.data.load.stream.mergecommit.be;

import com.baidu.bjf.remoting.protobuf.FieldType;
import com.baidu.bjf.remoting.protobuf.annotation.Protobuf;
import com.baidu.bjf.remoting.protobuf.annotation.ProtobufClass;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@ProtobufClass
@Setter
@Getter
public class PStreamLoadRequest {

    private String db;
    private String table;
    private String user;
    private String passwd;
    @Protobuf(fieldType= FieldType.OBJECT)
    private List<PStringPair> parameters;

    @Override
    public String toString() {
        return "PStreamLoadRequest{" +
                "db='" + db + '\'' +
                ", table='" + table + '\'' +
                ", user='" + user + '\'' +
                ", passwd='" + passwd + '\'' +
                ", parameters=" + parameters +
                '}';
    }
}
