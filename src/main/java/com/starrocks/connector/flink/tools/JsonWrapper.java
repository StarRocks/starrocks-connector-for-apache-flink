/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
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

package com.starrocks.connector.flink.tools;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.JSONSerializer;
import com.alibaba.fastjson.serializer.ObjectSerializer;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.alibaba.fastjson.serializer.SerializeWriter;
import com.alibaba.fastjson.util.IOUtils;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Type;

import static com.alibaba.fastjson.JSON.DEFAULT_PARSER_FEATURE;

public class JsonWrapper {

    // These two configs are to replace the global/static instance of SerializeConfig#globalInstance
    // and ParserConfig#global in fastjson so that the lifetimes of the configs are same as the instance
    // of this sink, and will not be used by other instances. Otherwise, Flink may fail on checking
    // classloader leak if the job failover repeatedly. See the flink document for details
    // https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/#classloader-check-leaked-classloader
    private SerializeConfig serializeConfig;
    private ParserConfig parserConfig;

    public JsonWrapper() {
        this.serializeConfig = new SerializeConfig();
        this.parserConfig = new ParserConfig();

        serializeConfig.put(BinaryStringData.class, new BinaryStringDataSerializer());
        serializeConfig.put(DecimalData.class, new DecimalDataSerializer());
        serializeConfig.put(TimestampData.class, new TimestampDataSerializer());
    }

    public byte[] toJSONBytes(Object object) {
        return JSON.toJSONBytes(object, serializeConfig);
    }

    public String toJSONString(Object object) {
        return JSON.toJSONString(object, serializeConfig);
    }

    public Object parse(String text) {
        return JSON.parse(text, parserConfig);
    }

    public <T> T parseObject(byte[] bytes, Type clazz) {
        return JSON.parseObject(bytes, 0, bytes.length, IOUtils.UTF8, clazz, parserConfig, null, DEFAULT_PARSER_FEATURE);
    }
}

final class BinaryStringDataSerializer implements ObjectSerializer, Serializable {
    private static final long serialVersionUID = 1L;
    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws
            IOException {
        SerializeWriter out = serializer.getWriter();
        if (null == object) {
            serializer.getWriter().writeNull();
            return;
        }
        BinaryStringData strData = (BinaryStringData)object;
        out.writeString(strData.toString());
    }
}

final class DecimalDataSerializer implements ObjectSerializer, Serializable {
    private static final long serialVersionUID = 1L;
    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        SerializeWriter out = serializer.getWriter();
        if (null == object) {
            serializer.getWriter().writeNull();
            return;
        }
        out.writeString(((DecimalData)object).toBigDecimal().toPlainString());
    }
}

final class TimestampDataSerializer implements ObjectSerializer, Serializable {
    private static final long serialVersionUID = 1L;
    @Override
    public void write(JSONSerializer serializer, Object object, Object fieldName, Type fieldType, int features) throws IOException {
        SerializeWriter out = serializer.getWriter();
        if (null == object) {
            serializer.getWriter().writeNull();
            return;
        }
        out.writeString(((TimestampData)object).toLocalDateTime().toString());
    }
}

