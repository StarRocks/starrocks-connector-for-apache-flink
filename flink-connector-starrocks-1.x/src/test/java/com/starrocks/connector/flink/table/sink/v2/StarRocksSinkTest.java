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

package com.starrocks.connector.flink.table.sink.v2;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.configuration.Configuration;

import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Verifications;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

public class StarRocksSinkTest {

    @Test
    public void testRestoreWriterPassesRecoveredState(
            @Mocked StarRocksWriterAdapter<Object> anyWriter,
            @Injectable Sink.InitContext context) throws Exception {
        StarRocksSink<Object> sink = new StarRocksSink<>(sinkOptions(), null, null);
        Collection<StarRocksWriterState> recoveredState =
                Collections.singletonList(new StarRocksWriterState(Collections.emptyList()));

        sink.restoreWriter(context, recoveredState);

        new Verifications() {{
            new StarRocksWriterAdapter<>(
                    (StarRocksSinkOptions) any,
                    (Sink.InitContext) any,
                    (RecordSerializationSchema<Object>) any,
                    (StreamLoadProperties) any,
                    recoveredState);
            times = 1;
        }};
    }

    private static StarRocksSinkOptions sinkOptions() {
        Configuration conf = new Configuration();
        conf.set(StarRocksSinkOptions.TABLE_NAME, "test_table");
        conf.set(StarRocksSinkOptions.DATABASE_NAME, "test_db");
        conf.setString(StarRocksSinkOptions.LOAD_URL.key(), "127.0.0.1:8030");
        conf.set(StarRocksSinkOptions.JDBC_URL, "jdbc:mysql://127.0.0.1:9030");
        conf.set(StarRocksSinkOptions.USERNAME, "root");
        conf.set(StarRocksSinkOptions.PASSWORD, "");
        return new StarRocksSinkOptions(conf, conf.toMap());
    }
}
