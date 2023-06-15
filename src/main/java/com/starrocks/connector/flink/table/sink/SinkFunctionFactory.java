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

package com.starrocks.connector.flink.table.sink;

import com.starrocks.connector.flink.row.sink.StarRocksIRowTransformer;
import org.apache.flink.table.api.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.starrocks.data.load.stream.StreamLoadUtils.isStarRocksSupportTransactionLoad;

/** Create sink function according to the configuration. */
public class SinkFunctionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SinkFunctionFactory.class);

    enum SinkVersion {
        // Implement exactly-once using stream load which has a
        // poor performance. All versions of StarRocks are supported
        V1,
        // Implement exactly-once using transaction load since StarRocks 2.4
        V2,
        // Select sink version automatically according to whether StarRocks
        // supports transaction load
        AUTO
    }

    public static void detectStarRocksFeature(StarRocksSinkOptions sinkOptions) {
        try {
            boolean supportTransactionLoad = isStarRocksSupportTransactionLoad(
                    sinkOptions.getLoadUrlList(), sinkOptions.getConnectTimeout(), sinkOptions.getUsername(), sinkOptions.getPassword());
            sinkOptions.setSupportTransactionStreamLoad(supportTransactionLoad);
            if (supportTransactionLoad) {
                LOG.info("StarRocks supports transaction load");
            } else {
                LOG.info("StarRocks does not support transaction load");
            }
        } catch (Exception e) {
            LOG.warn("Can't decide whether StarRocks supports transaction load, and enable it by default.");
            sinkOptions.setSupportTransactionStreamLoad(true);
        }
    }

    public static SinkVersion chooseSinkVersionAutomatically(StarRocksSinkOptions sinkOptions) {
        if (StarRocksSinkSemantic.AT_LEAST_ONCE.equals(sinkOptions.getSemantic())) {
            LOG.info("Choose sink version V2 for at-least-once.");
            return SinkVersion.V2;
        }

        if (sinkOptions.isSupportTransactionStreamLoad()) {
            LOG.info("StarRocks supports transaction load, and choose sink version V2");
            return SinkVersion.V2;
        } else {
            LOG.info("StarRocks does not support transaction load, and choose sink version V1");
            return SinkVersion.V1;
        }
    }

    public static SinkVersion getSinkVersion(StarRocksSinkOptions sinkOptions) {
        String sinkTypeOption = sinkOptions.getSinkVersion().trim().toUpperCase();
        SinkVersion sinkVersion;
        if (SinkVersion.V1.name().equals(sinkTypeOption)) {
            sinkVersion = SinkVersion.V1;
        } else if (SinkVersion.V2.name().equals(sinkTypeOption)) {
            sinkVersion = SinkVersion.V2;
        } else if (SinkVersion.AUTO.name().equals(sinkTypeOption)) {
            sinkVersion = chooseSinkVersionAutomatically(sinkOptions);
        } else {
            throw new UnsupportedOperationException("Unsupported sink type " + sinkTypeOption);
        }
        LOG.info("Choose sink version {}", sinkVersion.name());
        return sinkVersion;
    }

    public static <T> StarRocksDynamicSinkFunctionBase<T> createSinkFunction(
            StarRocksSinkOptions sinkOptions, TableSchema schema, StarRocksIRowTransformer<T> rowTransformer) {
        detectStarRocksFeature(sinkOptions);
        SinkVersion sinkVersion = getSinkVersion(sinkOptions);
        switch (sinkVersion) {
            case V1:
                return new StarRocksDynamicSinkFunction<>(sinkOptions, schema, rowTransformer);
            case V2:
                return new StarRocksDynamicSinkFunctionV2<>(sinkOptions, schema, rowTransformer);
            default:
                throw new UnsupportedOperationException("Unsupported sink type " + sinkVersion.name());
        }
    }

    public static <T> StarRocksDynamicSinkFunctionBase<T> createSinkFunction(StarRocksSinkOptions sinkOptions) {
        detectStarRocksFeature(sinkOptions);
        SinkVersion sinkVersion = getSinkVersion(sinkOptions);
        switch (sinkVersion) {
            case V1:
                return new StarRocksDynamicSinkFunction<>(sinkOptions);
            case V2:
                return new StarRocksDynamicSinkFunctionV2<>(sinkOptions);
            default:
                throw new UnsupportedOperationException("Unsupported sink type " + sinkVersion.name());
        }
    }
}
