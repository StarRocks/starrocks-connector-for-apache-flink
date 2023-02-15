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

package com.starrocks.connector.flink.manager;

import com.alibaba.fastjson.JSON;
import com.starrocks.data.load.stream.StreamLoadStrategy;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.stream.StreamSupport;

public class FlinkStreamLoadStrategy implements StreamLoadStrategy {

    private static final Logger log = LoggerFactory.getLogger(FlinkStreamLoadStrategy.class);

    private final long oldAge;
    private final long youngAge;
    private final long cacheMaxBytes;
    private final long writingThreshold;
    private final float regionBufferRatio;
    private final float cacheLimitBytes;

    public FlinkStreamLoadStrategy(StreamLoadProperties properties) {
        long expectDelayTime = properties.getExpectDelayTime();
        this.youngAge = (long) ((expectDelayTime / properties.getScanningFrequency()) * properties.getYoungThreshold());
        this.oldAge = (long) ((expectDelayTime / properties.getScanningFrequency()) * properties.getOldThreshold());
        this.cacheMaxBytes = properties.getMaxCacheBytes();
        this.cacheLimitBytes = cacheMaxBytes * 0.8F;
        this.writingThreshold = properties.getWritingThreshold();
        this.regionBufferRatio = properties.getRegionBufferRatio();
        log.info("Load Strategy properties : {}", JSON.toJSONString(this));
    }


    @Override
    public List<TableRegion> select(Iterable<TableRegion> regions) {
        List<TableRegion> waitFlushRegions = new ArrayList<>();
        List<TableRegion> youngRegions = new ArrayList<>();

        int totalReadableRegion = 0;
        for (TableRegion region : regions) {
            long age = region.getAge();
            totalReadableRegion++;
            if (age >= oldAge) {
                waitFlushRegions.add(region);
            } else if (age >= youngAge) {
                youngRegions.add(region);
            }
        }
        if (totalReadableRegion == 0) {
            return waitFlushRegions;
        }

        long cacheThreshold = (long) ((cacheMaxBytes / totalReadableRegion) * regionBufferRatio);
        long currentTimeMillis = System.currentTimeMillis();
        for (TableRegion region : youngRegions) {
            if (region.getCacheBytes() < cacheThreshold) {
                continue;
            }
            if (region.getCacheBytes() < cacheLimitBytes
                    && currentTimeMillis - region.getLastWriteTimeMillis() < writingThreshold) {
                continue;
            }
            waitFlushRegions.add(region);
        }

        if (waitFlushRegions.isEmpty()) {
            TableRegion maxFlushRegion = StreamSupport.stream(regions.spliterator(), false)
                    .max((r1, r2) -> {
                        if (r1.getFlushBytes() != r2.getFlushBytes()) {
                            return Long.compare(r2.getFlushBytes(), r1.getFlushBytes());
                        }
                        return Long.compare(r2.getCacheBytes(), r1.getCacheBytes());
                    })
                    .orElse(null);
            if (maxFlushRegion != null) {
                waitFlushRegions.add(maxFlushRegion);
            }
        }

        waitFlushRegions.sort(Comparator.comparingLong(TableRegion::getCacheBytes).reversed());
        return waitFlushRegions;
    }

    public boolean shouldCommit(TableRegion region) {
        return region.getAge() > oldAge;
    }
}
