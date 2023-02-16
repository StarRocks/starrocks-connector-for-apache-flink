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

import com.starrocks.data.load.stream.StreamLoadStrategy;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class FlushAndCommitStrategy implements StreamLoadStrategy {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FlushAndCommitStrategy.class);

    private final long expectDelayTime;
    private final long scanFrequency;
    private final long ageThreshold;

    private final AtomicLong numAgeTriggerFlush = new AtomicLong(0);
    private final AtomicLong numForceTriggerFlush = new AtomicLong(0);

    public FlushAndCommitStrategy(StreamLoadProperties properties) {
        this.expectDelayTime = properties.getExpectDelayTime();
        this.scanFrequency = properties.getScanningFrequency();
        this.ageThreshold = expectDelayTime / scanFrequency;
    }

    @Override
    public List<TableRegion> select(Iterable<TableRegion> regions) {
       throw new UnsupportedOperationException();
    }

    public List<TableRegion> selectFlushRegions(Queue<TableRegion> regions, boolean forceFlush) {
        List<TableRegion> flushRegions = new ArrayList<>();
        for (TableRegion region : regions) {
            long age = region.getAge();
            if (age >= ageThreshold) {
                numAgeTriggerFlush.getAndIncrement();
                flushRegions.add(region);
                LOG.debug("Choose region {} to flush because the age reach the threshold, age: {}, " +
                            "threshold: {}, scanFreq: {}, expectDelayTime: {}", region.getUniqueKey(),
                                age, ageThreshold, scanFrequency, expectDelayTime);
            }
        }

        // simply choose the region with maximum bytes
        if (flushRegions.isEmpty() && forceFlush) {
            regions.stream().max(Comparator.comparingLong(TableRegion::getCacheBytes)).ifPresent(flushRegions::add);
            if (!flushRegions.isEmpty()) {
                numForceTriggerFlush.getAndIncrement();
                TableRegion region = flushRegions.get(0);
                LOG.debug("Choose region {} to flush because it's force flush, age: {}, " +
                            "threshold: {}, scanFreq: {}, expectDelayTime: {}", region.getUniqueKey(),
                                region.getAge(), ageThreshold, scanFrequency, expectDelayTime);
            }
        }

        return flushRegions;
    }
    
    public boolean shouldCommit(TableRegion region) {
        return region.getAge() > ageThreshold;
    }

    @Override
    public String toString() {
        return "FlushAndCommitStrategy{" +
                "expectDelayTime=" + expectDelayTime +
                ", scanFrequency=" + scanFrequency +
                ", ageThreshold=" + ageThreshold +
                ", numAgeTriggerFlush=" + numAgeTriggerFlush +
                ", numForceTriggerFlush=" + numForceTriggerFlush +
                '}';
    }
}
