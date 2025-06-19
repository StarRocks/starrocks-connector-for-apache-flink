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

package com.starrocks.data.load.stream.mergecommit.fe;

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicLong;

public class RequestStat {

    private final long windowSizeMs;
    private final Logger logger;

    private final long startTimeMs;
    private final AtomicLong totalCount = new AtomicLong(0);
    private final AtomicLong totalLatencyNs = new AtomicLong(0);
    private final AtomicLong maxLatencyNs = new AtomicLong(0);
    private final AtomicLong minLatencyNs = new AtomicLong(Long.MAX_VALUE);

    // last window
    private final AtomicLong lastWindowStartMs = new AtomicLong(0);
    private final AtomicLong windowCount = new AtomicLong(0);
    private final AtomicLong windowLatencyNs = new AtomicLong(0);
    private final AtomicLong windowMaxLatencyNs = new AtomicLong(0);
    private final AtomicLong windowMinLatencyNs = new AtomicLong(Long.MAX_VALUE);

    public RequestStat(long windowSizeMs, Logger logger) {
        this.windowSizeMs = windowSizeMs;
        this.logger = logger;
        this.startTimeMs = System.currentTimeMillis();
    }

    public void addRequest(long latencyNs) {
        totalCount.incrementAndGet();
        totalLatencyNs.addAndGet(latencyNs);
        updateMax(maxLatencyNs, latencyNs);
        updateMin(minLatencyNs, latencyNs);

        long lastWindow = lastWindowStartMs.get();
        long now = System.currentTimeMillis();
        if (now - lastWindow >= windowSizeMs) {
            if (lastWindowStartMs.compareAndSet(lastWindow, now / windowSizeMs * windowSizeMs)) {
                printStat(lastWindow);
                windowCount.set(0);
                windowLatencyNs.set(0);
                windowMaxLatencyNs.set(0);
                windowMinLatencyNs.set(Long.MAX_VALUE);
            }
        }
        windowCount.incrementAndGet();
        windowLatencyNs.addAndGet(latencyNs);
        updateMax(windowMaxLatencyNs, latencyNs);
        updateMin(windowMinLatencyNs, latencyNs);
    }

    private void updateMax(AtomicLong currentValue, long newValue) {
        while (true) {
            long current = currentValue.get();
            if (newValue <= current) {
                break;
            }
            if (currentValue.compareAndSet(current, newValue)) {
                break;
            }
        }
    }

    private void updateMin(AtomicLong currentValue, long newValue) {
        while (true) {
            long current = currentValue.get();
            if (newValue >= current) {
                break;
            }
            if (currentValue.compareAndSet(current, newValue)) {
                break;
            }
        }
    }

    private void printStat(long lastPrintMs) {
        if (logger == null) {
            return;
        }

        long current = System.currentTimeMillis();
        logger.info("Request stat, total count: {}, duration: {} ms, avg: {} us, max: {} us, min: {} us, " +
                        "window count: {}, window duration: {} ms, window avg: {} us, window max: {} us, window min: {} us",
                totalCount, current - startTimeMs, totalCount.get() > 0 ? totalLatencyNs.get() / totalCount.get() / 1000 : 0,
                maxLatencyNs.get() / 1000, minLatencyNs.get() == Long.MAX_VALUE ? 0 : minLatencyNs.get() / 1000,
                windowCount.get(), current - lastPrintMs,
                windowCount.get() > 0 ? windowLatencyNs.get() / windowCount.get() / 1000 : 0,
                windowMaxLatencyNs.get() / 1000, windowMinLatencyNs.get() == Long.MAX_VALUE ? 0 : windowMinLatencyNs.get() / 1000);
    }
}
