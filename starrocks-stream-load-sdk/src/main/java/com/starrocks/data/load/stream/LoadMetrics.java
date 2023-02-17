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

package com.starrocks.data.load.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class LoadMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(LoadMetrics.class);

    private final long startTimeNano;
    private final AtomicLong numberOfSuccessLoad = new AtomicLong();
    private final AtomicLong totalSuccessLoadBytes = new AtomicLong();
    private final AtomicLong totalSuccessLoadRows = new AtomicLong();
    private final AtomicLong totalSuccessLoadTimeNano = new AtomicLong();
    private final AtomicLong numberOfFailedLoad = new AtomicLong();

    private final AtomicLong numberWriteTriggerFlush = new AtomicLong();
    private final AtomicLong numberWriteBlock = new AtomicLong();
    private final AtomicLong totalWriteBlockTimeNano = new AtomicLong();

    public LoadMetrics() {
        this.startTimeNano = System.nanoTime();
    }

    public void updateSuccessLoad(StreamLoadResponse response) {
        numberOfSuccessLoad.incrementAndGet();
        if (response.getFlushBytes() != null) {
            totalSuccessLoadBytes.addAndGet(response.getFlushBytes());
        }
        if (response.getFlushRows() != null) {
            totalSuccessLoadRows.addAndGet(response.getFlushRows());
        }
        if (response.getCostNanoTime() != null) {
            totalSuccessLoadTimeNano.addAndGet(response.getCostNanoTime());
        }
    }

    public void updateFailedLoad() {
        numberOfFailedLoad.incrementAndGet();
    }

    public void updateWriteTriggerFlush(int number) {
        numberWriteTriggerFlush.addAndGet(number);
    }

    public void updateWriteBlock(int number, long timeNano) {
        numberWriteBlock.addAndGet(number);
        totalWriteBlockTimeNano.addAndGet(timeNano);
    }

    @Override
    public String toString() {
        return "LoadMetrics{" +
                "startTimeNano=" + startTimeNano +
                ", totalRunningTimeNano=" + (System.nanoTime() - startTimeNano) +
                ", numberOfSuccessLoad=" + numberOfSuccessLoad +
                ", totalSuccessLoadBytes=" + totalSuccessLoadBytes +
                ", totalSuccessLoadRows=" + totalSuccessLoadRows +
                ", totalSuccessLoadTimeNano=" + totalSuccessLoadTimeNano +
                ", numberOfFailedLoad=" + numberOfFailedLoad +
                ", numberWriteTriggerFlush=" + numberWriteTriggerFlush +
                ", numberWriteBlock=" + numberWriteBlock +
                ", totalWriteBlockTimeNano=" + totalWriteBlockTimeNano +
                '}';
    }
}
