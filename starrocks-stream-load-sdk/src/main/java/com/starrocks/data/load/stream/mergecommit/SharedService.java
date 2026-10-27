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

package com.starrocks.data.load.stream.mergecommit;

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class SharedService {

    private final ReentrantReadWriteLock lock;
    private final AtomicInteger refCount;

    protected SharedService() {
        this.lock = new ReentrantReadWriteLock();
        this.refCount = new AtomicInteger(0);
    }

    protected abstract Logger getLogger();

    protected abstract void init() throws Exception;

    protected abstract void reset();

    public void takeRef() throws Exception {
        int oldCount;
        boolean success = false;
        lock.writeLock().lock();
        try {
            oldCount = refCount.getAndIncrement();
            if (oldCount == 0) {
                init();
            }
            success = true;
        } finally {
            if (!success) {
                refCount.decrementAndGet();
            }
            lock.writeLock().unlock();
        }
        if (getLogger() != null) {
            getLogger().info("Take reference, count: {}", oldCount + 1);
        }
    }

    public void releaseRef() {
        int newCount;
        lock.writeLock().lock();
        try {
            newCount = refCount.decrementAndGet();
            if (newCount == 0) {
                reset();
            }
        } finally {
            lock.writeLock().unlock();
        }
        if (getLogger() != null) {
            getLogger().info("Release reference, count: {}", newCount);
        }
    }
}
