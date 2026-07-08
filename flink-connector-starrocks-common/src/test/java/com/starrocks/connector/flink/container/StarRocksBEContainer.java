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

package com.starrocks.connector.flink.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

/** A container for BE. */
public class StarRocksBEContainer extends StarRocksContainer<StarRocksBEContainer> {

    static final Logger LOG = LoggerFactory.getLogger(StarRocksBEContainer.class);

    public static final int BE_PORT = 9060;
    public static final int WEB_SERVER_PORT = 8040;
    public static final int HEARBEAT_SERVICE_PORT = 9050;
    public static final int BRPC_PORT = 8060;

    public StarRocksBEContainer(DockerImageName dockerImageName, String id) {
        super(dockerImageName, id);
    }

    @Override
    public void waitUntilReachable(Duration timeout) {
        try {
            LOG.info("Waiting BE {} to be reachable, timeout: {}", getId(), timeout);
            new HttpWaitStrategy()
                    .forPort(WEB_SERVER_PORT)
                    .forPath("/")
                    .forStatusCode(200)
                    .withReadTimeout(timeout)
                    .withStartupTimeout(timeout)
                    .waitUntilReady(this);
        } catch (Exception e) {
            String errMsg = String.format("BE %s with container id %s can't be reachable", getId(), getContainerId());
            LOG.error("{}", errMsg, e);
            throw new StarRocksContainerException(errMsg, e);
        }
    }
}
