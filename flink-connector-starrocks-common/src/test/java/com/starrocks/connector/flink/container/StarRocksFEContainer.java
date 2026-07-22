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

/** A container for FE. */
public class StarRocksFEContainer extends StarRocksContainer<StarRocksFEContainer> {

    static final Logger LOG = LoggerFactory.getLogger(StarRocksFEContainer.class);

    public static final int HTTP_PORT = 8030;
    public static final int RPC_PORT = 9020;
    public static final int QUERY_PORT = 9030;
    public static final int EDIT_LOG_PORT = 9010;

    private final boolean isObserver;

    public StarRocksFEContainer(DockerImageName imageName, String id, boolean isObserver) {
        super(imageName, id);
        this.isObserver = isObserver;
    }

    public boolean isObserver() {
        return isObserver;
    }

    @Override
    public void waitUntilReachable(Duration timeout) {
        try {
            LOG.info("Waiting FE {} to be reachable, timeout: {}", getId(), timeout);
            new HttpWaitStrategy()
                    .forPort(StarRocksFEContainer.HTTP_PORT)
                    .forPath("/")
                    .withBasicCredentials("root", "")
                    .forStatusCode(200)
                    .withReadTimeout(timeout)
                    .waitUntilReady(this);
        } catch (Exception e) {
            String errMsg = String.format("FE %s with container id %s can't be reachable", getId(), getContainerId());
            LOG.error("{}", errMsg, e);
            throw new StarRocksContainerException(errMsg, e);
        }
    }

    public void executeMysqlCmd(String cmd) {
        StarRocksContainerException exception = null;
        try {
            String execCmd = String.format("mysql -h127.0.0.1 -P%s -uroot -e \"%s\"", QUERY_PORT, cmd);
            ExecResult result = execInContainer("/bin/bash", "-c", execCmd);
            if (result.getExitCode() == 0) {
                LOG.info("Successful to execute mysql command on FE {}, cmd=[{}], {}", getId(), cmd, result);
            } else {
                String errMsg = String.format("Failed to execute mysql command on FE %s, cmd=[%s], %s", getId(), cmd, result);
                LOG.error(errMsg);
                exception = new StarRocksContainerException(errMsg);
            }
        } catch (Exception e) {
            String errMsg = String.format("Failed to execute mysql command on FE %s, [%s]", getId(), cmd);
            LOG.error("{}", errMsg, e);
            exception = new StarRocksContainerException(errMsg, e);
        }

        if (exception != null) {
            throw exception;
        }
    }

    public void addFEFollower(StarRocksFEContainer follower) {
        LOG.info("Add FE follower {} to the helper {}", follower.getId(), getId());
        String cmd = String.format("ALTER SYSTEM ADD follower '%s:%s'",
                follower.getId(), StarRocksFEContainer.EDIT_LOG_PORT);
        executeMysqlCmd(cmd);
    }

    public void addFEObserver(StarRocksFEContainer observer) {
        LOG.info("Add FE observer {} to the helper {}", observer.getId(), getId());
        String cmd = String.format("ALTER SYSTEM ADD observer '%s:%s'",
                observer.getId(), StarRocksFEContainer.EDIT_LOG_PORT);
        executeMysqlCmd(cmd);
    }

    public void addBE(StarRocksBEContainer be) {
        LOG.info("Add BE {} to the helper {}", be.getId(), getId());
        String cmd = String.format("ALTER SYSTEM ADD backend '%s:%s'",
                be.getId(), StarRocksBEContainer.HEARBEAT_SERVICE_PORT);
        executeMysqlCmd(cmd);
    }
}
