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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * Manage user's cluster operations such as restart, add FE/BE.
 */
public class OperationManager implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(OperationManager.class);

    private final BlockingQueue<Operation> operationQueue;
    private final ExecutorService executor;
    private volatile boolean closed;

    public OperationManager() {
        this.operationQueue = new LinkedBlockingQueue<>();
        this.executor =
                Executors.newSingleThreadExecutor(
                        r -> new Thread(r, "Operation Executor"));
    }

    public void start() {
        executor.execute(() -> {
            while (!closed) {
                try {
                    Operation operation = operationQueue.take();
                    LOG.info("Staring to run operation {} ......", operation.getName());
                    try {
                        operation.run();
                        LOG.info("Waiting for the operation result {} ......", operation.getName());
                        boolean success = operation.verifyOperationResult();
                        LOG.info("Operation {} is {}", operation.getName(), success ? "successful" : "failed");
                        operation.getFuture().complete(success);
                    } catch (Exception e) {
                        LOG.error("Failed to complete operation {}", operation.getName(), e);
                        operation.getFuture().completeExceptionally(e);
                    }
                } catch (Exception e) {
                    LOG.error("Exception occurs in operation executor", e);
                }
            }
        });
    }

    public void addOperation(Operation operation) {
        operationQueue.add(operation);
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
        executor.shutdownNow();
    }

    public enum OperationType {
        RESTART_CLUSTER,
        RESTART_ALL_FE,
        RESTART_ALL_BE
    }

    public interface RunnableWithException {
        void run() throws Exception;
    }

    public static abstract class Operation implements RunnableWithException {

        private final OperationType operationType;
        /**
         * The future to indicate whether this operation is
         * executed successfully.
         */
        private final CompletableFuture<Boolean> future;

        public Operation(OperationType operationType) {
            this.operationType = operationType;
            this.future = new CompletableFuture<>();
        }

        public CompletableFuture<Boolean> getFuture() {
            return future;
        }

        public OperationType getOperationType() {
            return operationType;
        }

        public String getName() {
            return operationType.name();
        }

        public abstract boolean verifyOperationResult();
    }

    public static class ComposedOperation extends Operation {

        private final List<Operation> operations;

        public ComposedOperation(OperationType type, List<Operation> operations) {
            super(type);
            this.operations = operations;
        }

        @Override
        public void run() throws Exception {
            for (Operation operation : operations) {
                LOG.info("Starting to run sub-operation {} ......", operation.getName());
                operation.run();
            }
        }

        @Override
        public boolean verifyOperationResult() {
            for (Operation operation : operations) {
                LOG.info("Waiting for the sub-operation result {} ......", operation.getName());
                boolean success = operation.verifyOperationResult();
                LOG.info("Sub-pperation {} is {}", operation.getName(), success ? "successful" : "failed");
                if (!success) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String getName() {
            return String.format("%s{%s}", getOperationType().name(),
                    operations.stream().map(Operation::getName)
                            .collect(Collectors.joining(",")));
        }
    }
}
