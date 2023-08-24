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

package com.starrocks.connector.flink.table.sink;

import com.google.common.base.Objects;
import com.starrocks.data.load.stream.LabelGenerator;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Label generator for a table under exactly-once mode. The generator should guarantee the
 * uniqueness of the label across different flink jobs, different tables in one job, different
 * subtasks for a table in one job, and different transactions in on subtask. The label is
 * in the format {labelPrefix}-{table}-{subtaskIndex}-{id}, and it will be unique because
 * 1. labelPrefix is unique across different flink jobs, so there is no conflict among flink
 *    jobs. Note that the uniqueness of labelPrefix should be guaranteed by users.
 * 2. the table name is unique in a database of StarRocks, and the label only need to be unique in
 *    the scope of a database. so the label can be unique even if write to multiple tables in a job
 * 3. use subtaskIndex to make it unique across subtasks if the sink writes parallel
 * 4. use incremental id to make it unique across different transactions in a subtask
 *
 * <p>
 * The reason why we design this generator for exactly-once is to clean up lingering transactions.
 * Compared to at-least-once, exactly-once will not abort the PREPARED transactions when the job
 * failovers or exits because it's 2PC mechanism. Some of those PREPARED transactions may be in a
 * successful checkpoint, and will be committed when the job restores from the checkpoint, but some
 * of them are just useless, and should be aborted, otherwise they will be lingering in StarRocks
 * until timeout which maybe make StarRocks unstable, so we should try to abort these lingering
 * transactions when the job restores. The key is to find those lingering transactions, and we achieve
 * it by generating labels according to certain rules, storing the label generator information in
 * checkpoint, and constructing labels of possible lingering transactions when restoring from the
 * checkpoints. First, each label has an incremental id, and when checkpointing, the labels with ids
 * less than nextId must be successful, and ids for lingering transactions must be no less than nextId.
 * Secondly, make a snapshot {@link ExactlyOnceLabelGeneratorSnapshot} for the generator when checkpointing,
 * and store it in the state. Thirdly, when restoring from the checkpoint, we can get the nextId, build
 * labels with those ids no less than nextId, and , query label state in StarRocks, and abort them
 * if they are PREPARED.
 */
public class ExactlyOnceLabelGenerator implements LabelGenerator {

    private final String labelPrefix;
    private final String db;
    private final String table;
    private final int numberOfSubtasks;
    private final int subTaskIndex;
    /**
     * The id only increment when checkpoint triggers, so it will not
     * be larger than the next checkpoint id. But it may be smaller
     * than the next checkpoint id because some checkpoints will not
     * trigger on this subtask, and the id will not increment.
     */
    private final AtomicLong nextId;

    public ExactlyOnceLabelGenerator(String labelPrefix, String db, String table,
                                     int numberOfSubtasks, int subTaskIndex, long initId) {
        this.labelPrefix = labelPrefix;
        this.db = db;
        this.table = table;
        this.numberOfSubtasks = numberOfSubtasks;
        this.subTaskIndex = subTaskIndex;
        this.nextId = new AtomicLong(initId);
    }

    @Override
    public String next() {
        return genLabel(labelPrefix, table, subTaskIndex, nextId.getAndIncrement());
    }

    public ExactlyOnceLabelGeneratorSnapshot snapshot(long checkpointId) {
        return new ExactlyOnceLabelGeneratorSnapshot(
                checkpointId, db, table, labelPrefix, numberOfSubtasks, subTaskIndex, nextId.get());
    }

    public static String genLabel(String labelPrefix, String table, int subTaskIndex, long id) {
        return String.format("%s-%s-%s-%s", labelPrefix, table, subTaskIndex, id);
    }

    @Override
    public String toString() {
        return "ExactlyOnceLabelGenerator{" +
                "labelPrefix='" + labelPrefix + '\'' +
                ", db='" + db + '\'' +
                ", table='" + table + '\'' +
                ", numberOfSubtasks=" + numberOfSubtasks +
                ", subTaskIndex=" + subTaskIndex +
                ", nextId=" + nextId +
                '}';
    }

    public static class LabelDbTableSubtask {

        private final String labelPrefix;
        private final String db;
        private final String table;
        private final int subTaskIndex;

        public LabelDbTableSubtask(String labelPrefix, String db, String table, int subTaskIndex) {
            this.labelPrefix = labelPrefix;
            this.db = db;
            this.table = table;
            this.subTaskIndex = subTaskIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LabelDbTableSubtask that = (LabelDbTableSubtask) o;
            return subTaskIndex == that.subTaskIndex &&
                    Objects.equal(labelPrefix, that.labelPrefix) &&
                    Objects.equal(db, that.db) &&
                    Objects.equal(table, that.table);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(labelPrefix, db, table, subTaskIndex);
        }
    }
}
