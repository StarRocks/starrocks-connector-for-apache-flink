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

/** Snapshot for the label generator. */
public class ExactlyOnceLabelGeneratorSnapshot {

    /** Which checkpoint creates this snapshot. */
    private final long checkpointId;
    /** The database which the generator belongs to. */
    private final String db;
    /** The table which the generator belongs to. */
    private final String table;
    /** The label prefix. */
    private final String labelPrefix;
    /** The number of subtasks (the parallelism of the sink) when this snapshot is created. */
    private final int numberOfSubtasks;
    /** The index of the subtask that creates this snapshot. */
    private final int subTaskIndex;
    /** Next id used to create label. */
    private final long nextId;

    public ExactlyOnceLabelGeneratorSnapshot(long checkpointId, String db, String table,
                                             String labelPrefix, int numberOfSubtasks, int subTaskIndex, long nextId) {
        this.checkpointId = checkpointId;
        this.db = db;
        this.table = table;
        this.labelPrefix = labelPrefix;
        this.numberOfSubtasks = numberOfSubtasks;
        this.subTaskIndex = subTaskIndex;
        this.nextId = nextId;
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public String getLabelPrefix() {
        return labelPrefix;
    }

    public int getNumberOfSubtasks() {
        return numberOfSubtasks;
    }

    public int getSubTaskIndex() {
        return subTaskIndex;
    }

    public long getNextId() {
        return nextId;
    }

    public ExactlyOnceLabelGenerator.LabelDbTableSubtask createLabelDbTableSubtask() {
        return new ExactlyOnceLabelGenerator.LabelDbTableSubtask(labelPrefix, db, table, subTaskIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExactlyOnceLabelGeneratorSnapshot snapshot = (ExactlyOnceLabelGeneratorSnapshot) o;
        return checkpointId == snapshot.checkpointId
                && numberOfSubtasks == snapshot.numberOfSubtasks
                && subTaskIndex == snapshot.subTaskIndex
                && nextId == snapshot.nextId
                && Objects.equal(db, snapshot.db)
                && Objects.equal(table, snapshot.table)
                && Objects.equal(labelPrefix, snapshot.labelPrefix);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                checkpointId,
                db,
                table,
                labelPrefix,
                numberOfSubtasks,
                subTaskIndex,
                nextId);
    }

    @Override
    public String toString() {
        return "ExactlyOnceLabelGeneratorSnapshot{" +
                "checkpointId=" + checkpointId +
                ", db='" + db + '\'' +
                ", table='" + table + '\'' +
                ", labelPrefix='" + labelPrefix + '\'' +
                ", numberOfSubtasks=" + numberOfSubtasks +
                ", subtaskIndex=" + subTaskIndex +
                ", nextId=" + nextId +
                '}';
    }
}
