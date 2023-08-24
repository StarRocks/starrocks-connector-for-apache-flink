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

import com.starrocks.data.load.stream.LabelGenerator;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Label generator factory for exactly-once.
 */
public class ExactlyOnceLabelGeneratorFactory implements LabelGeneratorFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ExactlyOnceLabelGeneratorFactory.class);

    private final String labelPrefix;
    private final int numberOfSubtasks;
    private final int subtaskIndex;
    private final long restoreCheckpointId;
    // Label generators. The mapping is db -> table -> generator.
    private final Map<String, Map<String, ExactlyOnceLabelGenerator>> labelGenerators;

    public ExactlyOnceLabelGeneratorFactory(String labelPrefix, int numberOfSubtasks, int subtaskIndex, long restoreCheckpointId) {
        this.labelPrefix = labelPrefix;
        this.numberOfSubtasks = numberOfSubtasks;
        this.subtaskIndex = subtaskIndex;
        this.restoreCheckpointId = restoreCheckpointId;
        this.labelGenerators = new ConcurrentHashMap<>();
        LOG.info("Create label generator factory. labelPrefix: {}, numberOfSubtasks: {}, subtaskIndex: {}, " +
                "restoreCheckpointId: {}", labelPrefix, numberOfSubtasks, subtaskIndex, restoreCheckpointId);
    }

    @Override
    public synchronized LabelGenerator create(String db, String table) {
            Map<String, ExactlyOnceLabelGenerator> tableMap = labelGenerators.computeIfAbsent(db, key -> new HashMap<>());
            ExactlyOnceLabelGenerator generator = tableMap.get(table);
            if (generator == null) {
                // use restoreCheckpointId + 1 as the initial id rather than 0 to avoid
                // conflicts because the same labelPrefix is switched repeatedly
                generator = new ExactlyOnceLabelGenerator(labelPrefix, db, table,
                        numberOfSubtasks, subtaskIndex, restoreCheckpointId + 1);
                tableMap.put(table, generator);
                LOG.info("Create label generator: {}", generator);
            }
            return generator;
    }

    public synchronized List<ExactlyOnceLabelGeneratorSnapshot> snapshot(long checkpointId) {
        List<ExactlyOnceLabelGeneratorSnapshot> metas = new ArrayList<>();
        for (Map.Entry<String, Map<String, ExactlyOnceLabelGenerator>> entry : labelGenerators.entrySet()) {
            for (ExactlyOnceLabelGenerator generator : entry.getValue().values()) {
                metas.add(generator.snapshot(checkpointId));
            }
        }
        return metas;
    }

    public synchronized void restore(List<ExactlyOnceLabelGeneratorSnapshot> snapshots) {
        Map<ExactlyOnceLabelGenerator.LabelDbTableSubtask, ExactlyOnceLabelGeneratorSnapshot> map = new HashMap<>();
        for (ExactlyOnceLabelGeneratorSnapshot snapshot : snapshots) {
            if (snapshot.getSubtaskIndex() != subtaskIndex && !snapshot.getLabelPrefix().equals(labelPrefix)) {
                LOG.info("Skip snapshot: {}", snapshot);
                continue;
            }

            ExactlyOnceLabelGenerator.LabelDbTableSubtask meta = snapshot.createLabelDbTableSubtask();
            ExactlyOnceLabelGeneratorSnapshot oldSnapshot = map.get(meta);
            // Sanity check that there should not have duplicated snapshot for a LabelDbTableSubtask
            if (oldSnapshot != null) {
                LOG.warn("Find duplicate snapshot, old snapshot: {}, new snapshot: {}", oldSnapshot, snapshot);
            }
            map.put(meta, snapshot);
        }

        for (ExactlyOnceLabelGeneratorSnapshot snapshot : map.values()) {
            ExactlyOnceLabelGenerator generator = new ExactlyOnceLabelGenerator(
                    labelPrefix, snapshot.getDb(), snapshot.getTable(), numberOfSubtasks, subtaskIndex, snapshot.getNextId());
            labelGenerators.computeIfAbsent(snapshot.getDb(), key -> new HashMap<>())
                    .put(snapshot.getTable(), generator);
            LOG.info("Restore snapshot: {}, generator: {}", snapshot, generator);
        }
    }
}
