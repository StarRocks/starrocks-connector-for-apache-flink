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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/** Tests for {@link ExactlyOnceLabelGenerator} and {@link ExactlyOnceLabelGeneratorFactory}. */
public class ExactlyOnceLabelTest {

    @Test
    public void testGenerator() {
        ExactlyOnceLabelGenerator generator = new ExactlyOnceLabelGenerator(
                "test_label",
                "test_db",
                "test_table",
                5,
                2,
                10);
        for (int i = 10; i < 20; i++) {
            assertEquals("test_label-test_table-2-" + i, generator.next());
        }
    }

    @Test
    public void testGeneratorFactory() {
        ExactlyOnceLabelGeneratorFactory factory = new ExactlyOnceLabelGeneratorFactory(
                "test_label", 7, 3, 34);

        ExactlyOnceLabelGenerator generator1 = factory.create("db1", "tbl1");
        ExactlyOnceLabelGenerator generator2 = factory.create("db1", "tbl2");
        ExactlyOnceLabelGenerator generator3 = factory.create("db2", "tbl1");

        assertNotSame(generator1, generator2);
        assertNotSame(generator1, generator3);
        assertSame(generator1, factory.create("db1", "tbl1"));

        for (int i = 35; i < 50; i++) {
            for (ExactlyOnceLabelGenerator generator : Arrays.asList(generator1, generator2, generator3)) {
                String label = String.format("test_label-%s-3-%s", generator.getTable(), i);
                assertEquals(label, generator.next());
            }
        }
    }

    @Test
    public void testSnapshot() {
        ExactlyOnceLabelGeneratorFactory factory = new ExactlyOnceLabelGeneratorFactory(
                "test_label", 7, 3, 34);

        List<ExactlyOnceLabelGenerator> generators = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            String db = "db" + i;
            for (int j = 0; j < 5; j++) {
                generators.add(factory.create(db, "tbl" + j));
            }
        }

        Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 1000; i++) {
            int index = random.nextInt(generators.size());
            generators.get(index).next();
        }

        List<ExactlyOnceLabelGeneratorSnapshot> snapshots5 = factory.snapshot(5);
        assertEquals(generators.size(), snapshots5.size());
        Map<ExactlyOnceLabelGenerator.LabelDbTableSubtask, ExactlyOnceLabelGenerator> map = new HashMap<>();
        generators.forEach(gen -> map.put(gen.createLabelDbTableSubtask(), gen));
        for (ExactlyOnceLabelGeneratorSnapshot snapshot : snapshots5) {
            ExactlyOnceLabelGenerator generator = map.remove(snapshot.createLabelDbTableSubtask());
            assertNotNull(generator);
            verifySnapshot(5, generator, snapshot);
        }

        for (int i = 10; i < 15; i++) {
            String db = "db" + i;
            for (int j = 0; j < 5; j++) {
                generators.add(factory.create(db, "tbl" + j));
            }
        }
        for (int i = 0; i < 1000; i++) {
            int index = random.nextInt(generators.size());
            generators.get(index).next();
        }

        List<ExactlyOnceLabelGeneratorSnapshot> snapshots9 = factory.snapshot(9);
        assertEquals(generators.size(), snapshots9.size());
        Map<ExactlyOnceLabelGenerator.LabelDbTableSubtask, ExactlyOnceLabelGenerator> map1 = new HashMap<>();
        generators.forEach(gen -> map1.put(gen.createLabelDbTableSubtask(), gen));
        for (ExactlyOnceLabelGeneratorSnapshot snapshot : snapshots9) {
            ExactlyOnceLabelGenerator generator = map1.remove(snapshot.createLabelDbTableSubtask());
            assertNotNull(generator);
            verifySnapshot(9, generator, snapshot);
        }
    }

    @Test
    public void testRestoreWithoutRescaleAndLabelPrefixChange() {
        ExactlyOnceLabelGeneratorFactory factory = new ExactlyOnceLabelGeneratorFactory(
                "test_label", 7, 3, 34);

        ExactlyOnceLabelGeneratorSnapshot snapshot1 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db1", "tbl1", "test_label", 7, 3, 10);
        ExactlyOnceLabelGeneratorSnapshot snapshot2 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db1", "tbl2", "test_label", 7, 3, 5);
        ExactlyOnceLabelGeneratorSnapshot snapshot3 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db2", "tbl1", "test_label", 7, 3, 30);
        ExactlyOnceLabelGeneratorSnapshot snapshot4 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db2", "tbl2", "test_label", 7, 3, 23);

        List<ExactlyOnceLabelGeneratorSnapshot> snapshots = Arrays.asList(snapshot1, snapshot2, snapshot3, snapshot4);
        factory.restore(snapshots);
        assertEquals(snapshots.size(), factory.numGenerators());
        for (ExactlyOnceLabelGeneratorSnapshot snapshot : snapshots) {
            ExactlyOnceLabelGenerator generator = factory.create(snapshot.getDb(), snapshot.getTable());
            verifySnapshot(snapshot.getCheckpointId(), generator, snapshot);
        }
    }

    @Test
    public void testRestoreWithNewLabelPrefix() {
        ExactlyOnceLabelGeneratorFactory factory = new ExactlyOnceLabelGeneratorFactory(
                "test_label", 7, 3, 34);

        ExactlyOnceLabelGeneratorSnapshot snapshot1 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db1", "tbl1", "test_label_old", 7, 3, 10);
        ExactlyOnceLabelGeneratorSnapshot snapshot2 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db2", "tbl1", "test_label_old", 7, 3, 30);

        List<ExactlyOnceLabelGeneratorSnapshot> snapshots = Arrays.asList(snapshot1, snapshot2);
        factory.restore(snapshots);
        assertEquals(0, factory.numGenerators());
    }

    @Test
    public void testRestoreWithRescale() {
        // Only need to test scale down where a new subtask maybe receive snapshots from multiple old subtasks.
        // In scale up, each new subtask only receive snapshots from at most one of old subtasks, which is a
        // special case of no rescale
        ExactlyOnceLabelGeneratorFactory factory = new ExactlyOnceLabelGeneratorFactory(
                "test_label", 5, 2, 34);

        // from old subtask 1, and should not restore
        ExactlyOnceLabelGeneratorSnapshot snapshot11 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db1", "tbl1", "test_label", 7, 1, 10);
        ExactlyOnceLabelGeneratorSnapshot snapshot12 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db2", "tbl1", "test_label", 7, 1, 11);

        // from old subtask 2, and should not restore
        ExactlyOnceLabelGeneratorSnapshot snapshot21 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db1", "tbl1", "test_label", 7, 2, 5);
        ExactlyOnceLabelGeneratorSnapshot snapshot22 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db2", "tbl1", "test_label", 7, 2, 30);

        // from old subtask 3, and should not restore
        ExactlyOnceLabelGeneratorSnapshot snapshot31 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db1", "tbl1", "test_label", 7, 3, 23);
        ExactlyOnceLabelGeneratorSnapshot snapshot32 = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db2", "tbl1", "test_label", 7, 3, 25);

        List<ExactlyOnceLabelGeneratorSnapshot> snapshots =
                Arrays.asList(snapshot11, snapshot12, snapshot21, snapshot22, snapshot31, snapshot32);
        factory.restore(snapshots);
        assertEquals(2, factory.numGenerators());

        ExactlyOnceLabelGeneratorSnapshot snapshot21_restore = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db1", "tbl1", "test_label", 5, 2, 5);
        ExactlyOnceLabelGeneratorSnapshot snapshot22_restore = new ExactlyOnceLabelGeneratorSnapshot(
                34, "db2", "tbl1", "test_label", 5, 2, 30);
        for (ExactlyOnceLabelGeneratorSnapshot snapshot : Arrays.asList(snapshot21_restore, snapshot22_restore)) {
            ExactlyOnceLabelGenerator generator = factory.create(snapshot.getDb(), snapshot.getTable());
            verifySnapshot(snapshot.getCheckpointId(), generator, snapshot);
        }
    }

    private void verifySnapshot(long checkpointId, ExactlyOnceLabelGenerator generator, ExactlyOnceLabelGeneratorSnapshot snapshot) {
        assertEquals(checkpointId, snapshot.getCheckpointId());
        assertEquals(generator.getLabelPrefix(), snapshot.getLabelPrefix());
        assertEquals(generator.getDb(), snapshot.getDb());
        assertEquals(generator.getTable(), snapshot.getTable());
        assertEquals(generator.getNumberOfSubtasks(), snapshot.getNumberOfSubtasks());
        assertEquals(generator.getSubTaskIndex(), snapshot.getSubTaskIndex());
        assertEquals(generator.getNextId(), snapshot.getNextId());
    }
}
