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

import com.starrocks.connector.flink.tools.JsonWrapper;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class StarrocksSnapshotStateTest {

    @Test
    public void testSerDataAndLabelSnapshots() throws Exception {
        testSerDerBase(buildDataSnapshot(), buildLabelGeneratorSnapshots());
    }

    @Test
    public void testSerNullLabelSnapshots() throws Exception {
        testSerDerBase(buildDataSnapshot(), null);
    }

    private void testSerDerBase(Map<Long, List<StreamLoadSnapshot>> data, List<ExactlyOnceLabelGeneratorSnapshot> labelSnapshots) throws Exception {
        StarRocksVersionedSerializer serializer = new StarRocksVersionedSerializer(new JsonWrapper());
        StarrocksSnapshotState state = StarrocksSnapshotState.of(data, labelSnapshots);
        byte[] serData = serializer.serialize(state);
        StarrocksSnapshotState restoreState = serializer.deserialize(serializer.getVersion(), serData);
        verifyState(state, restoreState);
    }

    private void verifyState(StarrocksSnapshotState expectState, StarrocksSnapshotState actualState) {
        assertEquals(expectState.getVersion(), actualState.getVersion());
        verifyData(expectState.getData(), actualState.getData());
        verifyLabelSnapshots(expectState.getLabelSnapshots(), actualState.getLabelSnapshots());
    }

    private void verifyData(Map<Long, List<StreamLoadSnapshot>> expect, Map<Long, List<StreamLoadSnapshot>> actual) {
        if (expect == null || actual == null) {
            assertNull(expect);
            assertNull(actual);
            return;
        }

        assertEquals(expect.size(), actual.size());
        for (Map.Entry<Long, List<StreamLoadSnapshot>> entry : expect.entrySet()) {
            List<StreamLoadSnapshot> actualList = actual.get(entry.getKey());
            assertNotNull(actualList);
            assertEquals(entry.getValue().size(), actualList.size());
            for (int i = 0; i < actualList.size(); i++) {
                verifyStreamLoadSnapshot(entry.getValue().get(i), actualList.get(i));
            }
        }
    }

    private void verifyStreamLoadSnapshot(StreamLoadSnapshot expect, StreamLoadSnapshot actual) {
        assertEquals(expect.getId(), actual.getId());
        assertEquals(expect.getTimestamp(), actual.getTimestamp());
        assertEquals(expect.getTransactions().size(), actual.getTransactions().size());
        for (int i = 0; i < expect.getTransactions().size(); i++) {
            StreamLoadSnapshot.Transaction expectTxn = expect.getTransactions().get(i);
            StreamLoadSnapshot.Transaction actualTxn = actual.getTransactions().get(i);
            assertEquals(expectTxn.getDatabase(), actualTxn.getDatabase());
            assertEquals(expectTxn.getTable(), actualTxn.getTable());
            assertEquals(expectTxn.getLabel(), actualTxn.getLabel());
            assertEquals(expectTxn.isFinish(), actualTxn.isFinish());
        }
    }

    private void verifyLabelSnapshots(List<ExactlyOnceLabelGeneratorSnapshot> expect, List<ExactlyOnceLabelGeneratorSnapshot> actual) {
        if (expect == null || actual == null) {
            assertNull(expect);
            assertNull(actual);
            return;
        }

        assertEquals(expect.size(), actual.size());
        for (int i = 0; i < expect.size(); i++) {
            assertEquals(expect.get(i), actual.get(i));
        }
    }

    public Map<Long, List<StreamLoadSnapshot>> buildDataSnapshot() {
        Random random = new Random(System.currentTimeMillis());
        Map<Long, List<StreamLoadSnapshot>> data = new HashMap<>();
        for (long i = 0; i < 10; i++) {
            int num = random.nextInt(2);
            List<StreamLoadSnapshot> snapshots = new ArrayList<>();
            for (int j = 0; j < num; j++) {
                snapshots.add(buildStreamLoadSnapshot(random));
            }
            data.put(i, snapshots);
        }
        return data;
    }

    private StreamLoadSnapshot buildStreamLoadSnapshot(Random random) {
        StreamLoadSnapshot snapshot = new StreamLoadSnapshot();
        snapshot.setId(UUID.randomUUID().toString());
        List<StreamLoadSnapshot.Transaction> txns = new ArrayList<>();
        for (int i = 0; i < random.nextInt(5) + 1; i++) {
            txns.add(buildTransaction(random));
        }
        snapshot.setTransactions(txns);
        snapshot.setTimestamp(System.currentTimeMillis());
        return snapshot;
    }

    private StreamLoadSnapshot.Transaction buildTransaction(Random random) {
        return new StreamLoadSnapshot.Transaction(
                Integer.toString(random.nextInt()),
                Integer.toString(random.nextInt()),
                Integer.toString(random.nextInt()));
    }

    private List<ExactlyOnceLabelGeneratorSnapshot> buildLabelGeneratorSnapshots() {
        Random random = new Random(System.currentTimeMillis());
        List<ExactlyOnceLabelGeneratorSnapshot> list = new ArrayList<>();
        int num = random.nextInt(10);
        for (int i = 0; i < 10; i++) {
            list.add(buildLabelSnapshot(random));
        }
        return list;
    }

    private ExactlyOnceLabelGeneratorSnapshot buildLabelSnapshot(Random random) {
        return new ExactlyOnceLabelGeneratorSnapshot(
                random.nextInt(1000),
                Integer.toString(random.nextInt()),
                Integer.toString(random.nextInt()),
                Integer.toString(random.nextInt()),
                random.nextInt(100),
                random.nextInt(100),
                random.nextInt(10000));
    }
}
