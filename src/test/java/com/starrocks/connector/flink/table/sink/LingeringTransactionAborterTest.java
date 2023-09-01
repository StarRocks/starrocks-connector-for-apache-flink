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

import com.starrocks.data.load.stream.StreamLoadManager;
import com.starrocks.data.load.stream.StreamLoadResponse;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.TableRegion;
import com.starrocks.data.load.stream.TransactionStatus;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LingeringTransactionAborterTest {

    @Test
    public void testRestoreWithSnapshot() throws Exception {
        String labelPrefix = "test_label";
        int numberOfSubtasks = 10;
        int subtaskIndex = 1;
        long restoreCheckpointId = 10;
        Random random = new Random(System.currentTimeMillis());

        List<Tuple2<String, String>> dbTables = new ArrayList<>();
        List<ExactlyOnceLabelGeneratorSnapshot> snapshots = new ArrayList<>();
        MockStreamLoader streamLoader = new MockStreamLoader();

        for (int dbId = 0; dbId < 3; dbId++) {
            for (int tblId = 0; tblId < 5; tblId++) {
                String dbName = "db" + dbId;
                String tblName = "tbl" + tblId;
                dbTables.add(Tuple2.of(dbName, tblName));
                long nextId = random.nextInt(100) + 1;
                ExactlyOnceLabelGeneratorSnapshot snapshot = new ExactlyOnceLabelGeneratorSnapshot(
                        restoreCheckpointId, dbName, tblName, labelPrefix, numberOfSubtasks, subtaskIndex, nextId);
                snapshots.add(snapshot);
                boolean hasLingeringTxn = random.nextInt(3) < 2;
                if (hasLingeringTxn) {
                    int numTxns = random.nextInt(5) + 1;
                    for (int i = 0; i < numTxns; i++) {
                        streamLoader.addLabelStatus(dbName, tblName, ExactlyOnceLabelGenerator.genLabel(
                                labelPrefix, tblName, subtaskIndex, i + nextId), TransactionStatus.PREPARED);
                    }
                }
            }
        }

        LingeringTransactionAborter aborter = new LingeringTransactionAborter(labelPrefix,
                restoreCheckpointId, subtaskIndex, -1, dbTables, snapshots, streamLoader);
        aborter.execute();

        for (TransactionStatus status : streamLoader.getAllStatus().values()) {
            assertEquals(TransactionStatus.ABORTED, status);
        }
        assertEquals(0, streamLoader.getNumUnknownTxnToAbort());
    }

    @Test
    public void testRestoreWithoutSnapshot() throws Exception {
        String labelPrefix = "test_label";
        int subtaskIndex = 1;
        long restoreCheckpointId = 10;
        Random random = new Random(System.currentTimeMillis());

        List<Tuple2<String, String>> dbTables = new ArrayList<>();
        List<ExactlyOnceLabelGeneratorSnapshot> snapshots = new ArrayList<>();
        MockStreamLoader streamLoader = new MockStreamLoader();

        for (int dbId = 0; dbId < 3; dbId++) {
            for (int tblId = 0; tblId < 5; tblId++) {
                String dbName = "db" + dbId;
                String tblName = "tbl" + tblId;
                dbTables.add(Tuple2.of(dbName, tblName));
                // nextId should be restoreCheckpointId + 1
                long nextId = restoreCheckpointId + 1;
                boolean hasLingeringTxn = random.nextInt(3) < 2;
                if (hasLingeringTxn) {
                    int numTxns = random.nextInt(5) + 1;
                    for (int i = 0; i < numTxns; i++) {
                        streamLoader.addLabelStatus(dbName, tblName, ExactlyOnceLabelGenerator.genLabel(
                                labelPrefix, tblName, subtaskIndex, i + nextId), TransactionStatus.PREPARED);
                    }
                }
            }
        }

        LingeringTransactionAborter aborter = new LingeringTransactionAborter(labelPrefix,
                restoreCheckpointId, subtaskIndex, -1, dbTables, snapshots, streamLoader);
        aborter.execute();

        for (TransactionStatus status : streamLoader.getAllStatus().values()) {
            assertEquals(TransactionStatus.ABORTED, status);
        }
        assertEquals(0, streamLoader.getNumUnknownTxnToAbort());
    }

    @Test
    public void testRestoreAfterRescaleAndLabelChanged() throws Exception {
        String oldLabelPrefix = "test_label_old";
        String labelPrefix = "test_label";
        int oldNumberOfSubtasks = 10;
        int subtaskIndex = 1;
        long restoreCheckpointId = 10;
        Random random = new Random(System.currentTimeMillis());

        List<Tuple2<String, String>> dbTables = new ArrayList<>();
        List<ExactlyOnceLabelGeneratorSnapshot> snapshots = new ArrayList<>();
        MockStreamLoader streamLoader = new MockStreamLoader();

        for (int dbId = 0; dbId < 3; dbId++) {
            for (int tblId = 0; tblId < 5; tblId++) {
                String dbName = "db" + dbId;
                String tblName = "tbl" + tblId;
                dbTables.add(Tuple2.of(dbName, tblName));
                // construct snapshots for multiple subtasks
                for (int oldSubtaskIndex = subtaskIndex; oldSubtaskIndex < 5; oldSubtaskIndex++) {
                    long nextId = random.nextInt(100) + 1;
                    ExactlyOnceLabelGeneratorSnapshot snapshot = new ExactlyOnceLabelGeneratorSnapshot(
                            restoreCheckpointId, dbName, tblName, oldLabelPrefix, oldNumberOfSubtasks, oldSubtaskIndex,
                            nextId);
                    snapshots.add(snapshot);
                    boolean hasLingeringTxn = random.nextInt(3) < 2;
                    if (hasLingeringTxn) {
                        int numTxns = random.nextInt(5) + 1;
                        for (int i = 0; i < numTxns; i++) {
                            streamLoader.addLabelStatus(dbName, tblName, ExactlyOnceLabelGenerator.genLabel(
                                    oldLabelPrefix, tblName, oldSubtaskIndex, i + nextId), TransactionStatus.PREPARED);
                        }
                    }
                }

                // lingering transactions for new label prefix
                long nextId = restoreCheckpointId + 1;
                boolean hasLingeringTxn = random.nextInt(3) < 2;
                if (hasLingeringTxn) {
                    int numTxns = random.nextInt(5) + 1;
                    for (int i = 0; i < numTxns; i++) {
                        streamLoader.addLabelStatus(dbName, tblName, ExactlyOnceLabelGenerator.genLabel(
                                labelPrefix, tblName, subtaskIndex, i + nextId), TransactionStatus.PREPARED);
                    }
                }
            }
        }

        LingeringTransactionAborter aborter = new LingeringTransactionAborter(labelPrefix,
                restoreCheckpointId, subtaskIndex, -1, dbTables, snapshots, streamLoader);
        aborter.execute();

        for (TransactionStatus status : streamLoader.getAllStatus().values()) {
            assertEquals(TransactionStatus.ABORTED, status);
        }
        assertEquals(0, streamLoader.getNumUnknownTxnToAbort());
    }

    @Test
    public void testAbortSnapshotFailed() throws Exception {
        String labelPrefix = "test_label";
        int numberOfSubtasks = 10;
        int subtaskIndex = 1;
        long restoreCheckpointId = 10;

        List<Tuple2<String, String>> dbTables = new ArrayList<>();
        List<ExactlyOnceLabelGeneratorSnapshot> snapshots = new ArrayList<>();
        MockStreamLoader streamLoader = new MockStreamLoader();

        dbTables.add(Tuple2.of("db1", "tbl1"));
        snapshots.add(new ExactlyOnceLabelGeneratorSnapshot(
                restoreCheckpointId, "db1", "tbl1", labelPrefix, numberOfSubtasks, subtaskIndex, 2));
        String label1 = ExactlyOnceLabelGenerator.genLabel(labelPrefix, "tbl1", subtaskIndex, 2);
        streamLoader.addLabelStatus("db1", "tbl1", label1, TransactionStatus.PREPARED, false);

        dbTables.add(Tuple2.of("db1", "tbl2"));
        snapshots.add(new ExactlyOnceLabelGeneratorSnapshot(
                restoreCheckpointId, "db1", "tbl2", labelPrefix, numberOfSubtasks, subtaskIndex, 2));
        String label2 = ExactlyOnceLabelGenerator.genLabel(labelPrefix, "tbl2", subtaskIndex, 2);
        streamLoader.addLabelStatus("db1", "tbl2", label2, TransactionStatus.PREPARED, true);

        dbTables.add(Tuple2.of("db1", "tbl3"));
        snapshots.add(new ExactlyOnceLabelGeneratorSnapshot(
                restoreCheckpointId, "db1", "tbl3", labelPrefix, numberOfSubtasks, subtaskIndex, 2));
        String label3 = ExactlyOnceLabelGenerator.genLabel(labelPrefix, "tbl3", subtaskIndex, 2);
        streamLoader.addLabelStatus("db1", "tbl3", label3, TransactionStatus.PREPARED, false);

        LingeringTransactionAborter aborter = new LingeringTransactionAborter(labelPrefix,
                restoreCheckpointId, subtaskIndex, -1, dbTables, snapshots, streamLoader);
        try {
            aborter.execute();
            fail();
        } catch (Exception e) {
            assertEquals("artificial exception", e.getCause().getCause().getMessage());
        }

        assertEquals(TransactionStatus.ABORTED, streamLoader.getLoadStatus("db1", "tbl1", label1));
        assertEquals(TransactionStatus.PREPARED, streamLoader.getLoadStatus("db1", "tbl2", label2));
        assertEquals(TransactionStatus.PREPARED, streamLoader.getLoadStatus("db1", "tbl3", label3));
    }

    @Test
    public void testAbortNewLabelPrefixFailed() throws Exception {
        String labelPrefix = "test_label";
        int subtaskIndex = 1;
        long restoreCheckpointId = 10;

        List<Tuple2<String, String>> dbTables = new ArrayList<>();
        List<ExactlyOnceLabelGeneratorSnapshot> snapshots = new ArrayList<>();
        MockStreamLoader streamLoader = new MockStreamLoader();

        dbTables.add(Tuple2.of("db1", "tbl1"));
        String label1 = ExactlyOnceLabelGenerator.genLabel(labelPrefix, "tbl1", subtaskIndex,
                restoreCheckpointId + 1);
        streamLoader.addLabelStatus("db1", "tbl1", label1, TransactionStatus.PREPARED, false);

        dbTables.add(Tuple2.of("db1", "tbl2"));
        String label2 = ExactlyOnceLabelGenerator.genLabel(labelPrefix, "tbl2", subtaskIndex,
                restoreCheckpointId + 1);
        streamLoader.addLabelStatus("db1", "tbl2", label2, TransactionStatus.PREPARED, true);

        dbTables.add(Tuple2.of("db1", "tbl3"));
        String label3 = ExactlyOnceLabelGenerator.genLabel(labelPrefix, "tbl3", subtaskIndex,
                restoreCheckpointId + 1);
        streamLoader.addLabelStatus("db1", "tbl3", label3, TransactionStatus.PREPARED, false);

        LingeringTransactionAborter aborter = new LingeringTransactionAborter(labelPrefix,
                restoreCheckpointId, subtaskIndex, -1, dbTables, snapshots, streamLoader);
        try {
            aborter.execute();
            fail();
        } catch (Exception e) {
            assertEquals("artificial exception", e.getCause().getCause().getMessage());
        }

        assertEquals(TransactionStatus.ABORTED, streamLoader.getLoadStatus("db1", "tbl1", label1));
        assertEquals(TransactionStatus.PREPARED, streamLoader.getLoadStatus("db1", "tbl2", label2));
        assertEquals(TransactionStatus.PREPARED, streamLoader.getLoadStatus("db1", "tbl3", label3));
    }

    @Test
    public void testConcurrentAbort() throws Exception {
        String labelPrefix = "test_label";
        int subtaskIndex = 1;
        long restoreCheckpointId = 10;

        List<Tuple2<String, String>> dbTables = new ArrayList<>();
        List<ExactlyOnceLabelGeneratorSnapshot> snapshots = new ArrayList<>();
        MockStreamLoader streamLoader = new MockStreamLoader();

        dbTables.add(Tuple2.of("db1", "tbl1"));
        String label1 = ExactlyOnceLabelGenerator.genLabel(labelPrefix, "tbl1", subtaskIndex,
                restoreCheckpointId + 1);
        // simulate concurrent operation which maybe happen if two jobs are aborting concurrently,
        // because the old job is not canceled completely
        streamLoader.addLabelStatus("db1", "tbl1", label1, TransactionStatus.PREPARED,
                Collections.singletonList(TransactionStatus.ABORTED));

        LingeringTransactionAborter aborter = new LingeringTransactionAborter(labelPrefix,
                restoreCheckpointId, subtaskIndex, -1, dbTables, snapshots, streamLoader);
        aborter.execute();
        for (TransactionStatus status : streamLoader.getAllStatus().values()) {
            assertEquals(TransactionStatus.ABORTED, status);
        }
        assertEquals(0, streamLoader.getNumUnknownTxnToAbort());
    }

    @Test
    public void testCheckNumber() throws Exception {
        String oldLabelPrefix = "test_label_old";
        String labelPrefix = "test_label";
        int numberOfSubtasks = 10;
        int subtaskIndex = 1;
        long restoreCheckpointId = 10;

        List<Tuple2<String, String>> dbTables = new ArrayList<>();
        List<ExactlyOnceLabelGeneratorSnapshot> snapshots = new ArrayList<>();
        MockStreamLoader streamLoader = new MockStreamLoader();

        dbTables.add(Tuple2.of("db1", "tbl1"));
        ExactlyOnceLabelGeneratorSnapshot snapshot = new ExactlyOnceLabelGeneratorSnapshot(
                restoreCheckpointId, "db1", "tbl1", oldLabelPrefix, numberOfSubtasks, 1, 2);
        snapshots.add(snapshot);
        // discontinuous labels for snapshot
        streamLoader.addLabelStatus("db1", "tbl1", ExactlyOnceLabelGenerator.genLabel(
                oldLabelPrefix, "tbl1", subtaskIndex, 2), TransactionStatus.PREPARED);
        streamLoader.addLabelStatus("db1", "tbl1", ExactlyOnceLabelGenerator.genLabel(
                oldLabelPrefix, "tbl1", subtaskIndex, 4), TransactionStatus.PREPARED);

        // discontinuous labels for new label prefix
        streamLoader.addLabelStatus("db1", "tbl1", ExactlyOnceLabelGenerator.genLabel(
                labelPrefix, "tbl1", subtaskIndex, restoreCheckpointId + 1), TransactionStatus.PREPARED);
        streamLoader.addLabelStatus("db1", "tbl1", ExactlyOnceLabelGenerator.genLabel(
                labelPrefix, "tbl1", subtaskIndex, restoreCheckpointId + 3), TransactionStatus.PREPARED);

        LingeringTransactionAborter aborter = new LingeringTransactionAborter(labelPrefix,
                restoreCheckpointId, subtaskIndex, 4, dbTables, snapshots, streamLoader);
        aborter.execute();

        for (TransactionStatus status : streamLoader.getAllStatus().values()) {
            assertEquals(TransactionStatus.ABORTED, status);
        }
        assertEquals(0, streamLoader.getNumUnknownTxnToAbort());
    }

    @Test
    public void testAbortFinishTransaction() throws Exception {
        String labelPrefix = "test_label";
        int subtaskIndex = 1;
        long restoreCheckpointId = 10;

        List<Tuple2<String, String>> dbTables = new ArrayList<>();
        List<ExactlyOnceLabelGeneratorSnapshot> snapshots = new ArrayList<>();
        MockStreamLoader streamLoader = new MockStreamLoader();

        dbTables.add(Tuple2.of("db1", "tbl1"));
        String label1 = ExactlyOnceLabelGenerator.genLabel(labelPrefix, "tbl1", subtaskIndex,
                restoreCheckpointId + 1);
        // simulate restore from an earlier checkpoint, and the transaction already finished
        streamLoader.addLabelStatus("db1", "tbl1", label1, TransactionStatus.VISIBLE);

        LingeringTransactionAborter aborter = new LingeringTransactionAborter(labelPrefix,
                restoreCheckpointId, subtaskIndex, -1, dbTables, snapshots, streamLoader);
        try {
            aborter.execute();
            fail();
        } catch (Exception e) {
            // ignore
        }
    }

    private static class MockStreamLoader implements StreamLoader {

        // mapping from tuple<db, table, label> to transaction status
        private final Map<Tuple3<String, String, String>, TransactionStatus> labelStatus;

        // status to transit after getLoadStatus is called
        private final Map<Tuple3<String, String, String>, List<TransactionStatus>> transitStatusMap;

        // The behaviour to abort the transaction. Whether throw exception.
        private final Map<Tuple3<String, String, String>, Boolean> labelAbortBehaviours;

        private int numUnknownTxnToAbort = 0;

        public MockStreamLoader() {
            this.labelStatus = new HashMap<>();
            this.transitStatusMap = new HashMap<>();
            this.labelAbortBehaviours = new HashMap<>();
        }

        public Map<Tuple3<String, String, String>, TransactionStatus> getAllStatus() {
            return labelStatus;
        }

        public void addLabelStatus(String db, String table, String label, TransactionStatus initStatus) {
            addLabelStatus(db, table, label, initStatus, Collections.emptyList());
        }

        public void addLabelStatus(String db, String table, String label,
                               TransactionStatus initStatus, List<TransactionStatus> transitStatus) {
            Tuple3<String, String, String> tuple = Tuple3.of(db, table, label);
            labelStatus.put(tuple, initStatus);
            transitStatusMap.put(tuple, (transitStatus == null ? Collections.emptyList() : new ArrayList<>(transitStatus)));
        }

        public void addLabelStatus(String db, String table, String label, TransactionStatus initStatus, boolean aborFail) {
            addLabelStatus(db, table, label, initStatus, Collections.emptyList());
            labelAbortBehaviours.put(Tuple3.of(db, table, label), aborFail);
        }

        @Override
        public TransactionStatus getLoadStatus(String db, String table, String label) throws Exception {
            Tuple3<String, String, String> tuple = Tuple3.of(db, table, label);
            TransactionStatus status = labelStatus.get(tuple);
            List<TransactionStatus> list = transitStatusMap.get(tuple);
            if (list != null && !list.isEmpty()) {
                labelStatus.put(tuple, list.get(0));
                list.remove(0);
            }
            return status == null ? TransactionStatus.UNKNOWN : status;
        }

        @Override
        public boolean rollback(StreamLoadSnapshot.Transaction transaction) {
            Tuple3<String, String, String> tuple = Tuple3.of(
                    transaction.getDatabase(), transaction.getTable(), transaction.getLabel());
            Boolean fail = labelAbortBehaviours.get(tuple);
            if (fail != null && fail) {
                throw new RuntimeException("artificial exception");
            }

            TransactionStatus status = labelStatus.get(tuple);
            // same behaviour as StarRocks DatabaseTransactionMgr#abortTransaction
            if (status == null) {
                numUnknownTxnToAbort += 1;
                throw new RuntimeException("Transaction not found");
            }

            if (status == TransactionStatus.COMMITTED || status == TransactionStatus.VISIBLE) {
                throw new RuntimeException("Transaction already committed");
            }

            if (status == TransactionStatus.PREPARE || status == TransactionStatus.PREPARED) {
                labelStatus.put(tuple, TransactionStatus.ABORTED);
                return true;
            }

            throw new RuntimeException("Unknown transaction state");
        }

        public int getNumUnknownTxnToAbort() {
            return numUnknownTxnToAbort;
        }

        @Override
        public void start(StreamLoadProperties properties, StreamLoadManager manager) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean begin(TableRegion region) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<StreamLoadResponse> send(TableRegion region) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<StreamLoadResponse> send(TableRegion region, int delayMs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean prepare(StreamLoadSnapshot.Transaction transaction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean commit(StreamLoadSnapshot.Transaction transaction) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean prepare(StreamLoadSnapshot snapshot) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean commit(StreamLoadSnapshot snapshot) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean rollback(StreamLoadSnapshot snapshot) {
            throw new UnsupportedOperationException();
        }
    }
}
