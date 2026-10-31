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

package com.starrocks.connector.flink.it.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.starrocks.connector.flink.StarRocksSource;
import com.starrocks.connector.flink.it.StarRocksITTestBase;
import com.starrocks.connector.flink.table.source.StarRocksSourceOptions;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Restores the FLIP-27 source from checkpoints taken while its splits were being read. */
public class StarRocksSourceFailoverITTest extends StarRocksITTestBase {

    private static final int ROWS = 2000;
    // (c0, task attempt, job run)
    private static final Queue<Tuple3<Integer, Integer, Integer>> RECEIVED = new ConcurrentLinkedQueue<>();
    private static final AtomicBoolean FIRST_SUBTASK_FINISHED = new AtomicBoolean();
    private static final AtomicLong FIRST_CHECKPOINT_AFTER_FINISH = new AtomicLong();
    private static final AtomicBoolean CHECKPOINTED_AFTER_FINISH = new AtomicBoolean();
    private static final AtomicBoolean FAILED = new AtomicBoolean();
    private static volatile int jobRun;

    private String tableName;

    @Before
    public void prepare() throws Exception {
        tableName = "failover_" + genRandomUuid();
        executeSrSQL(String.format("CREATE TABLE `%s`.`%s` (c0 INT) ENGINE = OLAP DUPLICATE KEY(c0) " +
                "DISTRIBUTED BY HASH (c0) BUCKETS 8 PROPERTIES (\"replication_num\" = \"1\")", DB_NAME, tableName));
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < ROWS; i++) {
            values.append(i == 0 ? "" : ",").append('(').append(i).append(')');
        }
        executeSrSQL(String.format("INSERT INTO `%s`.`%s` VALUES %s", DB_NAME, tableName, values));
        RECEIVED.clear();
        FIRST_SUBTASK_FINISHED.set(false);
        FIRST_CHECKPOINT_AFTER_FINISH.set(Long.MAX_VALUE);
        CHECKPOINTED_AFTER_FINISH.set(false);
        FAILED.set(false);
        jobRun = 0;
    }

    @Test
    public void testRegionalFailoverRereadsOnlyTheSplitsInFlight() throws Exception {
        Configuration conf = new Configuration();
        conf.setString("restart-strategy.type", "fixed-delay");
        conf.setString("restart-strategy.fixed-delay.attempts", "1");
        conf.setString("restart-strategy.fixed-delay.delay", "100 ms");
        runJob(conf);

        assertTrue("the job must have failed and restarted once", FAILED.get());
        assertRestoreReadOnlyTheSplitsInFlight(row -> row.f1 > 0);
    }

    @Test
    public void testResumeFromRetainedCheckpointRereadsOnlyTheSplitsInFlight() throws Exception {
        // a resumed job re-creates the enumerator from its checkpoint, unlike a regional failover
        Configuration conf = new Configuration();
        conf.setString("restart-strategy.type", "none");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, temporaryFolder.getRoot().toURI().toString());
        conf.set(CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        try {
            runJob(conf);
            fail("the first run must fail");
        } catch (Exception expected) {
            assertTrue(FAILED.get());
        }

        jobRun = 1;
        conf.set(StateRecoveryOptions.SAVEPOINT_PATH, latestCheckpoint(temporaryFolder.getRoot()));
        runJob(conf);

        assertRestoreReadOnlyTheSplitsInFlight(row -> row.f2 > 0);
    }

    private void runJob(Configuration conf) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // one BE and eight tablets give one split per reader; subtask 0 finishes its split first,
        // and the job fails once a checkpoint has recorded that while subtask 1 is still reading
        env.setParallelism(2);
        env.enableCheckpointing(100);

        Configuration sourceConf = new Configuration();
        sourceConf.set(StarRocksSourceOptions.SCAN_URL, getHttpUrls());
        sourceConf.set(StarRocksSourceOptions.JDBC_URL, getJdbcUrl());
        sourceConf.set(StarRocksSourceOptions.USERNAME, USERNAME);
        sourceConf.set(StarRocksSourceOptions.PASSWORD, PASSWORD);
        sourceConf.set(StarRocksSourceOptions.DATABASE_NAME, DB_NAME);
        sourceConf.set(StarRocksSourceOptions.TABLE_NAME, tableName);
        StarRocksSourceOptions options = new StarRocksSourceOptions(sourceConf, sourceConf.toMap());
        ResolvedSchema schema = ResolvedSchema.physical(new String[] {"c0"}, new DataType[] {DataTypes.INT()});

        // chained, so checkpoint barriers are not stuck behind buffered records
        env.fromSource(StarRocksSource.source(schema, options), WatermarkStrategy.noWatermarks(), "StarRocks Source")
                .map(new FailOnceAfterCheckpoint())
                .sinkTo(new CollectingSink());
        env.execute("StarRocksSourceFailoverITTest");
    }

    private static void assertRestoreReadOnlyTheSplitsInFlight(
            Predicate<Tuple3<Integer, Integer, Integer>> afterRestore) {
        Set<Integer> distinct = new HashSet<>();
        int restoredRows = 0;
        for (Tuple3<Integer, Integer, Integer> row : RECEIVED) {
            distinct.add(row.f0);
            if (afterRestore.test(row)) {
                restoredRows++;
            }
        }
        assertEquals("every row must be read at least once", ROWS, distinct.size());
        assertTrue("the restored reader must re-read its in-flight split", restoredRows > 0);
        assertTrue("only the split in flight may be read again, but " + restoredRows + " rows were",
                restoredRows < ROWS);
    }

    private static String latestCheckpoint(File root) throws IOException {
        try (Stream<Path> files = Files.walk(root.toPath())) {
            Path metadata = files
                    .filter(p -> p.getFileName().toString().equals("_metadata"))
                    .max((a, b) -> Long.compare(checkpointId(a), checkpointId(b)))
                    .orElse(null);
            assertNotNull("the first run must retain a checkpoint", metadata);
            return metadata.getParent().toUri().toString();
        }
    }

    private static long checkpointId(Path metadata) {
        return Long.parseLong(metadata.getParent().getFileName().toString().substring("chk-".length()));
    }

    private static class FailOnceAfterCheckpoint extends RichMapFunction<RowData, Tuple3<Integer, Integer, Integer>>
            implements CheckpointedFunction, CheckpointListener {

        @Override
        public Tuple3<Integer, Integer, Integer> map(RowData row) throws Exception {
            if (getRuntimeContext().getTaskInfo().getIndexOfThisSubtask() == 1) {
                if (!FAILED.get() && CHECKPOINTED_AFTER_FINISH.get()) {
                    FAILED.set(true);
                    throw new RuntimeException("Injected failure while the source is reading");
                }
                // keep subtask 1 reading for a few checkpoints
                Thread.sleep(2);
            }
            return Tuple3.of(row.getInt(0), getRuntimeContext().getTaskInfo().getAttemptNumber(), jobRun);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            // the next checkpoint starts after this one completes, so it sees subtask 0 finished
            if (FIRST_SUBTASK_FINISHED.get()) {
                FIRST_CHECKPOINT_AFTER_FINISH.compareAndSet(Long.MAX_VALUE, context.getCheckpointId() + 1);
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (checkpointId >= FIRST_CHECKPOINT_AFTER_FINISH.get()) {
                CHECKPOINTED_AFTER_FINISH.set(true);
            }
        }
    }

    private static class CollectingSink implements Sink<Tuple3<Integer, Integer, Integer>> {

        @Override
        public SinkWriter<Tuple3<Integer, Integer, Integer>> createWriter(WriterInitContext context) {
            int subtask = context.getTaskInfo().getIndexOfThisSubtask();
            return new SinkWriter<Tuple3<Integer, Integer, Integer>>() {
                @Override
                public void write(Tuple3<Integer, Integer, Integer> element, Context context) {
                    RECEIVED.add(element);
                }

                @Override
                public void flush(boolean endOfInput) {
                    if (endOfInput && subtask == 0) {
                        FIRST_SUBTASK_FINISHED.set(true);
                    }
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
