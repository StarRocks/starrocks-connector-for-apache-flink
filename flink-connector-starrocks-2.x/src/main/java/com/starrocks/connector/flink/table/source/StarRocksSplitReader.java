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

package com.starrocks.connector.flink.table.source;

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.GenericRowData;

import com.starrocks.connector.flink.table.source.struct.ColumnRichInfo;
import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryPlan;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import com.starrocks.connector.flink.table.source.struct.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * Reads the tablets of one split at a time through the BE scan protocol. Blocking
 * happens here on the harness fetcher thread, keeping the task-thread pollNext
 * contract of the FLIP-27 reader non-blocking.
 */
public class StarRocksSplitReader implements SplitReader<GenericRowData, StarRocksSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSplitReader.class);
    private static final int FETCH_BATCH_ROWS = 4096;
    private static final long RETRY_DELAY_MS = 1000;

    private final Callable<QueryPlan> planner;
    private final int maxRetries;
    private final long retryDelayMs;
    private final ScanOpener scanOpener;

    private final Queue<StarRocksSourceSplit> splits = new ArrayDeque<>();
    private StarRocksSourceSplit currentSplit;
    // the tablets of the current split left to read, by BE; more than one only after a new plan moved them
    private final Deque<QueryBeXTablets> pendingScans = new ArrayDeque<>();
    private String currentPlan;
    private StarRocksSourceDataReader currentReader;

    public StarRocksSplitReader(
            StarRocksSourceOptions sourceOptions,
            List<ColumnRichInfo> columnRichInfos,
            SelectColumn[] selectColumns,
            Callable<QueryPlan> planner) {
        this(planner, sourceOptions.getScanMaxRetries(), RETRY_DELAY_MS, (beNode, tabletIds, opaquedQueryPlan) -> {
            StarRocksSourceBeReader beReader = new StarRocksSourceBeReader(
                    beNode, columnRichInfos, selectColumns, sourceOptions);
            try {
                beReader.openScanner(tabletIds, opaquedQueryPlan, sourceOptions);
                beReader.startToRead();
            } catch (RuntimeException e) {
                beReader.close();
                throw e;
            }
            return beReader;
        });
    }

    StarRocksSplitReader(Callable<QueryPlan> planner, int maxRetries, long retryDelayMs, ScanOpener scanOpener) {
        this.planner = planner;
        this.maxRetries = maxRetries;
        this.retryDelayMs = retryDelayMs;
        this.scanOpener = scanOpener;
    }

    @Override
    public RecordsWithSplitIds<GenericRowData> fetch() throws IOException {
        RecordsBySplits.Builder<GenericRowData> builder = new RecordsBySplits.Builder<>();
        if (currentReader == null && !openNextScan()) {
            return builder.build();
        }

        int batched = 0;
        while (batched < FETCH_BATCH_ROWS && currentReader.hasNext()) {
            GenericRowData row = currentReader.getNext();
            if (row != null) {
                builder.add(currentSplit.splitId(), row);
                batched++;
            }
        }
        if (!currentReader.hasNext()) {
            closeCurrentReader();
            if (pendingScans.isEmpty()) {
                builder.addFinishedSplit(currentSplit.splitId());
                LOG.info("Finished split {}", currentSplit.splitId());
                currentSplit = null;
            }
        }
        return builder.build();
    }

    // opens the next scan of the current split, or the first one of the next split; false if no split is left
    private boolean openNextScan() throws IOException {
        if (currentSplit == null) {
            currentSplit = splits.poll();
            if (currentSplit == null) {
                return false;
            }
            String splitId = currentSplit.splitId();
            if (splitId.startsWith("count-")) {
                currentReader = new StarRocksSourceTrickReader(Long.parseLong(splitId.substring("count-".length())));
                LOG.info("Opened split {} for reading", splitId);
                return true;
            }
            currentPlan = currentSplit.getOpaquedQueryPlan();
            pendingScans.add(currentSplit.getBeXTablets());
        }
        for (int retry = 0; ; retry++) {
            QueryBeXTablets scan = pendingScans.peek();
            try {
                currentReader = scanOpener.open(scan.getBeNode(), scan.getTabletIds(), currentPlan);
                pendingScans.poll();
                LOG.info("Opened split {} for reading on BE {}", currentSplit.splitId(), scan.getBeNode());
                return true;
            } catch (Exception e) {
                if (retry >= maxRetries) {
                    throw new IOException(String.format("Failed to open split %s on BE %s after %d retries with " +
                            "a new query plan (scan.max-retries)", currentSplit.splitId(), scan.getBeNode(), retry), e);
                }
                // the plan pins tablet versions only once a scanner is open, and BEs drop stale versions after a while
                LOG.warn("Failed to open split {} on BE {}, retrying with a new query plan ({} of {})",
                        currentSplit.splitId(), scan.getBeNode(), retry + 1, maxRetries, e);
                sleepBeforeRetry(retry);
                planAgain(scan.getBeNode(), e);
            }
        }
    }

    // moves the tablets left to the new plan, preferring a replica on another BE than the one that failed
    private void planAgain(String failedBe, Exception openFailure) throws IOException {
        QueryPlan plan;
        try {
            plan = planner.call();
        } catch (Exception e) {
            e.addSuppressed(openFailure);
            throw new IOException("Failed to plan split " + currentSplit.splitId() + " again", e);
        }
        Map<String, Tablet> planTablets = plan.getPartitions() == null ? Collections.emptyMap() : plan.getPartitions();
        Map<String, List<Long>> tabletsByBe = new LinkedHashMap<>();
        List<Long> missing = new ArrayList<>();
        for (QueryBeXTablets scan : pendingScans) {
            for (Long tabletId : scan.getTabletIds()) {
                Tablet tablet = planTablets.get(String.valueOf(tabletId));
                List<String> routings = tablet == null ? null : tablet.getRoutings();
                if (routings == null || routings.isEmpty()) {
                    missing.add(tabletId);
                    continue;
                }
                String beNode = routings.stream().filter(r -> !r.equals(failedBe)).findFirst().orElse(routings.get(0));
                tabletsByBe.computeIfAbsent(beNode, k -> new ArrayList<>()).add(tabletId);
            }
        }
        if (!missing.isEmpty()) {
            throw new IOException(String.format("Tablets %s of split %s are no longer in the query plan of the " +
                    "table, for example because their partition was dropped or replaced: restart the job without " +
                    "state to plan the scan again.", missing, currentSplit.splitId()), openFailure);
        }
        pendingScans.clear();
        tabletsByBe.forEach((beNode, tabletIds) -> pendingScans.add(new QueryBeXTablets(beNode, tabletIds)));
        currentPlan = plan.getOpaqued_query_plan();
    }

    private void sleepBeforeRetry(int retry) throws IOException {
        try {
            Thread.sleep(retryDelayMs * (retry + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while retrying to open split " + currentSplit.splitId(), e);
        }
    }

    private void closeCurrentReader() {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<StarRocksSourceSplit> splitsChange) {
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    "Unsupported splits change " + splitsChange.getClass().getName());
        }
        splits.addAll(splitsChange.splits());
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public void close() {
        closeCurrentReader();
        pendingScans.clear();
        currentSplit = null;
    }

    /** Opens a scan of tablets on a BE and fetches its first rows. */
    interface ScanOpener {
        StarRocksSourceDataReader open(String beNode, List<Long> tabletIds, String opaquedQueryPlan);
    }
}
