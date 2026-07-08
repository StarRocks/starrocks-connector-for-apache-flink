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
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/**
 * Reads the tablets of one split at a time through the BE scan protocol. Blocking
 * happens here on the harness fetcher thread, keeping the task-thread pollNext
 * contract of the FLIP-27 reader non-blocking.
 */
public class StarRocksSplitReader implements SplitReader<GenericRowData, StarRocksSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSplitReader.class);
    private static final int FETCH_BATCH_ROWS = 4096;

    private final StarRocksSourceOptions sourceOptions;
    private final List<ColumnRichInfo> columnRichInfos;
    private final SelectColumn[] selectColumns;

    private final Queue<StarRocksSourceSplit> splits = new ArrayDeque<>();
    private StarRocksSourceSplit currentSplit;
    private StarRocksSourceDataReader currentReader;

    public StarRocksSplitReader(
            StarRocksSourceOptions sourceOptions,
            List<ColumnRichInfo> columnRichInfos,
            SelectColumn[] selectColumns) {
        this.sourceOptions = sourceOptions;
        this.columnRichInfos = columnRichInfos;
        this.selectColumns = selectColumns;
    }

    @Override
    public RecordsWithSplitIds<GenericRowData> fetch() throws IOException {
        if (currentReader == null) {
            currentSplit = splits.poll();
            if (currentSplit == null) {
                return new RecordsBySplits.Builder<GenericRowData>().build();
            }
            openCurrentSplit();
        }

        RecordsBySplits.Builder<GenericRowData> builder = new RecordsBySplits.Builder<>();
        int batched = 0;
        while (batched < FETCH_BATCH_ROWS && currentReader.hasNext()) {
            GenericRowData row = currentReader.getNext();
            if (row != null) {
                builder.add(currentSplit.splitId(), row);
                batched++;
            }
        }
        if (!currentReader.hasNext()) {
            builder.addFinishedSplit(currentSplit.splitId());
            LOG.info("Finished split {}", currentSplit.splitId());
            closeCurrentSplit();
        }
        return builder.build();
    }

    private void openCurrentSplit() throws IOException {
        String splitId = currentSplit.splitId();
        if (splitId.startsWith("count-")) {
            long count = Long.parseLong(splitId.substring("count-".length()));
            currentReader = new StarRocksSourceTrickReader(count);
        } else {
            QueryBeXTablets beXTablets = currentSplit.getBeXTablets();
            StarRocksSourceBeReader beReader = new StarRocksSourceBeReader(
                    beXTablets.getBeNode(), columnRichInfos, selectColumns, sourceOptions);
            try {
                beReader.openScanner(
                        beXTablets.getTabletIds(),
                        currentSplit.getOpaquedQueryPlan(),
                        sourceOptions);
                beReader.startToRead();
            } catch (Exception e) {
                beReader.close();
                throw new IOException("Failed to open split " + splitId, e);
            }
            currentReader = beReader;
        }
        LOG.info("Opened split {} for reading", splitId);
    }

    private void closeCurrentSplit() {
        if (currentReader != null) {
            currentReader.close();
            currentReader = null;
            currentSplit = null;
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
        closeCurrentSplit();
    }
}
