/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Plans the scan once and hands out one split per reader request. Planning talks to the FE,
 * so it runs off the coordinator thread.
 */
public class StarRocksSourceEnumerator
        implements SplitEnumerator<StarRocksSourceSplit, StarRocksSourceEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSourceEnumerator.class);

    private final SplitEnumeratorContext<StarRocksSourceSplit> context;
    private final Callable<List<StarRocksSourceSplit>> splitDiscoverer;
    private final Deque<StarRocksSourceSplit> pendingSplits;
    private final Set<Integer> readersAwaitingSplit = new LinkedHashSet<>();
    private boolean splitsDiscovered;

    public StarRocksSourceEnumerator(
            SplitEnumeratorContext<StarRocksSourceSplit> context,
            Callable<List<StarRocksSourceSplit>> splitDiscoverer,
            StarRocksSourceEnumeratorState state) {
        this.context = context;
        this.splitDiscoverer = splitDiscoverer;
        this.pendingSplits = new ArrayDeque<>(state.getPendingSplits());
        this.splitsDiscovered = state.isSplitsDiscovered();
    }

    @Override
    public void start() {
        if (splitsDiscovered) {
            LOG.info("Restored {} pending splits, skipping split discovery", pendingSplits.size());
            return;
        }
        context.callAsync(splitDiscoverer, this::onSplitsDiscovered);
    }

    private void onSplitsDiscovered(List<StarRocksSourceSplit> splits, @Nullable Throwable error) {
        if (error != null) {
            throw new FlinkRuntimeException("Failed to discover splits from the StarRocks query plan", error);
        }
        LOG.info("Discovered {} splits", splits.size());
        pendingSplits.addAll(splits);
        splitsDiscovered = true;
        assignPendingSplits();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        readersAwaitingSplit.add(subtaskId);
        assignPendingSplits();
    }

    private void assignPendingSplits() {
        Iterator<Integer> readers = readersAwaitingSplit.iterator();
        while (readers.hasNext()) {
            int subtaskId = readers.next();
            if (!context.registeredReaders().containsKey(subtaskId)) {
                readers.remove();
                continue;
            }
            StarRocksSourceSplit split = pendingSplits.poll();
            if (split != null) {
                context.assignSplit(split, subtaskId);
                LOG.info("Assigned split {} to subtask {}", split.splitId(), subtaskId);
            } else if (splitsDiscovered) {
                context.signalNoMoreSplits(subtaskId);
            } else {
                return;
            }
            readers.remove();
        }
    }

    @Override
    public void addSplitsBack(List<StarRocksSourceSplit> splits, int subtaskId) {
        LOG.info("Subtask {} returned {} splits", subtaskId, splits.size());
        pendingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // readers ask for splits themselves, see StarRocksSourceReader
    }

    @Override
    public StarRocksSourceEnumeratorState snapshotState(long checkpointId) {
        return new StarRocksSourceEnumeratorState(splitsDiscovered, new ArrayList<>(pendingSplits));
    }

    @Override
    public void close() {
    }
}
