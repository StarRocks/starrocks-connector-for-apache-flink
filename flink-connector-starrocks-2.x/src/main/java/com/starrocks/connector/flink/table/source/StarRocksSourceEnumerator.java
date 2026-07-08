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
import org.apache.flink.api.connector.source.SplitsAssignment;

import com.starrocks.connector.flink.table.source.struct.QueryBeXTablets;
import com.starrocks.connector.flink.table.source.struct.QueryInfo;
import com.starrocks.connector.flink.table.source.struct.SelectColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Enumerator that discovers splits from StarRocks query plan and assigns them to readers. */
public class StarRocksSourceEnumerator
        implements SplitEnumerator<StarRocksSourceSplit, Collection<StarRocksSourceSplit>> {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSourceEnumerator.class);

    private final SplitEnumeratorContext<StarRocksSourceSplit> context;
    private final StarRocksSourceOptions sourceOptions;
    private final String[] columnNames;
    private final String filter;
    private final long limit;
    private final SelectColumn[] selectColumns;
    private final StarRocksSourceQueryType queryType;

    /** Splits not yet assigned to any reader, used for checkpoint state. */
    private final List<StarRocksSourceSplit> unassignedSplits;

    /** Pre-computed per-subtask assignments. Populated in start(), consumed in addReader(). */
    private final Map<Integer, List<StarRocksSourceSplit>> pendingAssignments = new HashMap<>();

    public StarRocksSourceEnumerator(
            SplitEnumeratorContext<StarRocksSourceSplit> context,
            StarRocksSourceOptions sourceOptions,
            String[] columnNames,
            String filter,
            long limit,
            SelectColumn[] selectColumns,
            StarRocksSourceQueryType queryType,
            Collection<StarRocksSourceSplit> restoredSplits) {
        this.context = context;
        this.sourceOptions = sourceOptions;
        this.columnNames = columnNames;
        this.filter = filter;
        this.limit = limit;
        this.selectColumns = selectColumns;
        this.queryType = queryType;
        this.unassignedSplits = new ArrayList<>(restoredSplits);
    }

    @Override
    public void start() {
        if (!unassignedSplits.isEmpty()) {
            // Restored from checkpoint — distribute restored splits round-robin
            distributeRoundRobin(unassignedSplits);
            unassignedSplits.clear();
            return;
        }

        List<StarRocksSourceSplit> discoveredSplits = new ArrayList<>();

        if (queryType == StarRocksSourceQueryType.QueryCount) {
            String sql = StarRocksSourceCommonFunc.buildSQL(
                    queryType, selectColumns, columnNames, sourceOptions, filter);
            Long count = StarRocksSourceCommonFunc.getQueryCount(sourceOptions, sql);
            QueryBeXTablets countTablets = new QueryBeXTablets("count", new ArrayList<>());
            discoveredSplits.add(new StarRocksSourceSplit(countTablets, "count-" + count));
        } else {
            String sql = StarRocksSourceCommonFunc.buildSQL(
                    queryType, selectColumns, columnNames, sourceOptions, filter);
            QueryInfo queryInfo = StarRocksSourceCommonFunc.getQueryInfo(sourceOptions, sql);
            int parallelism = context.currentParallelism();
            List<List<QueryBeXTablets>> splitsBySubtask =
                    StarRocksSourceCommonFunc.splitQueryBeXTablets(parallelism, queryInfo);

            int splitIndex = 0;
            for (List<QueryBeXTablets> subtaskSplits : splitsBySubtask) {
                for (QueryBeXTablets beXTablets : subtaskSplits) {
                    String splitId = "split-" + splitIndex + "-" + beXTablets.getBeNode();
                    discoveredSplits.add(new StarRocksSourceSplit(beXTablets, splitId));
                    splitIndex++;
                }
            }
        }

        LOG.info("Discovered {} splits", discoveredSplits.size());
        distributeRoundRobin(discoveredSplits);
    }

    private void distributeRoundRobin(List<StarRocksSourceSplit> splits) {
        int parallelism = context.currentParallelism();
        for (int i = 0; i < splits.size(); i++) {
            int subtask = i % parallelism;
            pendingAssignments.computeIfAbsent(subtask, k -> new ArrayList<>()).add(splits.get(i));
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // All splits are pre-assigned in addReader(); nothing to do here.
        context.signalNoMoreSplits(subtaskId);
    }

    @Override
    public void addSplitsBack(List<StarRocksSourceSplit> splits, int subtaskId) {
        pendingAssignments.computeIfAbsent(subtaskId, k -> new ArrayList<>()).addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        List<StarRocksSourceSplit> splits = pendingAssignments.remove(subtaskId);
        if (splits != null && !splits.isEmpty()) {
            context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(subtaskId, splits)));
            LOG.info("Assigned {} splits to subtask {}", splits.size(), subtaskId);
        }
        context.signalNoMoreSplits(subtaskId);
    }

    @Override
    public Collection<StarRocksSourceSplit> snapshotState(long checkpointId) throws Exception {
        // Include both unassigned splits and any pending assignments not yet delivered
        List<StarRocksSourceSplit> state = new ArrayList<>(unassignedSplits);
        for (List<StarRocksSourceSplit> pending : pendingAssignments.values()) {
            state.addAll(pending);
        }
        return state;
    }

    @Override
    public void close() throws IOException {
    }
}
