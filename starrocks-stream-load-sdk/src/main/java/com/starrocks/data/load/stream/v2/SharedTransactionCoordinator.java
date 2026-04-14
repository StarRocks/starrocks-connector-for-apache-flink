/*
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

package com.starrocks.data.load.stream.v2;

import com.starrocks.data.load.stream.LabelGenerator;
import com.starrocks.data.load.stream.LabelGeneratorFactory;
import com.starrocks.data.load.stream.StreamLoadSnapshot;
import com.starrocks.data.load.stream.StreamLoader;
import com.starrocks.data.load.stream.exception.StreamLoadFailException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Coordinates a single StarRocks transaction across multiple table regions.
 *
 * <p>In multi-table transaction mode, all table regions within one commit cycle
 * share a single transaction label. This coordinator manages the lifecycle:
 * <ol>
 *   <li>{@link #begin} — generate a shared label and call {@code /api/transaction/begin} once</li>
 *   <li>Each region sends data via {@code /api/transaction/load} using the shared label</li>
 *   <li>{@link #prepareAndCommit} — call {@code /api/transaction/prepare} and
 *       {@code /api/transaction/commit} once for the shared label</li>
 * </ol>
 *
 * <p>The coordinator does not own any data buffers; data management remains in
 * {@link TransactionTableRegion}.
 */
public class SharedTransactionCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(SharedTransactionCoordinator.class);

    private final StreamLoader streamLoader;
    private final LabelGeneratorFactory labelGeneratorFactory;

    private String sharedLabel;
    private String database;
    private String table;

    /** Tracks whether any HTTP load was sent under the current shared label. */
    private boolean dataLoaded;

    /** Timestamp (millis) when the current shared transaction was opened. */
    private long beginTimeMs;

    public SharedTransactionCoordinator(StreamLoader streamLoader,
                                        LabelGeneratorFactory labelGeneratorFactory) {
        this.streamLoader = streamLoader;
        this.labelGeneratorFactory = labelGeneratorFactory;
    }

    /**
     * Begins a shared transaction.
     *
     * <p>Generates a new label and issues a single {@code /api/transaction/begin}
     * request to StarRocks at the database level (no table binding). The label is
     * then injected into all regions that have data to load.
     *
     * @param database the database (all tables must be in the same database)
     * @param anyTable any table in the database (used only for label generation)
     */
    public synchronized void begin(String database, String anyTable) {
        LabelGenerator generator = labelGeneratorFactory.create(database, anyTable);
        String newLabel = generator.next();

        LOG.info("[MultiTxn] SharedTransaction begin: label={}, db={}", newLabel, database);

        // Invariant: coordinator state (sharedLabel/database/table) must reflect a
        // transaction that was actually opened on the FE. We therefore only publish
        // the new label after beginTransaction() succeeds. If the RPC fails — either
        // by returning false or by throwing — isActive() continues to report false,
        // so the next manager cycle will cleanly re-drive ensureSharedTransaction
        // instead of reusing a ghost label that was never created server-side.
        boolean ok;
        try {
            ok = streamLoader.beginTransaction(newLabel, database);
        } catch (RuntimeException ex) {
            clearState();
            throw ex;
        }
        if (!ok) {
            clearState();
            throw new StreamLoadFailException(
                    "Failed to begin shared transaction, label: " + newLabel +
                    ", db: " + database);
        }

        this.sharedLabel = newLabel;
        this.database = database;
        this.table = anyTable;
        this.dataLoaded = false;
        this.beginTimeMs = System.currentTimeMillis();
    }

    private void clearState() {
        this.sharedLabel = null;
        this.database = null;
        this.table = null;
        this.dataLoaded = false;
    }

    /**
     * Marks that at least one HTTP load has been sent under the current shared label.
     * Called by the manager when a region triggers a load.
     */
    public synchronized void markDataLoaded() {
        this.dataLoaded = true;
    }

    /**
     * Returns {@code true} if any data has been loaded under the current shared label.
     */
    public synchronized boolean hasDataLoaded() {
        return dataLoaded;
    }

    /**
     * Injects the shared label into all regions that have pending data.
     * After this call, each region's {@code streamLoad()} will use the shared label
     * (because {@code TransactionStreamLoader.begin(region)} skips begin when
     * label is already set).
     */
    public synchronized void injectLabel(Collection<TransactionTableRegion> regions) {
        for (TransactionTableRegion region : regions) {
            region.setLabel(sharedLabel);
        }
        LOG.info("[MultiTxn] Injected sharedLabel={} into {} regions", sharedLabel, regions.size());
    }

    /**
     * Executes a unified prepare + commit for the shared transaction.
     *
     * <p>Must be called after all regions have completed their HTTP loads.
     * Uses any table from the database (StarRocks resolves by label).
     *
     * @param anyTable any table in the database
     */
    public synchronized void prepareAndCommit(String anyTable) {
        StreamLoadSnapshot.Transaction txn =
                new StreamLoadSnapshot.Transaction(database, anyTable, sharedLabel, true);

        // Skip prepare for multi-table transactions — StarRocks does not
        // support TXN_PREPARE in multi-table transaction mode. Go directly
        // to commit.
        LOG.info("[MultiTxn] SharedTransaction commit (skip prepare): label={}", sharedLabel);
        if (!streamLoader.commit(txn)) {
            throw new StreamLoadFailException(
                    "Failed to commit shared transaction, label: " + sharedLabel);
        }

        LOG.info("[MultiTxn] SharedTransaction committed successfully: label={}", sharedLabel);
        this.sharedLabel = null;
        this.database = null;
        this.table = null;
        this.dataLoaded = false;
    }

    public synchronized String getSharedLabel() {
        return sharedLabel;
    }

    /**
     * Returns the database of the currently active shared transaction, or
     * {@code null} when no transaction is active. Callers injecting the shared
     * label into newly-created regions must reject any region whose database
     * does not match this value: shared transactions are single-database by
     * construction (see {@code ensureSharedTransaction}) and a cross-database
     * label injection would cause the region's first flush to POST to StarRocks
     * with a label that belongs to a different database.
     */
    public synchronized String getDatabase() {
        return database;
    }

    public synchronized boolean isActive() {
        return sharedLabel != null;
    }

    /**
     * Returns the elapsed time in milliseconds since the shared transaction was opened.
     * Returns 0 if no transaction is active.
     */
    public synchronized long getElapsedMs() {
        return sharedLabel != null ? System.currentTimeMillis() - beginTimeMs : 0;
    }

    /**
     * Attempts to rollback the in-progress shared transaction, then resets state.
     * Used on error paths and savepoint interruption. If rollback fails, the
     * StarRocks-side transaction will be cleaned up by its timeout.
     */
    public synchronized void reset() {
        if (sharedLabel != null) {
            LOG.warn("[MultiTxn] SharedTransactionCoordinator reset, attempting rollback for label={}", sharedLabel);
            try {
                StreamLoadSnapshot.Transaction txn =
                        new StreamLoadSnapshot.Transaction(database, table, sharedLabel, true);
                streamLoader.rollback(txn);
                LOG.info("[MultiTxn] Rollback succeeded for label={}", sharedLabel);
            } catch (Exception ex) {
                LOG.warn("[MultiTxn] Rollback failed for label={}, will rely on server-side timeout",
                        sharedLabel, ex);
            }
        }
        this.sharedLabel = null;
        this.database = null;
        this.table = null;
        this.dataLoaded = false;
    }
}
