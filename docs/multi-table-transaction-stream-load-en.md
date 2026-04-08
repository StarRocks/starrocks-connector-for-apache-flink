# StarRocks Flink Connector — Multi-Table Transaction Stream Load

## 1. Use Cases

When a single Flink job writes to multiple tables within the same StarRocks database in one processing cycle, enabling multi-table transaction guarantees:

- **Cross-table atomic commit**: Data written to `orders` and `order_items` within the same commit cycle becomes visible atomically — all or nothing.
- **Source transaction integrity**: A complete upstream transaction (e.g., from Kafka) is never split across two StarRocks transactions.
- **Sub-second data freshness**: Data continuously flows into StarRocks via `/api/transaction/load`, and is committed at the interval configured by `sink.buffer-flush.interval-ms`.

Typical scenarios:

- Synchronous writes to a master table `orders` and a detail table `order_items`
- Event routing to different partition tables (e.g., `events_202601`, `events_202602`)
- A single job maintaining multiple interrelated downstream result tables

> **Prerequisites**: StarRocks >= 4.0 (multi-table Transaction Stream Load support), Connector >= 1.2.9.

## 2. Core Capabilities

| Capability | Description |
|---|---|
| Cross-table atomic commit | All tables within the same flush cycle share one StarRocks transaction label; unified prepare + commit |
| Source transaction integrity | Commit timing is controlled by the `transactionEnd` flag; commit only occurs at complete source transaction boundaries |
| Sub-second data visibility | Data is periodically flushed to StarRocks (`/api/transaction/load`); committed when `transactionEnd` + timer conditions are met |
| N:1 transaction mapping | Multiple source transactions can accumulate in a single StarRocks transaction; no 1:1 requirement |
| Within-partition ordering | `keyBy(sourcePartition)` ensures transactions from the same partition are processed in order within the same sink subtask |

## 3. Configuration

### 3.1 New Configuration

| Parameter | Type | Default | Description |
|---|---|---|---|
| `sink.transaction.multi-table.enabled` | Boolean | `false` | Enable multi-table atomic transaction mode |
| `sink.transaction.multi-table.buffer-size` | Long | `134217728` (128 MB) | Global buffer size in bytes for multi-table transaction mode. When the total buffered data across all tables reaches this threshold, a flush is triggered |

### 3.2 Related Configuration

| Parameter | Recommended Value | Description |
|---|---|---|
| `sink.version` | `V2` | Required. V1 does not support transaction stream load |
| `sink.semantic` | `at-least-once` | Multi-table mode currently supports at-least-once only |
| `database-name` | `*` | Wildcard to enable dynamic multi-table routing |
| `table-name` | `*` | Wildcard to enable dynamic multi-table routing |
| `sink.buffer-flush.interval-ms` | `1000` | Controls the commit cycle (set to 1000 for ~1s freshness) |
| `sink.properties.format` | `json` | Data format |
| `sink.properties.strip_outer_array` | `true` | JSON array parsing |

## 4. Interface Definitions

### 4.1 StarRocksRowData

```java
public interface StarRocksRowData {
    String getUniqueKey();    // Region routing key (nullable; auto-derived from database.table)
    String getDatabase();     // Target database
    String getTable();        // Target table
    String getRow();          // Row data in JSON format

    /**
     * Indicates this is the last row of a source transaction batch.
     * Used by multi-table transaction mode to determine safe commit points:
     * the connector only commits when the most recent write had this flag set,
     * ensuring no partial source transaction is committed.
     */
    default boolean isTransactionEnd() {
        return false;
    }

    /**
     * Returns the source partition ID for this row.
     * Used by multi-table transaction mode to track per-partition transaction
     * boundaries. Returns -1 when partition tracking is not applicable.
     */
    default int getSourcePartition() {
        return -1;
    }
}
```

### 4.2 DefaultStarRocksRowData

```java
public class DefaultStarRocksRowData implements StarRocksRowData {
    // Basic fields
    private String uniqueKey;
    private String database;
    private String table;
    private String row;

    // Multi-table transaction fields
    private boolean transactionEnd;       // Source transaction end marker
    private int sourcePartition = -1;     // Source partition ID (for keyBy ordering)

    // Constructors
    public DefaultStarRocksRowData();
    public DefaultStarRocksRowData(String database, String table);
    public DefaultStarRocksRowData(String uniqueKey, String database, String table, String row);

    // Setters
    public void setUniqueKey(String uniqueKey);
    public void setDatabase(String database);
    public void setTable(String table);
    public void setRow(String row);
    public void setTransactionEnd(boolean transactionEnd);
    public void setSourcePartition(int sourcePartition);

    // Getters (inherited from StarRocksRowData)
    public String getUniqueKey();
    public String getDatabase();
    public String getTable();
    public String getRow();
    public boolean isTransactionEnd();
    public int getSourcePartition();
}
```

### 4.3 User-Implemented Component

Users need to implement a `KeyedProcessFunction` (referred to as **TransactionAssembler** in this document) that:

1. Keys by source partition and buffers data rows within a transaction
2. Emits all rows only when the source transaction is closed (e.g., upon receiving `TXN_END`)
3. Sets `transactionEnd=true` on the last row
4. Sets `sourcePartition` on every row

No custom SinkFunction is needed — the standard connector API (`SinkFunctionFactory.createSinkFunction()`) handles everything.

## 5. Complete Example

### 5.1 StarRocks Table DDL

```sql
CREATE DATABASE `test`;

CREATE TABLE `test`.`orders` (
    `order_id` BIGINT NOT NULL,
    `customer_id` BIGINT NOT NULL,
    `total_amount` DECIMAL(10,2) DEFAULT "0",
    `order_status` VARCHAR(32) DEFAULT ""
) ENGINE=OLAP PRIMARY KEY(`order_id`)
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES("replication_num" = "1");

CREATE TABLE `test`.`order_items` (
    `item_id` BIGINT NOT NULL,
    `order_id` BIGINT NOT NULL,
    `product_name` VARCHAR(128) DEFAULT "",
    `quantity` INT DEFAULT "0",
    `price` DECIMAL(10,2) DEFAULT "0"
) ENGINE=OLAP PRIMARY KEY(`item_id`)
DISTRIBUTED BY HASH(`item_id`)
PROPERTIES("replication_num" = "1");
```

### 5.2 Flink Job Code

```java
import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class WriteMultipleTablesWithTransaction {

    // =====================================================
    // 1. Source Event Model (define per your business logic)
    // =====================================================

    enum EventType { TXN_BEGIN, DATA, TXN_END }

    static class TxnEvent implements Serializable {
        private static final long serialVersionUID = 1L;

        int partition;
        String txnId;
        EventType type;
        String database;
        String table;
        String json;

        TxnEvent() {}

        TxnEvent(int partition, String txnId, EventType type,
                 String database, String table, String json) {
            this.partition = partition;
            this.txnId = txnId;
            this.type = type;
            this.database = database;
            this.table = table;
            this.json = json;
        }

        static TxnEvent begin(int partition, String txnId) {
            return new TxnEvent(partition, txnId, EventType.TXN_BEGIN, null, null, null);
        }

        static TxnEvent data(int partition, String txnId, String db, String table, String json) {
            return new TxnEvent(partition, txnId, EventType.DATA, db, table, json);
        }

        static TxnEvent end(int partition, String txnId) {
            return new TxnEvent(partition, txnId, EventType.TXN_END, null, null, null);
        }
    }

    // =====================================================
    // 2. TransactionAssembler — Core User Component
    // =====================================================

    /**
     * Buffers DATA events per partition; on TXN_END emits the complete
     * transaction's rows as individual DefaultStarRocksRowData records.
     *
     * Only emits when a source transaction is fully closed (TXN_END received).
     * All rows are emitted synchronously within one processElement() call,
     * so they enter the downstream sink buffer without intervening checkpoint barriers.
     *
     * Multiple source transactions accumulate in the sink's buffer between
     * flush cycles — the connector handles grouping them into StarRocks transactions.
     */
    static class TransactionAssembler
            extends KeyedProcessFunction<Integer, TxnEvent, DefaultStarRocksRowData> {

        private transient ListState<TxnEvent> pendingRows;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<TxnEvent> descriptor = new ListStateDescriptor<>(
                    "pending-txn-rows", Types.POJO(TxnEvent.class));
            pendingRows = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(TxnEvent event, Context ctx,
                                   Collector<DefaultStarRocksRowData> out) throws Exception {
            switch (event.type) {
                case TXN_BEGIN:
                    pendingRows.clear();
                    break;

                case DATA:
                    pendingRows.add(event);
                    break;

                case TXN_END:
                    List<TxnEvent> rows = new ArrayList<>();
                    for (TxnEvent row : pendingRows.get()) {
                        rows.add(row);
                    }
                    int partition = ctx.getCurrentKey();
                    for (int i = 0; i < rows.size(); i++) {
                        TxnEvent row = rows.get(i);
                        DefaultStarRocksRowData rowData = new DefaultStarRocksRowData(
                                null, row.database, row.table, row.json);
                        rowData.setSourcePartition(partition);
                        // Mark the last row as transaction end
                        if (i == rows.size() - 1) {
                            rowData.setTransactionEnd(true);
                        }
                        out.collect(rowData);
                    }
                    pendingRows.clear();
                    break;
            }
        }
    }

    // =====================================================
    // 3. Main Program
    // =====================================================

    public static void main(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        String jdbcUrl  = params.get("jdbcUrl", "jdbc:mysql://127.0.0.1:9030");
        String loadUrl  = params.get("loadUrl", "127.0.0.1:8030");
        String userName = params.get("userName", "root");
        String password = params.get("password", "");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000);  // Checkpoint is for recovery only; does not affect commit cycle

        // --- Source ---
        // Replace with an actual Kafka Source that deserializes into TxnEvent
        DataStream<TxnEvent> events = env.addSource(/* KafkaSource or MockTxnEventSource */);

        // --- Step 1: Assemble complete transactions per partition ---
        // TransactionAssembler emits rows only after TXN_END.
        // Multiple closed source txns accumulate in the sink buffer between flushes.
        DataStream<DefaultStarRocksRowData> rows = events
                .keyBy(e -> e.partition)
                .process(new TransactionAssembler());

        // --- Step 2: Partition-affinity routing to sink ---
        // keyBy(sourcePartition) routes same-partition data to the same sink subtask.
        // The connector uses per-partition regions internally, so even when multiple
        // partitions land on the same sink subtask, transaction boundaries are tracked
        // independently per partition via PartitionCommitTracker.
        DataStream<DefaultStarRocksRowData> partitionedRows = rows
                .keyBy(DefaultStarRocksRowData::getSourcePartition);

        // --- Step 3: Configure the connector ---
        // sink.transaction.multi-table.enabled=true activates per-partition region
        // tracking inside StreamLoadManagerV2: each partition's regions are switched
        // independently when its txnEnd arrives, and commit triggers only when all
        // active partitions have been switched.
        StarRocksSinkOptions options = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", jdbcUrl)
                .withProperty("load-url", loadUrl)
                .withProperty("database-name", "*")               // Wildcard for dynamic multi-table routing
                .withProperty("table-name", "*")                  // Wildcard for dynamic multi-table routing
                .withProperty("username", userName)
                .withProperty("password", password)
                .withProperty("sink.version", "V2")               // Required: V2
                .withProperty("sink.semantic", "at-least-once")
                .withProperty("sink.transaction.multi-table.enabled", "true")  // Enable multi-table txn
                .withProperty("sink.buffer-flush.interval-ms", "1000")         // ~1s data freshness
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .build();

        // Optional: per-table stream load properties
        StreamLoadTableProperties orderItemsProps = StreamLoadTableProperties.builder()
                .database("test")
                .table("order_items")
                .addProperty("format", "json")
                .addProperty("strip_outer_array", "true")
                .addProperty("ignore_json_size", "true")
                .build();
        options.addTableProperties(orderItemsProps);

        // --- Step 4: Create and attach the sink ---
        // Standard connector API — no custom SinkFunction needed.
        // addSink on the keyBy'd stream ensures partition affinity.
        SinkFunction<DefaultStarRocksRowData> sink = SinkFunctionFactory.createSinkFunction(options);
        partitionedRows.addSink(sink);

        env.execute("WriteMultipleTablesWithTransaction");
    }
}
```

### 5.3 Data Flow Topology

```
Kafka (60 partitions)
  |
  v
keyBy(partition) ———— Ensures same-partition events go to the same subtask
  |
  v
TransactionAssembler (KeyedProcessFunction)
  |  Buffers DATA events
  |  On TXN_END: emits all rows (last row has transactionEnd=true)
  |  Every row carries sourcePartition
  |
  v
keyBy(sourcePartition) ———— Ensures same-partition rows go to the same sink subtask
  |
  v
StarRocksDynamicSinkFunctionV2 (via SinkFunctionFactory.createSinkFunction)
  |
  |  +——————————— StreamLoadManagerV2 (multi-table txn mode) ———————————+
  |  |                                                                   |
  |  |  Per-partition, per-table regions:                                 |
  |  |    Region(P0, orders), Region(P0, order_items)                    |
  |  |    Region(P2, orders), Region(P2, order_items)                    |
  |  |                                                                   |
  |  |  Each region tracks:                                              |
  |  |    - activeChunk / inactiveChunks                                 |
  |  |    - lastSwitchTimeMs      (for miniInterval batching)            |
  |  |    - activeChunkCleanBoundary (true iff last task event is txnEnd)|
  |  |                                                                   |
  |  |  write(partition, db, table, row)  [task thread]                  |
  |  |    -> routes to Region(partition, db, table)                      |
  |  |    -> write0 sets activeChunkCleanBoundary = false                |
  |  |    -> In multi-table mode write0 does NOT switchChunk, so         |
  |  |       activeChunk only freezes at a txnEnd boundary               |
  |  |                                                                   |
  |  |  setCommitAllowed(partition, txnEnd=true)  [task thread]          |
  |  |    -> region.tryMiniIntervalSwitch():                             |
  |  |         sets activeChunkCleanBoundary = true                      |
  |  |         if (now - lastSwitchTimeMs >= miniInterval                |
  |  |             && activeChunk has data): switchChunkForCommit        |
  |  |         else: data batches into activeChunk with subsequent       |
  |  |               completed source transactions (N:1 mapping)         |
  |  |    -> PartitionCommitTracker.onTxnEnd(partition)                  |
  |  |                                                                   |
  |  |  SharedTransactionCoordinator:                                    |
  |  |    -> eagerly opens shared txn before any flush                   |
  |  |    -> all autonomous flushes use the shared label                 |
  |  |    -> recycles idle txn at 80% of server timeout                  |
  |  |                                                                   |
  |  |  Manager thread (every scanningFrequency):                        |
  |  |    -> tryForceCleanSwitch per region:                             |
  |  |         if cleanBoundary && has data && miniInterval elapsed      |
  |  |             -> switchChunkForCommit (source-idle fallback)        |
  |  |    -> tryStartTimerDrivenCommit:                                  |
  |  |         if commitInterval elapsed && hasDataLoaded                |
  |  |             -> set commitInFlight = true                          |
  |  |    -> autonomous flush: drain inactiveChunks via streamLoad       |
  |  |       (never touches activeChunk in multi-table mode)             |
  |  |                                                                   |
  |  |  Manager thread (commitInFlight=true):                            |
  |  |    -> triggerLoadIfNeeded per region (HTTP /api/transaction/load) |
  |  |    -> wait all loads complete                                     |
  |  |    -> unified commit via SharedTransactionCoordinator             |
  |  |    -> reset tracker; open next shared txn                         |
  |  +———————————————————————————————————————————————————————————————————+
  |
  v
StarRocks (test.orders + test.order_items)
```

## 6. How It Works

### 6.1 Chunk Lifecycle and miniInterval Batching

Each `(partition, table)` region has one `activeChunk` (currently accepting
writes) and a FIFO of `inactiveChunks` (frozen data, pending HTTP load). In
multi-table transaction mode, the **only** way data moves from `activeChunk`
into `inactiveChunks` is via `switchChunkForCommit`, which is called from
exactly three sites:

1. **Task thread, on txnEnd** — via `region.tryMiniIntervalSwitch()` inside
   `setCommitAllowed(partition, true)`. This is the common path.
2. **Manager thread, source-idle fallback** — via `region.tryForceCleanSwitch()`
   on every scan cycle, for regions whose `activeChunk` has been sitting idle
   at a clean transaction boundary.
3. **Manager thread, savepoint/recycle** — force-switch all regions after
   verifying every region is at a clean boundary.

To avoid one HTTP load per source transaction in high-throughput CDC, the task
thread only performs a switch when at least `miniSwitchIntervalMs` has elapsed
since the previous switch on the same region. `miniSwitchIntervalMs` is
computed as `min(1000 ms, max(100 ms, commitInterval / 10))`, so a 1-second
commit interval batches at 100 ms while a 30-second interval caps batching at
1 second. Within a miniInterval window, multiple completed source transactions
accumulate into the same `activeChunk` (N:1 mapping) and are frozen together
on the next switch.

Each region carries two fields that drive these decisions:

- `lastSwitchTimeMs` — epoch ms of the most recent switch. Initially 0, so the
  very first txnEnd after region creation always triggers a switch.
- `activeChunkCleanBoundary` — `true` iff the most recent task-thread event on
  this region was either an `onTxnEnd` or a `switchChunk`. `false` after any
  `write()`. The manager thread's `tryForceCleanSwitch` only runs when this
  flag is `true`, so it can never freeze partial source-transaction data.

### 6.2 Task Thread — Write and txnEnd

```
invoke(record)                                           [Flink task thread]
  |
  |  if record is a data row:
  |      write(partition, db, table, row)
  |          region.write(row) → addRow(); cleanBoundary = false
  |
  |  if record carries transactionEnd=true:
  |      setCommitAllowed(partition, true)
  |          for each region owned by this partition:
  |              region.tryMiniIntervalSwitch():
  |                  cleanBoundary = true       // always (txnEnd observed)
  |                  if (now - lastSwitchTimeMs >= miniInterval
  |                      && activeChunk has data):
  |                      switchChunkForCommit()  // freezes activeChunk
  |          partitionTracker.onTxnEnd(partition)  // safety bookkeeping only
```

The task thread is the sole serializer of `write()` and `setCommitAllowed()`
events, which is why both the `cleanBoundary` mark and the conditional switch
must run here (not deferred to the manager thread): `cleanBoundary` must
reflect the most recent task-thread event at all times, or the manager's
idle-fallback could race a write and freeze partial data.

### 6.3 Manager Thread — Time-Driven Commit

The manager thread runs a scan loop at `scanningFrequency`. Each iteration:

1. **Ensure shared transaction**: open a new one (eager) or proactively
   recycle the current one if it is approaching the StarRocks server-side
   timeout (80% of `timeout` header, default 480 s).
2. **Source-idle fallback**: call `region.tryForceCleanSwitch()` on every
   region to freeze any `activeChunk` that is clean and has been idle for at
   least `miniInterval`. This handles the "source paused after a few txnEnds"
   case where the task thread stopped before issuing a fresh switch.
3. **Time-driven commit trigger**: call `tryStartTimerDrivenCommit()`, which
   sets `commitInFlight=true` if **both** conditions hold:
   - `now - lastCommitTimeMs >= commitInterval` (the configured
     `sink.buffer-flush.interval-ms`).
   - There is data to commit — either `txnCoordinator.hasDataLoaded()` is
     true or at least one region still has pending inactiveChunks.
4. **Autonomous flush**: drain any region whose `inactiveChunks` is
   non-empty via the `FlushAndCommitStrategy`. Multi-table mode's `flush()`
   only streams out already-frozen inactive chunks — it **never** touches
   `activeChunk`. This preserves the invariant that every chunk reaching
   StarRocks under the shared label comes from completed source transactions.

When `commitInFlight=true` the main loop enters `processMultiTableCommit`,
which waits for in-flight loads, triggers loads for any remaining inactive
chunks, runs the unified commit via `SharedTransactionCoordinator`, updates
`lastCommitTimeMs`, resets the tracker, and opens a new shared transaction
for the next cycle.

### 6.4 Shared Transaction Coordination

All tables within the same commit cycle share a single StarRocks transaction managed by `SharedTransactionCoordinator`:

1. **Eager transaction opening**: A shared transaction is opened eagerly before any autonomous flush, so all HTTP loads use the shared label. This eliminates the data-loss window where an independent-label flush could be orphaned when a shared transaction later overwrites the label.
2. **Unified commit**: After all regions' data is loaded, a single `commit` is executed for the shared label. Multi-table transactions skip the `prepare` step because StarRocks does not support `TXN_PREPARE` in multi-table mode.
3. **Idle transaction recycling**: If the shared transaction remains open longer than 80% of the StarRocks server-side timeout (default: 480s for a 600s timeout), it is proactively recycled (commit-or-rollback + reopen) to prevent server-side timeout errors. Recycle fails fast if any region has in-progress transaction data (clean-boundary violation) or any partition has written data without ever receiving a txnEnd.

### 6.5 PartitionCommitTracker (Safety Bookkeeping)

In the current design, commit timing is driven entirely by the commit
interval and the per-region clean-boundary flags. `PartitionCommitTracker`
is reduced to an informational/safety aid:

- `onWrite(partition)` registers a partition as `ACTIVE` on first write.
- `onTxnEnd(partition)` transitions the partition to `TXN_END_SEEN`
  (sticky — subsequent writes do **not** demote it back to `ACTIVE`).
- `getPartitionsWithoutTxnEnd()` lists partitions that have written data
  but never received a txnEnd. Used by savepoint and recycle to fail fast
  on upstream contract violations (a source transaction that never closed).
- `reset()` clears all partitions at the end of a commit cycle.

The tracker no longer drives switch/commit decisions, does not track a
`SWITCHED` state, and does not manage pending txnEnd signals. Partitions
that stop producing data after a commit are simply cleared by `reset()`;
if they resume later, the next `onWrite` re-registers them.

### 6.6 Autonomous Flush with Shared Labels

When a region's `inactiveChunks` becomes non-empty, the manager thread's
autonomous-flush loop streams it to StarRocks via
`/api/transaction/load` under the **current shared label**. Because the
shared transaction is always opened before any data is loaded, there is no
window where data could be loaded under an independent (orphaned) label.
In multi-table mode, `flush()` never triggers a `switchChunk` — it only
drains already-frozen inactive chunks — so the invariant "every chunk
reaching StarRocks under the shared label comes from completed source
transactions" holds unconditionally.

### 6.7 Safety Guarantees

| Guarantee | Mechanism |
|---|---|
| switchChunk does not split source transactions | `switchChunkForCommit` is only called at a clean transaction boundary (on txnEnd or when `activeChunkCleanBoundary=true`) |
| Commit never includes partial source transactions | Every chunk reaching StarRocks originates from `switchChunkForCommit`; autonomous flush never switches `activeChunk` in multi-table mode |
| Per-partition isolation | Each `(partition, table)` has its own region; one partition's switch never affects another's data |
| Within-partition ordering | `keyBy(sourcePartition)` routes same-partition rows to the same sink subtask |
| Task thread is non-blocking | `tryMiniIntervalSwitch` is O(regions-in-partition); HTTP work happens asynchronously on the manager thread |
| Source-idle data visibility | Manager thread's `tryForceCleanSwitch` freezes clean `activeChunk`s after `miniInterval` of idleness, so data remains visible within `commitInterval + miniInterval` even if the source pauses |
| Autonomous flushes are transaction-safe | Every load uses the shared label; every frozen chunk is from completed source transactions |
| Idle transactions don't timeout | Shared transactions are recycled at 80% of server timeout; recycle fails fast on in-progress data |
| Per-partition independent commit | A partition with a completed source transaction commits on the next commit interval, independent of other partitions' in-progress transactions |

### 6.8 N:1 Transaction Mapping

Multiple source transactions can accumulate in a single StarRocks transaction
via the miniInterval batching mechanism:

```
Source txn K1 (3 rows) -> write -> activeChunk -> txnEnd (1st switch)
Source txn K2 (2 rows) -> write -> activeChunk -> txnEnd (inside miniInterval: no switch)
Source txn K3 (4 rows) -> write -> activeChunk -> txnEnd (inside miniInterval: no switch)
                                                  (miniInterval elapsed → next switch batches K2+K3)
                                                  -> ...
commitInterval elapsed
                                                  -> commit(label=A)
                                                  -> K1 + K2 + K3 atomically committed to StarRocks
```

Because the commit decision is time-driven (not tied to a specific txnEnd),
the connector amortizes HTTP-load and begin/commit overhead across many
small source transactions without any configuration changes. For a CDC
source emitting 100 txnEnds per second with `commitInterval=1 s`,
`miniInterval=100 ms`, the connector issues at most ~10 load calls per
second instead of ~100.

## 7. Limitations

1. **Requires `sink.version=V2`**: V1 does not support transaction stream load.

2. **At-least-once only**: Failed retries may produce duplicate writes. Multi-table mode guarantees all tables within the same batch succeed or fail together, but does not provide global exactly-once. For PRIMARY KEY tables, duplicate writes are idempotent (upsert).

3. **All tables must be in the same database**: StarRocks multi-table transactions are database-scoped; cross-database transactions are not supported.

4. **Transaction scope is per sink subtask + per partition**: Each sink subtask maintains its own StarRocks transaction independently. Atomicity is guaranteed **within a single source transaction** (all rows for one txnEnd on one partition, across all tables that partition writes to). Data visibility across **different** source partitions can interleave: once partition P0's source transaction has fully arrived and the commit interval has elapsed, P0's data is committed even if partition P1's source transaction is still in progress. Applications that require cross-partition atomicity at the StarRocks level must either use a single source partition or coordinate commits upstream.

5. **Data visibility latency**: Governed by `sink.buffer-flush.interval-ms` and the internal `miniInterval = min(1000, max(100, commitInterval/10))`. In a continuously flowing CDC stream, an individual row becomes visible in StarRocks within roughly `commitInterval + miniInterval` of its source commit. During a source pause, previously-committed data remains visible while any row between the last switch and the pause becomes visible after one more `miniInterval` (handled by the manager-thread clean-boundary fallback).

6. **Depends on StarRocks cluster transaction settings**: Monitor running txn limits, prepared timeout (default 600s), and label retention. Ensure `sink.buffer-flush.interval-ms` is significantly shorter than the StarRocks transaction timeout.

7. **activeChunk memory growth under long source transactions**: Because multi-table mode disables chunk-size-triggered internal switching (to preserve the clean-transaction-boundary invariant), `activeChunk` can grow until the next txnEnd arrives. Memory is bounded by `sink.transaction.multi-table.buffer-size` (soft) and `2 × buffer-size` (hard via `blockIfCacheFull`). Exceptionally large source transactions will throttle the task thread via back-pressure; if this becomes routine, either split the source transactions upstream or increase `sink.transaction.multi-table.buffer-size`.

8. **Cross-database writes are rejected**: Multi-table transactions validate that all regions belong to the same database. Writing to tables in different databases within the same commit cycle will throw an error.

## 8. Monitoring and Troubleshooting

Recommended metrics:
- **Flink**: Checkpoint success rate, checkpoint duration, sink flush/commit latency
- **StarRocks**: Running/prepared txn count, txn timeout occurrences, label conflicts

Common issues:

| Error | Cause | Solution |
|---|---|---|
| `transaction not existed` | StarRocks transaction timeout | The connector automatically recycles idle transactions at 80% of server timeout. If this still occurs, check if prepared timeout is too short or flush interval is too large |
| `too many running txns` | Too many concurrent transactions | Reduce sink parallelism or increase StarRocks `max_running_txn_num_per_db` |
| `Transaction start failed` | beginTransaction HTTP call failed | Verify load-url connectivity and StarRocks version (requires >= 4.0) |
| High data visibility latency | Commit conditions not met | Verify upstream data has correct `transactionEnd=true` markers; expect up to `commitInterval + miniInterval` latency per row. If latency exceeds this budget, check the manager thread is not stuck in a recycle or in-flight load (see `StarRocks-Sink-Manager` logs) |
| Cross-database write error | Tables in different databases in same commit cycle | Ensure all tables written in the same job belong to the same StarRocks database |

## 9. Best Practices

1. **TransactionAssembler contract**:
   - Emit rows only after the source transaction is fully closed
   - The last row must have `setTransactionEnd(true)`
   - Every row must have `setSourcePartition(partition)`
   - All rows must be emitted synchronously within a single `processElement()` call

2. **keyBy before sink is mandatory**: `rows.keyBy(DefaultStarRocksRowData::getSourcePartition).addSink(sink)` — omitting this breaks within-partition transaction ordering.

3. **Checkpoint is decoupled from commit**: Checkpoint interval can be set to a large value (e.g., 60s) for fault recovery; data visibility is governed by `sink.buffer-flush.interval-ms` (e.g., 1000ms).

4. **Keep routing strategies stable**: Avoid single transactions writing to an excessive number of distinct tables, which increases transaction duration and failure probability.

5. **Fault injection testing before production**: Kill TaskManagers / introduce network jitter and verify data correctness after checkpoint recovery.
