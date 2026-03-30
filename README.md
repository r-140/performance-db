# Performance DB — Index, SQL & Java 21 Showcase

An educational database built from scratch in Java 21. Demonstrates index
internals, SQL query planning, transaction isolation, Write-Ahead Logging,
buffer pool management, and idiomatic Java 21 features.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Project Structure](#project-structure)
3. [Java 21 Features Used](#java-21-features-used)
4. [Index Implementations](#index-implementations)
5. [SQL Support](#sql-support)
6. [Transaction Isolation](#transaction-isolation)
7. [Write-Ahead Log](#write-ahead-log)
8. [Buffer Pool](#buffer-pool)
9. [Architecture Overview](#architecture-overview)
10. [Running the Tests](#running-the-tests)
11. [Wire Protocol](#wire-protocol)
12. [Bugs Fixed from Original](#bugs-fixed-from-original)

---

## Quick Start

```bash
# Build
mvn clean install -DskipTests

# Start server
cd db-server/server-instance
mvn exec:java -Dexec.mainClass="com.iu.dbserver.PooledDbServer"

# Run unit tests (no server needed)
cd db-server/indexes
mvn test

# Run SQL query via client
SqlHelper.query("SELECT * FROM data WHERE id = 42");
SqlHelper.query("SELECT * FROM data WHERE id = 42 LIMIT 1");
SqlHelper.query("SELECT * FROM data");

# Run Cucumber integration tests (server must be running)
cd db-performance-test
mvn test
```

---

## Project Structure

```
performance-db/
├── common/                      MessageBean record (Java 16)
├── db-client/                   Client helpers
│   └── sql/SqlHelper.java       SQL client — sends queries to server
├── db-server/
│   ├── db-server-common/        FileHelper, JsonHelper (iterative, resource-safe)
│   ├── indexes/                 All index data structures + unit tests
│   │   └── src/main/java/com/iu/
│   │       ├── indexes/
│   │       │   ├── btreebased/  BTreeIndex, BPlusTreeIndex (corrected)
│   │       │   ├── lsmtree/     LSMTreeIndex, MemoryTable, SSTable (corrected)
│   │       │   ├── gin/         GINIndex — inverted index
│   │       │   ├── bitmap/      BitmapIndex — bitset per value
│   │       │   ├── skiplist/    SkipListIndex — probabilistic
│   │       │   ├── bloom/       BloomFilter — for LSM SSTable filtering
│   │       │   └── transaction/ MVCCStore — isolation level demo
│   │       ├── wal/             WriteAheadLog — crash recovery
│   │       └── buffer/          BufferPool — LRU page cache
│   ├── tasks/                   Task workers + SQL engine
│   │   └── sql/                 SqlParser, QueryPlanner, QueryExecutor
│   ├── server-service/          VirtualThreadDbServer (Java 21)
│   └── server-instance/         Main entry point
└── db-performance-test/         Cucumber BDD tests
    └── features/
        ├── index/               All 6 index types
        ├── sql/                 SQL SELECT scenarios
        ├── data/                CRUD operations
        └── performance/         Timing comparison
```

---

## Java 21 Features Used

### Records (JEP 395, Java 16)

`MessageBean` was a hand-written POJO with 40 lines. Now:

```java
public record MessageBean(String taskType, String payload) implements Serializable {
    public MessageBean {
        if (taskType == null || taskType.isBlank())
            throw new IllegalArgumentException("taskType must not be blank");
    }
}
```

Records automatically generate: constructor, accessors (`taskType()`, `payload()`),
`equals`, `hashCode`, `toString`. They are immutable by default.

Also used for: `QueryPlan.HashIndexScan`, `QueryPlan.FullScan`, `DbError.IndexExists`,
`BufferPool.Stats`, `WalRecord.Insert`, `WalRecord.Commit`, etc.

### Sealed Classes + Interfaces (JEP 409, Java 17)

Sealed types restrict which classes can implement an interface. Combined with
records, they create exhaustive discriminated unions:

```java
// The compiler enforces ALL subtypes are handled in every switch
public sealed interface WalRecord
        permits WalRecord.Insert, WalRecord.Delete, WalRecord.Commit, WalRecord.Rollback {
    long lsn();
    long txId();

    record Insert(long lsn, long txId, int docId, String payload) implements WalRecord {}
    record Delete(long lsn, long txId, int docId, String originalLine) implements WalRecord {}
    record Commit(long lsn, long txId)  implements WalRecord {}
    record Rollback(long lsn, long txId) implements WalRecord {}
}
```

Used for: `WalRecord`, `DbError`, `TaskResult`, `QueryPlan`, `SqlNode`.

### Pattern-Matching Switch (JEP 441, Java 21)

Switch expressions over sealed hierarchies are exhaustive — no `default` needed,
and adding a new subtype is a compile error until all switches handle it:

```java
// WAL recovery — exhaustive, compiler-verified
String desc = switch (rec) {
    case WalRecord.Insert   r -> "insert  id=" + r.docId();
    case WalRecord.Delete   r -> "delete  id=" + r.docId();
    case WalRecord.Commit   r -> "commit  tx=" + r.txId();
    case WalRecord.Rollback r -> "rollback tx=" + r.txId();
};

// Query execution — exhaustive over sealed QueryPlan
List<String> rows = switch (plan) {
    case QueryPlan.HashIndexScan s  -> executeHashLookup(s.id());
    case QueryPlan.BPlusTreeScan s  -> executeBPlusLookup(s.id());
    case QueryPlan.GINScan s        -> executeGINSearch(s.token());
    case QueryPlan.BitmapScan s     -> executeBitmapSearch(s.value());
    case QueryPlan.FullScan s       -> executeFullScan(s.field(), s.value());
};
```

### Virtual Threads (JEP 444, Java 21)

The server replaced `ThreadPoolExecutor` with virtual threads:

```java
// Old: ThreadPoolExecutor with pool sizing, queue limits, rejection handlers
ThreadPoolExecutor pool = new ThreadPoolExecutor(core, max, keepAlive, ...);

// New: one virtual thread per connection — no sizing, no queue, no blocking
try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
    while (running) {
        Socket connection = server.accept();
        executor.submit(() -> handleConnection(connection));
    }
}
```

Virtual threads are cheap enough (nanoseconds to create, ~1KB stack) that
one per socket connection is practical. A platform thread pool needs tuning;
virtual threads do not.

### Structured Concurrency (JEP 453, Java 21)

Startup recovery previously used a busy-spin `while(!future.isDone())`.
Now it uses `StructuredTaskScope`:

```java
try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
    scope.fork(() -> { new HashIndexRecoverTask().call(); return null; });
    scope.fork(() -> { new SequenceRecoverTask().call();  return null; });
    scope.join();           // wait for both
    scope.throwIfFailed();  // re-throw if either crashed
}
```

If either task fails, the scope cancels the other and re-throws. Both tasks
run concurrently on virtual threads.

### Scoped Values (JEP 446, Java 21)

`ScopedValue` passes request context through a virtual thread's call tree
without `ThreadLocal`'s inheritance/cleanup problems:

```java
// Defined once:
public static final ScopedValue<Socket> CURRENT_CONNECTION = ScopedValue.newInstance();

// Bound per request (in VirtualThreadDbServer):
ScopedValue.where(CURRENT_CONNECTION, connection)
           .run(() -> task.call());
```

### Switch Expressions (JEP 361, Java 14)

`TaskType.getTaskByType()` replaced a for loop over `values()` with a direct
switch returning the enum constant:

```java
public static TaskType getTaskByType(String taskType) {
    return switch (taskType) {
        case "append"      -> APPEND;
        case "find"        -> FIND;
        case "createIndex" -> CREATE_INDEX;
        // ...
        default            -> null;
    };
}
```

### Text Blocks (JEP 378, Java 15)

Used in `SqlParser` to write the regex pattern readably:

```java
private static final Pattern SELECT_PATTERN = Pattern.compile(
    """
    (?i)SELECT\\s+\\*\\s+FROM\\s+(\\w+)
    (?:\\s+WHERE\\s+(\\w+)\\s*=\\s*'?([^'\\s]+)'?)?
    (?:\\s+LIMIT\\s+(\\d+))?
    \\s*;?\\s*
    """.trim().replace("\\n", ""),
    Pattern.CASE_INSENSITIVE
);
```

### Stream.toList() and var (Java 16 / Java 10)

```java
// var — local type inference
var allOffsets = FileHelper.readFile(dataFile, false);
var allRows    = new ArrayList<String>();

// Stream.toList() — unmodifiable, no Collectors.toList() boilerplate
List<String> result = rows.stream().filter(...).toList();
```

---

## Index Implementations

### Hash Index
**Structure:** `ConcurrentHashMap<Integer, Long>` — in memory, persisted via snapshot.
**Lookup:** O(1). **Cannot do range queries.**

### B-Tree Index
Disk-based. Every node (leaf and internal) stores both keys and values.
Internal nodes have lower fanout than B+ Tree because they waste space on values.
Included to demonstrate why B+ Tree is preferred.

**Fixed bugs from original:**
- Values stored as raw 8-byte longs (not 256-byte ASCII strings).
- `nodeCounter` recovered from file size on reload (no root corruption).
- Null slots use sentinel `-1L` (no `Long.valueOf("null")` crash).

### B+ Tree Index
Disk-based. **Internal nodes store keys only.** Leaf nodes store values.
Leaf nodes form a linked list enabling O(log N + k) range scans.

**Fixed bugs from original:**
- Internal nodes now have no `values[]` array — correct B+ Tree structure.
- Leaf splits duplicate the median key into the right child (not move it).
- `rangeScan(lo, hi)` implemented using the leaf linked list.
- Deletion handles leaf/internal nodes differently as required by the spec.
- `nodeCounter` recovery — no position-0 overwrite on reload.

### LSM Tree Index
Write path: MemTable → SSTable flush. Read path: MemTable first, then SSTables newest-first.

**Fixed bugs from original:**
- Tombstone uses `Long.MIN_VALUE` sentinel, not `null` — deletions no longer
  fall through to stale SSTable values.
- SSTables scanned newest-first (oldest-first returned stale values).
- Compaction drops tombstoned keys correctly.
- Merged SSTables created in the configured data directory.

### GIN Index (Generalized Inverted Index)
Tokenises document values → `TreeMap<String, List<Long>>` posting lists.
Supports AND/OR token searches. Used by PostgreSQL full-text search.

### Bitmap Index
One `BitSet` per distinct value. AND/OR across predicates = native 64-bit
bitwise ops. Ideal for low-cardinality columns (status, category).

### Skip List Index (new)
Probabilistic layered linked lists. O(log N) expected search/insert/delete
without tree rotations. Used by Redis Sorted Sets, LevelDB memtable.
Supports range scan via level-0 linked list — same O(log N + k) as B+ Tree.

```
Level 2:  1 ---------> 50 ------------> null
Level 1:  1 ---> 20 -> 50 -> 70 ------> null
Level 0:  1 -> 10 -> 20 -> 30 -> 50 -> 70 -> null

search(30): L2: 1→50 too big, drop; L1: 1→20→50 too big, drop; L0: 20→30 FOUND
```

### Bloom Filter (new — for LSM SSTables)
Answers "definitely not here" or "probably here" in O(k) bit operations.
Eliminates unnecessary SSTable reads for cache misses:

```
Without Bloom filter: get(key) → read ALL sstable files → O(SSTables × log N)
With Bloom filter:    get(key) → check filter (O(k)) → read 0 or 1 files
```

Uses double-hashing (Kirsch & Mitzenmacher) to simulate k hash functions
from two base hashes. Configurable false positive rate (default 1%).

---

## SQL Support

The server understands a subset of SQL:

```sql
SELECT * FROM data
SELECT * FROM data WHERE id = 42
SELECT * FROM data WHERE data = 'testdata5'
SELECT * FROM data WHERE id = 42 LIMIT 1
```

### Query Pipeline

```
SQL string
    │
    ▼
SqlParser          → SqlNode.SelectStatement (sealed record AST)
    │
    ▼
QueryPlanner       → QueryPlan (sealed: HashIndexScan | BPlusTreeScan |
    │                           GINScan | BitmapScan | FullScan)
    ▼
QueryExecutor      → List<String> (matching document lines)
    │
    ▼
SqlQueryTask       → JSON array response to client
```

### Automatic Index Selection

The planner checks which indexes currently exist and picks the cheapest:

| Predicate          | Index present    | Plan chosen      | Cost      |
|--------------------|------------------|------------------|-----------|
| `WHERE id = N`     | hash index       | HashIndexScan    | O(1)      |
| `WHERE id = N`     | B+ tree          | BPlusTreeScan    | O(log N)  |
| `WHERE field = V`  | GIN index        | GINScan          | O(log T)  |
| `WHERE field = V`  | Bitmap index     | BitmapScan       | O(1) bits |
| anything           | none             | FullScan         | O(N)      |

```java
// Client usage
String json = SqlHelper.query("SELECT * FROM data WHERE id = 42");
String all  = SqlHelper.query("SELECT * FROM data LIMIT 100");
```

---

## Transaction Isolation

`MVCCStore` in `db-server/indexes/.../transaction/` demonstrates all four
SQL isolation levels with concrete, runnable test cases.

### Isolation levels

| Level              | Dirty Read | Non-Repeatable Read | Phantom Read |
|--------------------|:----------:|:-------------------:|:------------:|
| READ_UNCOMMITTED   | possible   | possible            | possible     |
| READ_COMMITTED     | prevented  | possible            | possible     |
| REPEATABLE_READ    | prevented  | prevented           | prevented*   |
| SERIALIZABLE       | prevented  | prevented           | prevented    |

*Our MVCC snapshot also prevents phantoms at REPEATABLE_READ.

### Visibility rule (`canSee()`)

```java
public boolean canSee(long writerTxId, TransactionStatus writerStatus) {
    return switch (isolationLevel) {
        case READ_UNCOMMITTED -> true;
        case READ_COMMITTED   -> writerStatus == COMMITTED;
        case REPEATABLE_READ,
             SERIALIZABLE     -> writerTxId <= snapshotTxId
                                 && writerStatus == COMMITTED;
    };
}
```

`snapshotTxId` is captured once at `BEGIN TRANSACTION`. Any row inserted
after this point (writerTxId > snapshotTxId) is invisible — this is how
phantom reads are prevented without predicate locks.

### Phantom read demo

```java
// tx A begins REPEATABLE_READ — snapshot fixed here
TransactionContext txA = store.beginTransaction(REPEATABLE_READ);
List<VersionedRecord> first = store.rangeScan(txA, 10, 20);
// first.size() == 2

// tx B inserts doc 17 and commits AFTER txA started
TransactionContext txB = store.beginTransaction(READ_COMMITTED);
store.insert(txB, 17, "doc17");
store.commit(txB);

// txA rescans — snapshot prevents seeing txB's insert
List<VersionedRecord> second = store.rangeScan(txA, 10, 20);
// second.size() == 2  (no phantom)
```

Run: `cd db-server/indexes && mvn test -Dtest=TransactionIsolationTest`

---

## Write-Ahead Log

`WriteAheadLog` in `db-server/indexes/src/main/java/com/iu/wal/`.

### How WAL works

Before any write touches the data file, the operation is appended to an
append-only log and flushed to disk (fsync). On crash, only operations
belonging to COMMITTED transactions are re-applied.

```
Normal write:
  1. wal.appendInsert(txId, docId, payload)   ← write to WAL, fsync
  2. FileHelper.writeToFile(...)              ← apply to data file
  3. wal.appendCommit(txId)                   ← mark transaction done

Crash recovery (wal.replay()):
  - Insert + Commit found  → re-apply if row absent
  - Insert, no Commit      → skip (transaction never committed)
  - Rollback found         → skip
```

### WAL record types (sealed)

```java
public sealed interface WalRecord extends Serializable
        permits WalRecord.Insert, WalRecord.Delete,
                WalRecord.Commit, WalRecord.Rollback {
    long lsn();   // Log Sequence Number — unique, monotonically increasing
    long txId();  // transaction id
}
```

### Why WAL enables durability without full fsync per write

Without WAL: every write must fsync the data file → very slow.
With WAL: writes go to the append-only log (fast sequential I/O).
The data file is flushed lazily (checkpoint). If a crash happens between
the log write and the data file write, recovery re-applies the log.

Run: `cd db-server/indexes && mvn test -Dtest=WriteAheadLogTest`

---

## Buffer Pool

`BufferPool` in `db-server/indexes/src/main/java/com/iu/buffer/`.

### How the buffer pool works

A database never reads individual bytes. It reads fixed-size **pages** (4 KB).
The buffer pool is a fixed-size cache of pages in memory with LRU eviction.

```
fetchPage(pageId):
  hit:  return from pool                        → 0 disk I/O
  miss: load from disk, store in pool, return   → 1 disk read
        if pool full: evict LRU page first
          (write to disk if dirty → 1 disk write)
```

### LRU implementation

Uses `LinkedHashMap` with `accessOrder=true`. Java's `LinkedHashMap` maintains
insertion order by default; with `accessOrder=true` it re-orders on every `get()`,
making the eldest (least-recently-used) entry easy to identify and evict.

```java
new LinkedHashMap<>(capacity, 0.75f, true) {   // accessOrder=true
    @Override
    protected boolean removeEldestEntry(Map.Entry<Integer, Page> eldest) {
        if (size() > capacity) { evict(eldest.getValue()); return true; }
        return false;
    }
};
```

### Buffer pool + WAL interaction

The WAL must be flushed before a dirty page is evicted (write-ahead rule):

```
dirty page eviction:
  1. Ensure WAL records up to page's LSN are flushed
  2. Write dirty page to disk
  3. Remove from pool
```

This ensures the log always reflects what's on disk, so recovery is correct.

Run: `cd db-server/indexes && mvn test -Dtest=BufferPoolTest`

---

## Architecture Overview

```
Client
  |  TCP socket (port 5555)
  |  MessageBean record { taskType, payload }
  v
VirtualThreadDbServer
  |  newVirtualThreadPerTaskExecutor() — one virtual thread per connection
  |  StructuredTaskScope for startup recovery
  v
TaskType (switch expression dispatch)
  v
Task (Append | Find | SQL | CreateIndex | DeleteIndex | Delete | DeleteDB)
  |
  +-- SqlQueryTask
  |     +-- SqlParser      (text block regex, ParseException)
  |     +-- QueryPlanner   (sealed QueryPlan, index selection)
  |     +-- QueryExecutor  (pattern-matching switch over QueryPlan)
  |
  +-- FileHelper           (iterative, resource-safe, correct file modes)
  +-- WriteAheadLog        (sealed WalRecord, append-before-write)
  +-- BufferPool           (LRU page cache, dirty tracking, flush)
  +-- IndexKeeper          (all live index instances)
  +-- IndexTypes           (dispatch to service impls)
```

### Data file format

```
<id>,<JSON payload>

0,{"data":"testdata0","id":0}
1,{"data":"testdata1","id":1}
42,{"data":"testdata42","id":42}
```

Every index stores the **file offset** of its document's line.
Retrieval: one `RandomAccessFile.seek()` = O(1).

---

## Running the Tests

### Unit tests — no server required

```bash
# All index structure tests
cd db-server/indexes && mvn test

# Runs:
#   BPlusTreeIndexTest  — structural correctness, range scan, persistence
#   BTreeIndexTest      — (via btreebased package)
#   LSMTreeIndexTest    — deletion, tombstones, compaction, newest-wins
#   GINIndexTest        — tokenisation, AND/OR search, persistence
#   BitmapIndexTest     — AND/OR predicates, cardinality, persistence
#   SkipListIndexTest   — insert, search, range scan, deletion
#   BloomFilterTest     — no false negatives, FPR within bound
#   TransactionIsolationTest — dirty read, phantom read, non-repeatable read
#   WriteAheadLogTest   — commit/rollback, recovery, LSN ordering
#   BufferPoolTest      — cache hit/miss, LRU, dirty tracking, persistence

# SQL parser tests
cd db-server/tasks && mvn test
# Runs: SqlParserTest
```

### Cucumber integration tests — server required

```bash
# Start the server first
cd db-server/server-instance
mvn exec:java -Dexec.mainClass="com.iu.dbserver.PooledDbServer"

# Run tests
cd db-performance-test && mvn test
```

HTML report: `db-performance-test/target/cucumber-reports.html`

---

## Wire Protocol

**Request (client → server):**
```
ObjectOutputStream → MessageBean record { taskType, payload }
```

**Response (server → client):**
```
ObjectInputStream ← String (result or JSON error body)
```

**Task types:**

| taskType    | payload example                                | Returns              |
|-------------|------------------------------------------------|----------------------|
| append      | `{"data":"x","id":1}`                          | Stored line          |
| find        | `{"id":42,"indexType":"bplustree"}`            | Document or null     |
| sql         | `SELECT * FROM data WHERE id = 42`             | JSON array           |
| createIndex | `hashIndex` / `btree` / `bplustree` / `lsmtree` / `gin` / `bitmap` | Success or error |
| deleteIndex | same strings                                   | Success or error     |
| delete      | `{"id":42}`                                    | Deleted line or error|
| deleteDb    | `deleteDb`                                     | Confirmation string  |

**Error response:**
```json
{"code":"DB-402","message":"Index 'bplustree' already exists"}
```

| Code   | Meaning                    |
|--------|----------------------------|
| DB-401 | IOException                |
| DB-402 | Index already exists       |
| DB-403 | Index does not exist       |
| DB-404 | Unexpected index type      |
| DB-405 | Document not found         |
| SQL-001| SQL parse error            |

---

## Bugs Fixed from Original

| # | Location               | Bug                                                         | Fix                                              |
|---|------------------------|-------------------------------------------------------------|--------------------------------------------------|
| 1 | DBConnection           | `socket.connect()` on every request → SocketAlreadyConnected | Fresh Socket per request                       |
| 2 | ThreadPoolInstance     | Busy-spin `while(!future.isDone())`                         | `StructuredTaskScope` / `future.get(timeout)`    |
| 3 | ThreadPoolInstance     | `awaitTermination()` before `shutdown()`                    | `shutdown()` first, then `awaitTermination()`    |
| 4 | FindDocumentTask       | `writeLock` for reads — all queries serialised              | `readLock`                                       |
| 5 | FileHelper.readFile    | Recursive → `StackOverflowError` on large files             | Iterative loop                                   |
| 6 | FileHelper             | `RandomAccessFile("rw")` in read-only methods               | Open as `"r"`                                    |
| 7 | FileHelper.writeToFile | `FileOutputStream` not in try-with-resources                | try-with-resources                               |
| 8 | AbstractTest           | Hardcoded Windows paths                                     | JUnit 5 `@TempDir`                               |
| 9 | Hooks                  | `removeDB()` commented out — state leaks between runs       | Restored `@BeforeAll/@AfterAll`                  |
|10 | Task classes           | Separate `StampedLock` per class — no shared exclusion      | Single shared lock                               |
|11 | BPlusTreeIndex         | Internal nodes stored values (B-Tree behaviour)             | Values only in leaf nodes                        |
|12 | BPlusTreeIndex         | Leaf split lost the median key                              | Median duplicated into right leaf                |
|13 | BPlusTreeIndex         | No `rangeScan()` — leaf list never used                     | `rangeScan(lo, hi)` implemented                  |
|14 | BPlusTreeIndex         | `nodeCounter` reset on reload → root corruption             | Recovered from file size                         |
|15 | BTreeIndex             | Values stored as 256-byte ASCII strings                     | Raw `writeLong()` — 8 bytes                      |
|16 | BTreeIndex             | `Long.valueOf("null")` crash on empty slots                 | Sentinel `-1L`                                   |
|17 | LSMTreeIndex           | `null` tombstone indistinguishable from "not found"         | `TOMBSTONE = Long.MIN_VALUE` sentinel            |
|18 | LSMTreeIndex           | SSTables scanned oldest-first — stale values returned       | Scan newest-first                                |
|19 | LSMTreeIndex           | Tombstones dropped during SSTable read                      | Tombstones survive `read()` and `write()`        |
|20 | LSMTreeIndex           | Compaction resurrected deleted keys                         | Tombstoned keys removed during merge             |
|21 | LSMTreeIndex           | Merged SSTable created in wrong directory                   | Use `dirPrefix` from constructor                 |
