package com.iu.indexes.transaction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.iu.indexes.transaction.TransactionIsolationLevel.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests demonstrating transaction isolation behaviour at all four SQL levels.
 *
 * Each test focuses on one classic anomaly:
 *  - Dirty Read:           reading uncommitted data from another transaction.
 *  - Non-Repeatable Read:  two reads of the same row return different values
 *                          because another tx committed between them.
 *  - Phantom Read:         a range scan returns more (or fewer) rows on the
 *                          second call because another tx inserted/deleted rows.
 *
 * The MVCCStore models these purely in-memory so the tests run without a
 * live DB server or network socket.
 */
class TransactionIsolationTest {

    private MVCCStore store;

    @BeforeEach
    void setUp() {
        store = new MVCCStore();
    }

    // =======================================================================
    // DIRTY READ
    // =======================================================================

    /**
     * READ_UNCOMMITTED allows seeing data written by a transaction that has
     * NOT yet committed.  This is the lowest isolation level and is almost
     * never used in production.
     */
    @Test
    void dirtyRead_readUncommitted_seesUncommittedData() {
        // Tx A writes but does NOT commit
        TransactionContext txA = store.beginTransaction(READ_UNCOMMITTED);
        store.insert(txA, 1, "dirty value");
        // txA still ACTIVE — not committed

        // Tx B reads at READ_UNCOMMITTED → sees txA's uncommitted write
        TransactionContext txB = store.beginTransaction(READ_UNCOMMITTED);
        VersionedRecord record = store.read(txB, 1);

        assertNotNull(record, "READ_UNCOMMITTED should see uncommitted (dirty) data");
        assertEquals("dirty value", record.value);

        store.rollback(txA); // txA never committed — the data was phantom
    }

    /**
     * READ_COMMITTED prevents dirty reads: txB cannot see txA's write until
     * txA commits.
     */
    @Test
    void dirtyRead_readCommitted_doesNotSeeDirtyData() {
        TransactionContext txA = store.beginTransaction(READ_COMMITTED);
        store.insert(txA, 1, "dirty value");
        // txA not committed

        TransactionContext txB = store.beginTransaction(READ_COMMITTED);
        VersionedRecord record = store.read(txB, 1);

        assertNull(record, "READ_COMMITTED must NOT see uncommitted writes");
        store.rollback(txA);
    }

    // =======================================================================
    // NON-REPEATABLE READ
    // =======================================================================

    /**
     * At READ_COMMITTED a transaction can see changes committed by others
     * between two reads of the same row (non-repeatable read).
     */
    @Test
    void nonRepeatableRead_readCommitted_seesDifferentValueOnSecondRead() throws Exception {
        // Setup: doc 42 exists and is committed
        TransactionContext setup = store.beginTransaction(READ_COMMITTED);
        store.insert(setup, 42, "original");
        store.commit(setup);

        TransactionContext txA = store.beginTransaction(READ_COMMITTED);

        // First read by txA
        VersionedRecord first = store.read(txA, 42);
        assertNotNull(first);
        assertEquals("original", first.value);

        // Another transaction deletes and re-inserts doc 42 with a new value, commits
        TransactionContext txB = store.beginTransaction(READ_COMMITTED);
        store.delete(txB, 42);
        store.insert(txB, 42, "updated");
        store.commit(txB);

        // Second read by txA at READ_COMMITTED — sees the committed update
        // This is the non-repeatable read anomaly
        VersionedRecord second = store.read(txA, 42);
        // Under READ_COMMITTED the second read sees txB's committed value
        assertNotNull(second);
        assertEquals("updated", second.value,
                "READ_COMMITTED allows non-repeatable reads: second read sees updated value");
    }

    /**
     * At REPEATABLE_READ the snapshot is taken at transaction start, so both
     * reads return the same value even after another tx commits a change.
     */
    @Test
    void nonRepeatableRead_repeatableRead_sameValueBothReads() {
        TransactionContext setup = store.beginTransaction(READ_COMMITTED);
        store.insert(setup, 42, "original");
        store.commit(setup);

        // txA uses REPEATABLE_READ — snapshot is fixed at start
        TransactionContext txA = store.beginTransaction(REPEATABLE_READ);
        VersionedRecord first = store.read(txA, 42);
        assertNotNull(first);
        assertEquals("original", first.value);

        // txB updates and commits
        TransactionContext txB = store.beginTransaction(READ_COMMITTED);
        store.delete(txB, 42);
        store.insert(txB, 42, "updated");
        store.commit(txB);

        // txA's second read still uses its snapshot — no change visible
        VersionedRecord second = store.read(txA, 42);
        assertNotNull(second);
        assertEquals("original", second.value,
                "REPEATABLE_READ prevents non-repeatable reads: same value on second read");
    }

    // =======================================================================
    // PHANTOM READ
    // =======================================================================

    /**
     * Phantom reads are the hardest anomaly to prevent.  They occur on range
     * scans when another transaction inserts rows in the scanned range and
     * commits between two scans.
     *
     * This test is the core demonstration of how isolation levels affect index
     * range scans (like a B+Tree range query on id ∈ [10, 20]).
     */
    @Test
    void phantomRead_readCommitted_seesPhantomRowOnSecondScan() {
        // Pre-populate docs 10 and 15 and commit
        TransactionContext setup = store.beginTransaction(READ_COMMITTED);
        store.insert(setup, 10, "doc10");
        store.insert(setup, 15, "doc15");
        store.commit(setup);

        // txA starts a READ_COMMITTED transaction and does first scan
        TransactionContext txA = store.beginTransaction(READ_COMMITTED);
        List<VersionedRecord> firstScan = store.rangeScan(txA, 10, 20);
        assertEquals(2, firstScan.size(), "First scan should see docs 10 and 15");

        // txB inserts doc 17 and commits — a new row in the scanned range
        TransactionContext txB = store.beginTransaction(READ_COMMITTED);
        store.insert(txB, 17, "doc17-phantom");
        store.commit(txB);

        // txA does a second scan — at READ_COMMITTED it sees txB's committed insert
        List<VersionedRecord> secondScan = store.rangeScan(txA, 10, 20);
        assertEquals(3, secondScan.size(),
                "READ_COMMITTED phantom read: second scan sees the newly inserted doc 17");
        assertTrue(secondScan.stream().anyMatch(r -> r.docId == 17));
    }

    /**
     * At REPEATABLE_READ (and SERIALIZABLE) the snapshot prevents phantoms:
     * a row inserted after the transaction started is invisible to that
     * transaction's range scans.
     */
    @Test
    void phantomRead_repeatableRead_noPhantomRowOnSecondScan() {
        TransactionContext setup = store.beginTransaction(READ_COMMITTED);
        store.insert(setup, 10, "doc10");
        store.insert(setup, 15, "doc15");
        store.commit(setup);

        // txA starts REPEATABLE_READ — snapshot fixed here
        TransactionContext txA = store.beginTransaction(REPEATABLE_READ);
        List<VersionedRecord> firstScan = store.rangeScan(txA, 10, 20);
        assertEquals(2, firstScan.size());

        // txB inserts doc 17 and commits after txA started
        TransactionContext txB = store.beginTransaction(READ_COMMITTED);
        store.insert(txB, 17, "doc17-phantom");
        store.commit(txB);

        // txA's second scan — snapshot isolation means it cannot see txB's insert
        List<VersionedRecord> secondScan = store.rangeScan(txA, 10, 20);
        assertEquals(2, secondScan.size(),
                "REPEATABLE_READ prevents phantom reads: second scan unchanged");
        assertTrue(secondScan.stream().noneMatch(r -> r.docId == 17));
    }

    /**
     * SERIALIZABLE behaves identically to REPEATABLE_READ for phantom
     * prevention in our MVCC model.  The difference (predicate locking) is
     * enforced at the lock layer which we don't fully model here, but the
     * visibility rule is the same.
     */
    @Test
    void phantomRead_serializable_noPhantomRowOnSecondScan() {
        TransactionContext setup = store.beginTransaction(READ_COMMITTED);
        store.insert(setup, 10, "doc10");
        store.commit(setup);

        TransactionContext txA = store.beginTransaction(SERIALIZABLE);
        List<VersionedRecord> first = store.rangeScan(txA, 10, 20);
        assertEquals(1, first.size());

        TransactionContext txB = store.beginTransaction(READ_COMMITTED);
        store.insert(txB, 14, "doc14");
        store.commit(txB);

        List<VersionedRecord> second = store.rangeScan(txA, 10, 20);
        assertEquals(1, second.size(),
                "SERIALIZABLE prevents phantom reads like REPEATABLE_READ");
    }

    // =======================================================================
    // ROLLBACK
    // =======================================================================

    @Test
    void rollback_dataBecomesInvisibleToAll() {
        TransactionContext txA = store.beginTransaction(READ_COMMITTED);
        store.insert(txA, 99, "will rollback");
        store.rollback(txA);

        TransactionContext txB = store.beginTransaction(READ_UNCOMMITTED);
        // Even READ_UNCOMMITTED cannot see rolled-back data because
        // the status is now ROLLED_BACK, not ACTIVE
        VersionedRecord record = store.read(txB, 99);
        assertNull(record, "Rolled-back data must not be visible to any transaction");
    }

    // =======================================================================
    // VISIBLE COUNT
    // =======================================================================

    @Test
    void visibleCount_correctPerIsolationLevel() {
        // Commit 3 docs
        TransactionContext s = store.beginTransaction(READ_COMMITTED);
        store.insert(s, 1, "a");
        store.insert(s, 2, "b");
        store.insert(s, 3, "c");
        store.commit(s);

        TransactionContext txRepeatable = store.beginTransaction(REPEATABLE_READ);

        // Now add a 4th doc — should be invisible to REPEATABLE_READ
        TransactionContext txNew = store.beginTransaction(READ_COMMITTED);
        store.insert(txNew, 4, "d");
        store.commit(txNew);

        assertEquals(3, store.visibleCount(txRepeatable),
                "REPEATABLE_READ snapshot should see only 3 docs committed before it started");

        // READ_COMMITTED sees all 4 committed docs
        TransactionContext txRC = store.beginTransaction(READ_COMMITTED);
        assertEquals(4, store.visibleCount(txRC));
    }

    // =======================================================================
    // CONCURRENT INSERTS (thread-safety smoke test)
    // =======================================================================

    @Test
    void concurrent_inserts_allEventuallyVisible() throws Exception {
        int threads = 10;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            final int docId = i + 100;
            pool.submit(() -> {
                TransactionContext tx = store.beginTransaction(READ_COMMITTED);
                store.insert(tx, docId, "concurrent-" + docId);
                store.commit(tx);
                latch.countDown();
            });
        }

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        pool.shutdown();

        TransactionContext reader = store.beginTransaction(READ_COMMITTED);
        assertEquals(threads, store.visibleCount(reader),
                "All " + threads + " concurrently inserted docs should be visible");
    }
}
