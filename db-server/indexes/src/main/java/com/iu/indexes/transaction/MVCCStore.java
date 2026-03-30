package com.iu.indexes.transaction;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * In-memory MVCC data store used to demonstrate transaction isolation anomalies.
 *
 * How phantom reads happen with B+Tree / range indexes:
 *  1. Transaction A begins (REPEATABLE_READ snapshot).
 *  2. Transaction A scans index for id ∈ [10, 20] → gets ids 10,15.
 *  3. Transaction B inserts id=17, commits.
 *  4. Transaction A rescans same range:
 *       - REPEATABLE_READ: still sees 10,15  (snapshot isolation; no phantom).
 *       - READ_COMMITTED:  now sees 10,15,17 (phantom!).
 *
 * This class simulates that in-process without a live server so tests can
 * assert the expected behaviour deterministically.
 */
public class MVCCStore {
    private static final Logger LOGGER = Logger.getLogger(MVCCStore.class.getName());

    /** All versions, keyed by docId. Each list is ordered oldest→newest. */
    private final ConcurrentHashMap<Integer, List<VersionedRecord>> versions = new ConcurrentHashMap<>();

    /** Global committed-tx high-water mark, used for snapshot generation. */
    private final AtomicLong latestCommittedTxId = new AtomicLong(0);

    // -----------------------------------------------------------------------
    // Transaction management
    // -----------------------------------------------------------------------

    public TransactionContext beginTransaction(TransactionIsolationLevel level) {
        TransactionContext tx = new TransactionContext(level, latestCommittedTxId.get());
        LOGGER.log(Level.FINE, "begin " + tx);
        return tx;
    }

    public void commit(TransactionContext tx) {
        tx.commit();
        latestCommittedTxId.updateAndGet(cur -> Math.max(cur, tx.getTxId()));
        // Update writerStatus in all records written by this tx
        versions.values().forEach(list ->
                list.stream()
                    .filter(r -> r.writerTxId == tx.getTxId())
                    .forEach(r -> r.writerStatus = TransactionStatus.COMMITTED));
        LOGGER.log(Level.FINE, "committed " + tx);
    }

    public void rollback(TransactionContext tx) {
        tx.rollback();
        // Mark all records by this tx as rolled-back; they will be invisible
        versions.values().forEach(list ->
                list.stream()
                    .filter(r -> r.writerTxId == tx.getTxId())
                    .forEach(r -> r.writerStatus = TransactionStatus.ROLLED_BACK));
        LOGGER.log(Level.FINE, "rolled-back " + tx);
    }

    // -----------------------------------------------------------------------
    // Write operations (must be within a transaction)
    // -----------------------------------------------------------------------

    public void insert(TransactionContext tx, int docId, String value) {
        assertActive(tx);
        VersionedRecord record = new VersionedRecord(docId, value, -1L, tx.getTxId(), TransactionStatus.ACTIVE);
        versions.computeIfAbsent(docId, k -> Collections.synchronizedList(new ArrayList<>())).add(record);
        LOGGER.log(Level.FINE, tx + " insert id=" + docId);
    }

    public void delete(TransactionContext tx, int docId) {
        assertActive(tx);
        VersionedRecord tombstone = new VersionedRecord(docId, null, -1L, tx.getTxId(), TransactionStatus.ACTIVE, true);
        versions.computeIfAbsent(docId, k -> Collections.synchronizedList(new ArrayList<>())).add(tombstone);
    }

    // -----------------------------------------------------------------------
    // Read operations
    // -----------------------------------------------------------------------

    /**
     * Read the latest visible version of a single document.
     * Returns null if not found / deleted / not visible to this transaction.
     */
    public VersionedRecord read(TransactionContext tx, int docId) {
        List<VersionedRecord> list = versions.get(docId);
        if (list == null) return null;

        // Walk newest → oldest looking for first visible version
        List<VersionedRecord> snapshot = new ArrayList<>(list);
        for (int i = snapshot.size() - 1; i >= 0; i--) {
            VersionedRecord r = snapshot.get(i);
            if (tx.canSee(r.writerTxId, r.writerStatus)) {
                return r.deleted ? null : r;
            }
        }
        return null;
    }

    /**
     * Range scan: returns all visible documents whose id is in [lo, hi].
     *
     * This is the operation that reveals phantom reads when called twice within
     * the same transaction at READ_COMMITTED level while another transaction
     * inserts into the range between the two calls.
     */
    public List<VersionedRecord> rangeScan(TransactionContext tx, int lo, int hi) {
        List<VersionedRecord> result = new ArrayList<>();
        for (int id = lo; id <= hi; id++) {
            VersionedRecord r = read(tx, id);
            if (r != null) result.add(r);
        }
        LOGGER.log(Level.FINE, tx + " rangeScan [" + lo + "," + hi + "] → " + result.size() + " rows");
        return result;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private void assertActive(TransactionContext tx) {
        if (!tx.isActive())
            throw new IllegalStateException("Transaction " + tx.getTxId() + " is not active");
    }

    /** Visible document count for a transaction (useful in tests). */
    public long visibleCount(TransactionContext tx) {
        return versions.keySet().stream()
                .mapToLong(id -> read(tx, id) != null ? 1 : 0)
                .sum();
    }
}
