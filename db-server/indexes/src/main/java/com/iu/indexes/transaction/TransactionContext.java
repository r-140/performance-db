package com.iu.indexes.transaction;

import java.util.concurrent.atomic.AtomicLong;

/**
 * MVCC (Multi-Version Concurrency Control) transaction context.
 *
 * Each transaction is assigned a monotonically increasing transaction id (txId).
 * Records written during a transaction are tagged with that txId; readers only
 * see records whose write txId ≤ their snapshot txId (for REPEATABLE_READ /
 * SERIALIZABLE), or ≤ the latest committed txId (for READ_COMMITTED).
 *
 * This class is intentionally simple — it models the concept rather than
 * implementing a full WAL-backed MVCC engine.
 */
public class TransactionContext {

    private static final AtomicLong TX_COUNTER = new AtomicLong(0);

    private final long txId;
    private final long snapshotTxId;     // largest committed txId at START of this tx
    private final TransactionIsolationLevel isolationLevel;
    private volatile TransactionStatus status;

    public TransactionContext(TransactionIsolationLevel isolationLevel, long latestCommittedTxId) {
        this.txId           = TX_COUNTER.incrementAndGet();
        this.snapshotTxId   = latestCommittedTxId;
        this.isolationLevel = isolationLevel;
        this.status         = TransactionStatus.ACTIVE;
    }

    public long getTxId()                         { return txId; }
    public long getSnapshotTxId()                 { return snapshotTxId; }
    public TransactionIsolationLevel getLevel()   { return isolationLevel; }
    public TransactionStatus getStatus()          { return status; }

    public void commit()   { this.status = TransactionStatus.COMMITTED; }
    public void rollback() { this.status = TransactionStatus.ROLLED_BACK; }
    public boolean isActive() { return status == TransactionStatus.ACTIVE; }

    /**
     * Determines whether this transaction should see a record written by
     * {@code writerTxId}.
     *
     * Rules:
     *  READ_UNCOMMITTED – see everything, even uncommitted writes.
     *  READ_COMMITTED   – see only records whose writer has already committed,
     *                     evaluated at the moment of each read (no snapshot).
     *  REPEATABLE_READ  – see only records committed before this tx started
     *                     (snapshotTxId).
     *  SERIALIZABLE     – same snapshot as REPEATABLE_READ; phantom prevention
     *                     requires a predicate lock on top (not modelled here).
     */
    public boolean canSee(long writerTxId, TransactionStatus writerStatus) {
        return switch (isolationLevel) {
            case READ_UNCOMMITTED -> true;
            case READ_COMMITTED   -> writerStatus == TransactionStatus.COMMITTED;
            case REPEATABLE_READ,
                 SERIALIZABLE     -> writerTxId <= snapshotTxId &&
                                     writerStatus == TransactionStatus.COMMITTED;
        };
    }

    @Override
    public String toString() {
        return "Tx{id=" + txId + ",snap=" + snapshotTxId + ",lvl=" + isolationLevel + ",st=" + status + "}";
    }
}
