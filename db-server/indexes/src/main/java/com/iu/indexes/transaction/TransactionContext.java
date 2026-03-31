package com.iu.indexes.transaction;

import java.util.concurrent.atomic.AtomicLong;

/**
 * MVCC transaction context.
 *
 * canSee() visibility rules per isolation level:
 *
 *   ROLLED_BACK records are NEVER visible regardless of isolation level.
 *   A rolled-back transaction never committed, so its writes must be
 *   completely invisible — even to READ_UNCOMMITTED.
 *
 *   READ_UNCOMMITTED: see ACTIVE and COMMITTED records.
 *   READ_COMMITTED:   see only COMMITTED records (re-evaluated per read).
 *   REPEATABLE_READ:  see only COMMITTED records with writerTxId <= snapshotTxId.
 *   SERIALIZABLE:     same as REPEATABLE_READ in this MVCC model.
 */
public class TransactionContext {

    private static final AtomicLong TX_COUNTER = new AtomicLong(0);

    private final long txId;
    private final long snapshotTxId;
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
     * Can this transaction see a record written by writerTxId with writerStatus?
     *
     * ROLLED_BACK is invisible at every level — a transaction that rolled back
     * never committed, so it is as if its writes never happened.
     */
    public boolean canSee(long writerTxId, TransactionStatus writerStatus) {
        // Rolled-back writes are invisible to everyone, always.
        if (writerStatus == TransactionStatus.ROLLED_BACK) return false;

        return switch (isolationLevel) {
            case READ_UNCOMMITTED ->
                // See ACTIVE and COMMITTED, but never ROLLED_BACK (handled above)
                true;
            case READ_COMMITTED ->
                // Only committed writes; re-evaluated at each read (no snapshot)
                writerStatus == TransactionStatus.COMMITTED;
            case REPEATABLE_READ, SERIALIZABLE ->
                // Only writes committed before this transaction started
                writerTxId <= snapshotTxId && writerStatus == TransactionStatus.COMMITTED;
        };
    }

    @Override
    public String toString() {
        return "Tx{id=" + txId + ",snap=" + snapshotTxId
             + ",lvl=" + isolationLevel + ",st=" + status + "}";
    }
}
