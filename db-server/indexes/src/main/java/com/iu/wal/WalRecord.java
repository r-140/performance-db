package com.iu.wal;

import java.io.Serial;
import java.io.Serializable;

/**
 * Single WAL entry — sealed + records (Java 17/21).
 *
 * Before any write touches the data file the operation is first appended
 * to the WAL as one of these record types.  On crash the WAL is replayed
 * in order to restore the data file to a consistent state.
 *
 * The sealed hierarchy is exhaustive: the recovery loop uses a pattern-
 * matching switch so adding a new operation type is a compile error until
 * every switch handles it.
 */
public sealed interface WalRecord extends Serializable
        permits WalRecord.Insert, WalRecord.Delete, WalRecord.Commit, WalRecord.Rollback {

    /** The LSN (Log Sequence Number) uniquely identifies each record. */
    long lsn();

    /** Transaction id this record belongs to. */
    long txId();

    record Insert(long lsn, long txId, int docId, String payload)
            implements WalRecord {
        @Serial private static final long serialVersionUID = 1L;
    }

    record Delete(long lsn, long txId, int docId, String originalLine)
            implements WalRecord {
        @Serial private static final long serialVersionUID = 1L;
    }

    record Commit(long lsn, long txId)
            implements WalRecord {
        @Serial private static final long serialVersionUID = 1L;
    }

    record Rollback(long lsn, long txId)
            implements WalRecord {
        @Serial private static final long serialVersionUID = 1L;
    }
}
