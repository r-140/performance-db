package com.iu.indexes.transaction;

/**
 * A single version of a document record used to demonstrate MVCC behaviour.
 *
 * Each write (INSERT / UPDATE) creates a new VersionedRecord.  DELETE is
 * modelled as a record with {@code deleted = true}.
 *
 * Fields mirror what the DB stores on disk but are kept in-memory here so
 * isolation tests can run without a live server.
 */
public class VersionedRecord {
    public final int    docId;
    public final String value;         // raw JSON payload
    public final long   fileOffset;    // position in data file (or -1 if in-memory only)
    public final long   writerTxId;    // transaction that wrote this version
    public volatile TransactionStatus writerStatus;  // updated when writer commits/rolls back
    public final boolean deleted;

    public VersionedRecord(int docId, String value, long fileOffset,
                           long writerTxId, TransactionStatus writerStatus,
                           boolean deleted) {
        this.docId        = docId;
        this.value        = value;
        this.fileOffset   = fileOffset;
        this.writerTxId   = writerTxId;
        this.writerStatus = writerStatus;
        this.deleted      = deleted;
    }

    /** Convenience: non-deleted record */
    public VersionedRecord(int docId, String value, long fileOffset,
                           long writerTxId, TransactionStatus writerStatus) {
        this(docId, value, fileOffset, writerTxId, writerStatus, false);
    }

    @Override
    public String toString() {
        return "VR{id=" + docId + ",tx=" + writerTxId + ",st=" + writerStatus
                + (deleted ? ",DELETED" : "") + "}";
    }
}
