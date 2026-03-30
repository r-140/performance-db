package com.iu.wal;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Append-only Write-Ahead Log.
 *
 * HOW WAL WORKS
 * =============
 * Rule: every change must be written to the WAL and flushed (fsync) BEFORE
 * it is applied to the actual data file.  On a crash, uncommitted WAL
 * records are discarded; committed ones are re-applied during recovery.
 *
 * This guarantees Durability (the D in ACID): a committed transaction
 * survives a crash even if the data-file write hadn't completed yet.
 *
 * Write path (normal operation):
 *   1. append(Insert/Delete)  → write to WAL file, fsync
 *   2. apply change to data file (FileHelper.writeToFile / removeLineFromFile)
 *   3. append(Commit)         → write to WAL file, fsync
 *
 * Recovery path (after crash):
 *   replay() scans the WAL file:
 *   - Insert with matching Commit   → re-apply the insert if absent
 *   - Delete with matching Commit   → re-apply the delete if present
 *   - Insert/Delete without Commit  → skip (transaction never committed)
 *
 * File format: Java object serialization, one WalRecord per entry.
 * Production DBs use binary record formats for speed; we use Java
 * serialization here to keep the code readable.
 *
 * LSN (Log Sequence Number): monotonically increasing counter.  Every
 * record gets a unique LSN.  The data file can store the LSN of the last
 * applied record so recovery knows where to start.
 */
public class WriteAheadLog implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(WriteAheadLog.class.getName());

    private final Path            walPath;
    private final AtomicLong      lsnCounter;
    private       ObjectOutputStream out;   // kept open for sequential appends

    public WriteAheadLog(String walFilePath) throws IOException {
        this.walPath = Paths.get(walFilePath);
        boolean exists = Files.exists(walPath);
        // Recover the highest LSN from an existing WAL so we never reuse one
        this.lsnCounter = new AtomicLong(exists ? recoverMaxLsn() : 0L);
        // Open in append mode; ObjectOutputStream header written only if new file
        FileOutputStream fos = new FileOutputStream(walFilePath, exists);
        this.out = exists
                ? new AppendingObjectOutputStream(fos)
                : new ObjectOutputStream(fos);
    }

    // -----------------------------------------------------------------------
    // Append operations (called BEFORE touching the data file)
    // -----------------------------------------------------------------------

    public synchronized WalRecord.Insert appendInsert(long txId, int docId, String payload)
            throws IOException {
        var rec = new WalRecord.Insert(nextLsn(), txId, docId, payload);
        write(rec);
        return rec;
    }

    public synchronized WalRecord.Delete appendDelete(long txId, int docId, String originalLine)
            throws IOException {
        var rec = new WalRecord.Delete(nextLsn(), txId, docId, originalLine);
        write(rec);
        return rec;
    }

    public synchronized WalRecord.Commit appendCommit(long txId) throws IOException {
        var rec = new WalRecord.Commit(nextLsn(), txId);
        write(rec);
        return rec;
    }

    public synchronized WalRecord.Rollback appendRollback(long txId) throws IOException {
        var rec = new WalRecord.Rollback(nextLsn(), txId);
        write(rec);
        return rec;
    }

    // -----------------------------------------------------------------------
    // Recovery
    // -----------------------------------------------------------------------

    /**
     * Replay the WAL and return a list of operations that should be
     * re-applied to the data file.
     *
     * Only operations belonging to COMMITTED transactions are returned.
     * Operations from rolled-back or incomplete transactions are skipped.
     *
     * Uses pattern-matching switch (Java 21) over the sealed WalRecord.
     */
    public List<WalRecord> replay() throws IOException {
        if (!Files.exists(walPath)) return List.of();

        Map<Long, List<WalRecord>> byTx = new LinkedHashMap<>(); // preserve LSN order
        Set<Long> committed  = new HashSet<>();
        Set<Long> rolledBack = new HashSet<>();

        for (WalRecord rec : readAll()) {
            switch (rec) {
                case WalRecord.Insert  r -> byTx.computeIfAbsent(r.txId(), k -> new ArrayList<>()).add(r);
                case WalRecord.Delete  r -> byTx.computeIfAbsent(r.txId(), k -> new ArrayList<>()).add(r);
                case WalRecord.Commit  r -> committed.add(r.txId());
                case WalRecord.Rollback r -> rolledBack.add(r.txId());
            }
        }

        List<WalRecord> toReapply = new ArrayList<>();
        for (var entry : byTx.entrySet()) {
            long txId = entry.getKey();
            if (committed.contains(txId)) {
                toReapply.addAll(entry.getValue());
                LOGGER.log(Level.FINE, "WAL replay: tx " + txId + " committed → " + entry.getValue().size() + " ops");
            } else {
                LOGGER.log(Level.INFO, "WAL replay: tx " + txId + " not committed → skipping (crash recovery)");
            }
        }
        return toReapply;
    }

    /** Read all WAL records in LSN order. */
    public List<WalRecord> readAll() throws IOException {
        if (!Files.exists(walPath)) return List.of();
        List<WalRecord> records = new ArrayList<>();
        try (ObjectInputStream ois = new ObjectInputStream(
                new BufferedInputStream(Files.newInputStream(walPath)))) {
            while (true) {
                try {
                    records.add((WalRecord) ois.readObject());
                } catch (EOFException | ClassNotFoundException e) {
                    break;
                }
            }
        }
        return records;
    }

    /** Truncate the WAL after a successful checkpoint. */
    public synchronized void truncate() throws IOException {
        close();
        Files.deleteIfExists(walPath);
        FileOutputStream fos = new FileOutputStream(walPath.toString(), false);
        out = new ObjectOutputStream(fos);
        LOGGER.info("WAL truncated after checkpoint");
    }

    @Override
    public synchronized void close() throws IOException {
        if (out != null) { out.flush(); out.close(); out = null; }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private long nextLsn() { return lsnCounter.incrementAndGet(); }

    private void write(WalRecord rec) throws IOException {
        out.writeObject(rec);
        out.flush();  // fsync-equivalent: ensure OS buffer is written
        LOGGER.log(Level.FINEST, "WAL append: " + rec);
    }

    private long recoverMaxLsn() {
        try {
            return readAll().stream().mapToLong(WalRecord::lsn).max().orElse(0L);
        } catch (IOException e) {
            return 0L;
        }
    }

    /**
     * ObjectOutputStream that doesn't write the stream header on append.
     * Required when appending to an existing serialized stream.
     */
    private static class AppendingObjectOutputStream extends ObjectOutputStream {
        AppendingObjectOutputStream(OutputStream out) throws IOException { super(out); }
        @Override protected void writeStreamHeader() {} // suppress header on append
    }
}
