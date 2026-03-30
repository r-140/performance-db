package com.iu.wal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests verifying WAL durability guarantees.
 *
 * The key property: only COMMITTED transactions appear in replay().
 * Incomplete or rolled-back transactions are silently dropped.
 */
class WriteAheadLogTest {

    // -----------------------------------------------------------------------
    // Normal operation
    // -----------------------------------------------------------------------

    @Test
    void append_commit_appearsInReplay(@TempDir Path tmp) throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(tmp.resolve("wal.dat").toString())) {
            wal.appendInsert(1L, 42, "{\"id\":42}");
            wal.appendCommit(1L);
        }

        WriteAheadLog wal2 = new WriteAheadLog(tmp.resolve("wal.dat").toString());
        List<WalRecord> ops = wal2.replay();
        assertEquals(1, ops.size());
        assertInstanceOf(WalRecord.Insert.class, ops.get(0));
        assertEquals(42, ((WalRecord.Insert) ops.get(0)).docId());
    }

    @Test
    void append_noCommit_doesNotAppearInReplay(@TempDir Path tmp) throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(tmp.resolve("wal.dat").toString())) {
            wal.appendInsert(1L, 99, "{\"id\":99}");
            // NO commit — simulates crash after write but before commit
        }

        WriteAheadLog wal2 = new WriteAheadLog(tmp.resolve("wal.dat").toString());
        List<WalRecord> ops = wal2.replay();
        assertTrue(ops.isEmpty(),
            "Uncommitted insert must not appear in replay (crash safety)");
    }

    @Test
    void rollback_doesNotAppearInReplay(@TempDir Path tmp) throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(tmp.resolve("wal.dat").toString())) {
            wal.appendInsert(1L, 7, "{\"id\":7}");
            wal.appendRollback(1L);
        }

        WriteAheadLog wal2 = new WriteAheadLog(tmp.resolve("wal.dat").toString());
        assertTrue(wal2.replay().isEmpty(),
            "Rolled-back transaction must not appear in replay");
    }

    // -----------------------------------------------------------------------
    // Multiple transactions
    // -----------------------------------------------------------------------

    @Test
    void twoTransactions_onlyCommittedReplayed(@TempDir Path tmp) throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(tmp.resolve("wal.dat").toString())) {
            // tx 1 — committed
            wal.appendInsert(1L, 10, "{\"id\":10}");
            wal.appendCommit(1L);
            // tx 2 — crashed (no commit)
            wal.appendInsert(2L, 20, "{\"id\":20}");
        }

        WriteAheadLog wal2 = new WriteAheadLog(tmp.resolve("wal.dat").toString());
        List<WalRecord> ops = wal2.replay();
        assertEquals(1, ops.size());
        assertEquals(10, ((WalRecord.Insert) ops.get(0)).docId());
    }

    @Test
    void deleteOperation_committedAndReplayed(@TempDir Path tmp) throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(tmp.resolve("wal.dat").toString())) {
            wal.appendDelete(1L, 5, "5,{\"id\":5}");
            wal.appendCommit(1L);
        }

        WriteAheadLog wal2 = new WriteAheadLog(tmp.resolve("wal.dat").toString());
        List<WalRecord> ops = wal2.replay();
        assertEquals(1, ops.size());
        assertInstanceOf(WalRecord.Delete.class, ops.get(0));
        assertEquals(5, ((WalRecord.Delete) ops.get(0)).docId());
    }

    // -----------------------------------------------------------------------
    // LSN ordering
    // -----------------------------------------------------------------------

    @Test
    void lsns_areMonotonicallyIncreasing(@TempDir Path tmp) throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(tmp.resolve("wal.dat").toString())) {
            wal.appendInsert(1L, 1, "a");
            wal.appendInsert(1L, 2, "b");
            wal.appendCommit(1L);
        }

        WriteAheadLog wal2 = new WriteAheadLog(tmp.resolve("wal.dat").toString());
        List<WalRecord> all = wal2.readAll();
        for (int i = 1; i < all.size(); i++) {
            assertTrue(all.get(i).lsn() > all.get(i - 1).lsn(),
                "LSNs must be strictly increasing");
        }
    }

    // -----------------------------------------------------------------------
    // Truncate / checkpoint
    // -----------------------------------------------------------------------

    @Test
    void truncate_clearsAllRecords(@TempDir Path tmp) throws IOException {
        WriteAheadLog wal = new WriteAheadLog(tmp.resolve("wal.dat").toString());
        wal.appendInsert(1L, 1, "a");
        wal.appendCommit(1L);
        wal.truncate();

        assertTrue(wal.replay().isEmpty(), "WAL must be empty after truncate");
        wal.close();
    }

    // -----------------------------------------------------------------------
    // Sealed pattern-matching smoke test
    // -----------------------------------------------------------------------

    @Test
    void patternMatchingSwitch_coversAllTypes(@TempDir Path tmp) throws IOException {
        try (WriteAheadLog wal = new WriteAheadLog(tmp.resolve("wal.dat").toString())) {
            wal.appendInsert(1L, 1, "x");
            wal.appendDelete(2L, 2, "y");
            wal.appendCommit(1L);
            wal.appendRollback(2L);
        }

        WriteAheadLog wal2 = new WriteAheadLog(tmp.resolve("wal.dat").toString());
        for (WalRecord rec : wal2.readAll()) {
            // Exhaustive switch — compile error if a new WalRecord subtype is added
            // without updating this switch
            String desc = switch (rec) {
                case WalRecord.Insert   r -> "insert  id=" + r.docId();
                case WalRecord.Delete   r -> "delete  id=" + r.docId();
                case WalRecord.Commit   r -> "commit  tx=" + r.txId();
                case WalRecord.Rollback r -> "rollback tx=" + r.txId();
            };
            assertNotNull(desc);
        }
    }
}
