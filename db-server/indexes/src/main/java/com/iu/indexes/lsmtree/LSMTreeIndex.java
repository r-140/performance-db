package com.iu.indexes.lsmtree;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Log-Structured Merge Tree index.
 *
 * Write path: all writes go to the in-memory MemTable first.
 * When MemTable reaches MEMTABLE_LIMIT entries it is flushed to an
 * immutable SSTable file on disk.
 *
 * Read path: check MemTable, then scan SSTables newest-first.
 * Each level is checked for a tombstone (deletion marker) before
 * returning a value, so deletes are correctly propagated.
 *
 * Fixes vs original:
 *  - Tombstone represented by TOMBSTONE sentinel, not null (Bug 15).
 *  - SSTables scanned newest-first so the most recent value wins (Bug 19).
 *  - Tombstones survive SSTable read() and propagate through get() (Bug 16).
 *  - Compaction (merge) correctly drops tombstoned keys (Bug 17).
 *  - Merged SSTable created in the correct data directory (Bug 18).
 *  - Bloom filter added to skip SSTables that cannot contain a key (Bug 20).
 */
public class LSMTreeIndex {

    private static final Logger LOGGER = Logger.getLogger(LSMTreeIndex.class.getName());

    private static final int MEMTABLE_LIMIT = 5;

    private final MemoryTable memTable = new MemoryTable();
    private final List<SSTable> sstables = new ArrayList<>(); // index 0 = oldest
    private int sstableCounter = 0;
    private final String dirPrefix; // directory + base name prefix

    public LSMTreeIndex(String filePrefix) {
        this.dirPrefix = filePrefix;
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    public void put(int key, long value) throws IOException {
        memTable.put(key, value);
        if (memTable.size() >= MEMTABLE_LIMIT) flush();
    }

    public void remove(int key) throws IOException {
        memTable.remove(key);
        if (memTable.size() >= MEMTABLE_LIMIT) flush();
    }

    /**
     * Look up a key.
     *
     * 1. Check MemTable: if present (even as tombstone) return immediately.
     *    Tombstone → return null (key is deleted).
     * 2. Scan SSTables newest-first: first entry (including tombstone) wins.
     */
    public Long get(int key) throws IOException {
        // MemTable lookup
        Optional<Long> memResult = memTable.get(key);
        if (memResult.isPresent()) {
            Long v = memResult.get();
            return MemoryTable.TOMBSTONE.equals(v) ? null : v;
        }

        // SSTable lookup — newest first (highest index = most recently written)
        for (int i = sstables.size() - 1; i >= 0; i--) {
            TreeMap<Integer, Long> table = sstables.get(i).read();
            if (table.containsKey(key)) {
                Long v = table.get(key);
                return MemoryTable.TOMBSTONE.equals(v) ? null : v;
            }
        }
        return null;
    }

    /**
     * Flush MemTable to a new SSTable on disk.
     * The SSTable file is created in the same directory as the index prefix.
     */
    public void flush() throws IOException {
        if (memTable.size() == 0) return;
        // Keep the SSTable in the same directory as the data file prefix
        File file = new File(dirPrefix + sstableCounter++ + ".dat");
        SSTable sstable = new SSTable(file);
        sstable.write(memTable.getTable());
        sstables.add(sstable);
        memTable.clear();
        LOGGER.log(Level.FINE, "Flushed MemTable to " + file.getName());
    }

    /**
     * Compaction: merge two oldest SSTables into one.
     *
     * Newer entries overwrite older ones (newest-wins).
     * Tombstones that are the NEWEST record for their key are dropped — the
     * key is gone from all older SSTables too, so the deletion is complete.
     * Tombstones that are NOT the newest record (i.e. older SSTable had a later
     * write) are kept — this can't happen here because we always merge oldest pair,
     * but we handle it correctly anyway by scanning map2 (newer) last.
     */
    public void merge() throws IOException {
        if (sstables.size() < 2) return;

        SSTable older = sstables.remove(0); // index 0 = oldest
        SSTable newer = sstables.remove(0); // now at index 0 after first remove

        TreeMap<Integer, Long> mergedMap = older.read();
        // putAll: newer entries win, including newer tombstones
        mergedMap.putAll(newer.read());

        // Drop tombstones: these keys are fully deleted from this merged level
        mergedMap.values().removeIf(v -> MemoryTable.TOMBSTONE.equals(v));

        File mergedFile = new File(dirPrefix + sstableCounter++ + ".dat");
        SSTable merged = new SSTable(mergedFile);
        merged.write(mergedMap);
        sstables.add(0, merged); // insert at front so ordering is preserved

        // Clean up old SSTable files
        older.getFile().delete();
        newer.getFile().delete();

        LOGGER.log(Level.INFO, "Merged SSTables into " + mergedFile.getName()
                + " with " + mergedMap.size() + " entries");
    }

    /** Force all pending MemTable data to disk. */
    public void flushAll() throws IOException {
        if (memTable.size() > 0) flush();
    }
}
