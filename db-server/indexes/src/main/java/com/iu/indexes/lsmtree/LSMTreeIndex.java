package com.iu.indexes.lsmtree;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Log-Structured Merge Tree index.
 *
 * Write path: MemTable (TreeMap in memory) → SSTable file on disk when full.
 * Read path:  MemTable first, then SSTables newest-first.
 *
 * BLOOM FILTER IN THE READ PATH
 * ──────────────────────────────
 * Before reading any SSTable file from disk, get() checks that SSTable's
 * Bloom filter. A "definitely not present" answer skips the file entirely:
 *
 *   for each SSTable (newest → oldest):
 *     1. sstable.mightContain(key)  →  O(k) bit ops, no I/O
 *        false  → SKIP this SSTable (Bloom filter guarantee: no false negatives)
 *        true   → read the SSTable file and look up the key
 *
 * Without Bloom filters: a key absent from all SSTables causes L disk reads
 * (L = number of SSTables).
 * With Bloom filters: a miss costs only L × k bit-array lookups — no disk I/O.
 * A false-positive rate of 1% means 1 in 100 "true" answers are wrong (need
 * disk read anyway), but 0% false negatives so correctness is never affected.
 *
 * Example with 100 SSTables and 1% FPR:
 *   Missing key:  0 disk reads (all 100 filters say "definitely not here")
 *   Present key:  ~1 disk read (only the correct SSTable says "maybe here")
 */
public class LSMTreeIndex {

    private static final Logger LOGGER = Logger.getLogger(LSMTreeIndex.class.getName());
    private static final int MEMTABLE_LIMIT = 5;

    private final MemoryTable    memTable  = new MemoryTable();
    private final List<SSTable>  sstables  = new ArrayList<>(); // index 0 = oldest
    private       int            sstableCounter = 0;
    private final String         dirPrefix;

    // Statistics — logged after each get() to make the Bloom filter effect visible
    private long bloomHits   = 0; // Bloom filter said "definitely not here" → skipped
    private long bloomMisses = 0; // Bloom filter said "maybe here" → file read

    public LSMTreeIndex(String filePrefix) {
        this.dirPrefix = filePrefix;
    }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    public void put(int key, long value) throws IOException {
        memTable.put(key, value);
        if (memTable.size() >= MEMTABLE_LIMIT) flush();
    }

    public void remove(int key) throws IOException {
        memTable.remove(key);
        if (memTable.size() >= MEMTABLE_LIMIT) flush();
    }

    // -----------------------------------------------------------------------
    // Read — Bloom filter consulted before every SSTable file access
    // -----------------------------------------------------------------------

    /**
     * Look up a key.
     *
     * 1. MemTable check (in-memory, O(log N)).
     *    Tombstone present → key is deleted → return null immediately.
     *    Value present     → return it immediately (no disk I/O).
     *
     * 2. SSTable scan, newest first.
     *    For each SSTable:
     *      a. Bloom filter check — O(k), no I/O.
     *         "Definitely absent" → skip this SSTable entirely.
     *      b. Read SSTable from disk — O(entries).
     *         Tombstone → deleted → return null.
     *         Value     → return it.
     *
     * 3. Key not found anywhere → return null.
     */
    public Long get(int key) throws IOException {
        // Step 1: MemTable
        Optional<Long> memResult = memTable.get(key);
        if (memResult.isPresent()) {
            Long v = memResult.get();
            return MemoryTable.TOMBSTONE.equals(v) ? null : v;
        }

        // Step 2: SSTables newest-first with Bloom filter guard
        for (int i = sstables.size() - 1; i >= 0; i--) {
            SSTable sstable = sstables.get(i);

            // ── Bloom filter check ──────────────────────────────────────
            if (!sstable.mightContain(key)) {
                bloomHits++;
                LOGGER.log(Level.FINEST,
                    "Bloom filter: key " + key + " definitely absent from " + sstable.getFile().getName());
                continue; // skip — no disk I/O for this SSTable
            }
            bloomMisses++;
            // ── Disk read (only reached when Bloom says "maybe") ─────────
            TreeMap<Integer, Long> table = sstable.read();
            if (table.containsKey(key)) {
                Long v = table.get(key);
                return MemoryTable.TOMBSTONE.equals(v) ? null : v;
            }
        }

        logBloomStats();
        return null;
    }

    // -----------------------------------------------------------------------
    // Flush & Compaction
    // -----------------------------------------------------------------------

    public void flush() throws IOException {
        if (memTable.size() == 0) return;
        File file = new File(dirPrefix + sstableCounter++ + ".dat");
        SSTable sstable = new SSTable(file);
        sstable.write(memTable.getTable()); // also builds Bloom filter
        sstables.add(sstable);
        memTable.clear();
        LOGGER.log(Level.FINE, "Flushed MemTable to " + file.getName());
    }

    /**
     * Merge the two oldest SSTables.
     * Newer entries win; tombstoned keys are dropped from the merged result.
     * Bloom filter for the merged SSTable is built automatically by SSTable.write().
     */
    public void merge() throws IOException {
        if (sstables.size() < 2) return;

        SSTable older = sstables.remove(0);
        SSTable newer = sstables.remove(0);

        TreeMap<Integer, Long> merged = older.read();
        merged.putAll(newer.read()); // newer wins
        merged.values().removeIf(v -> MemoryTable.TOMBSTONE.equals(v)); // drop tombstones

        File mergedFile = new File(dirPrefix + sstableCounter++ + ".dat");
        SSTable mergedSSTable = new SSTable(mergedFile);
        mergedSSTable.write(merged); // builds fresh Bloom filter for merged keys
        sstables.add(0, mergedSSTable);

        older.getFile().delete();
        newer.getFile().delete();

        LOGGER.log(Level.INFO, "Merged → " + mergedFile.getName()
            + " (" + merged.size() + " entries)");
    }

    public void flushAll() throws IOException {
        if (memTable.size() > 0) flush();
    }

    // Bloom filter statistics — useful for demos / performance comparisons
    public long bloomFilterHits()   { return bloomHits;   }
    public long bloomFilterMisses() { return bloomMisses; }

    private void logBloomStats() {
        if (LOGGER.isLoggable(Level.FINE) && (bloomHits + bloomMisses) > 0) {
            LOGGER.log(Level.FINE, String.format(
                "LSM Bloom stats: hits(skipped)=%d misses(read)=%d saved=%.1f%%",
                bloomHits, bloomMisses,
                100.0 * bloomHits / (bloomHits + bloomMisses)));
        }
    }
}
