package com.iu.indexes.lsmtree;

import com.iu.indexes.bloom.BloomFilter;

import java.io.*;
import java.util.TreeMap;

/**
 * Immutable sorted SSTable file — one per MemTable flush.
 *
 * BLOOM FILTER INTEGRATION
 * ─────────────────────────
 * Every SSTable now carries a {@link BloomFilter} built at write time.
 * During a point lookup (get), the caller checks the filter BEFORE
 * opening the file:
 *
 *   sstable.mightContain(key)  →  false  → skip file entirely  (O(k) bit ops)
 *                              →  true   → open and binary-search the file
 *
 * Without Bloom filters, a miss forces reading every SSTable on disk.
 * With them, a key that exists in no SSTable costs O(L·k) bit ops total
 * (L = number of SSTables, k = hash functions ≈ 7), versus O(L) disk reads.
 * For large L this is the difference between microseconds and milliseconds.
 *
 * The filter is rebuilt from the file contents on reload (see {@link #loadBloomFilter()}).
 *
 * File format: key(4 bytes) + value(8 bytes) per entry.
 * TOMBSTONE = {@link MemoryTable#TOMBSTONE} = Long.MIN_VALUE.
 */
class SSTable {
    private final File        file;
    private       BloomFilter bloomFilter;

    SSTable(File file) {
        this.file = file;
        // If the file already exists (reload after restart) rebuild the filter from it
        if (file.exists()) {
            try { loadBloomFilter(); } catch (IOException ignored) {}
        }
    }

    File getFile() { return file; }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /**
     * Write a sorted map to disk and build the Bloom filter.
     * Tombstones are written as Long.MIN_VALUE so compaction can honour them.
     */
    void write(TreeMap<Integer, Long> map) throws IOException {
        // Build Bloom filter from keys — tombstoned keys are still "in the SSTable"
        bloomFilter = new BloomFilter(Math.max(map.size(), 1), 0.01);

        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(file)))) {
            for (var entry : map.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeLong(entry.getValue());
                bloomFilter.add(entry.getKey()); // add int key to filter
            }
        }
    }

    // -----------------------------------------------------------------------
    // Bloom filter — O(k) membership check before any disk I/O
    // -----------------------------------------------------------------------

    /**
     * Returns false if the key is DEFINITELY NOT in this SSTable.
     * Returns true if the key MIGHT be in this SSTable (requires disk read to confirm).
     */
    boolean mightContain(int key) {
        return bloomFilter == null || bloomFilter.mightContain(key);
    }

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    /**
     * Read all entries including tombstones.
     * Only called after mightContain() returns true.
     */
    TreeMap<Integer, Long> read() throws IOException {
        TreeMap<Integer, Long> map = new TreeMap<>();
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file)))) {
            while (in.available() >= 12) {
                int  key   = in.readInt();
                long value = in.readLong();
                map.put(key, value);
            }
        }
        return map;
    }

    // -----------------------------------------------------------------------
    // Reload
    // -----------------------------------------------------------------------

    /** Rebuild the Bloom filter by reading keys from the existing file. */
    private void loadBloomFilter() throws IOException {
        TreeMap<Integer, Long> map = read();
        bloomFilter = new BloomFilter(Math.max(map.size(), 1), 0.01);
        map.keySet().forEach(bloomFilter::add);
    }
}
