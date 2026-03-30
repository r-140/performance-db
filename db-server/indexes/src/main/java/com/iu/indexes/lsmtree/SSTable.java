package com.iu.indexes.lsmtree;

import java.io.*;
import java.util.TreeMap;

/**
 * Immutable sorted file of key→value entries written during a MemTable flush.
 *
 * Tombstones are stored as the sentinel value {@link MemoryTable#TOMBSTONE}
 * so compaction can detect and drop deleted keys.  The original code used
 * empty-string to mean "deleted" but then skipped those entries during read(),
 * causing compaction to resurrect deleted keys (Bugs 16 & 17).
 *
 * Format per entry: key(4 bytes) + value(8 bytes, Long.MIN_VALUE = tombstone).
 */
class SSTable {
    private final File file;

    SSTable(File file) {
        this.file = file;
    }

    File getFile() { return file; }

    /**
     * Write a sorted map to disk.
     * Tombstones ({@link MemoryTable#TOMBSTONE}) are written as-is — they must
     * survive the flush so compaction can honour them.
     */
    void write(TreeMap<Integer, Long> map) throws IOException {
        try (DataOutputStream out = new DataOutputStream(
                new BufferedOutputStream(new FileOutputStream(file)))) {
            for (var entry : map.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeLong(entry.getValue()); // TOMBSTONE = Long.MIN_VALUE
            }
        }
    }

    /**
     * Read all entries including tombstones.
     * Callers must check {@link MemoryTable#TOMBSTONE} to honour deletions.
     */
    TreeMap<Integer, Long> read() throws IOException {
        TreeMap<Integer, Long> map = new TreeMap<>();
        try (DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(file)))) {
            while (in.available() >= 12) { // 4 (int) + 8 (long)
                int  key   = in.readInt();
                long value = in.readLong();
                map.put(key, value);   // keep tombstones
            }
        }
        return map;
    }
}
