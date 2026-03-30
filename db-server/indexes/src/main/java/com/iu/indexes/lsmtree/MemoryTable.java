package com.iu.indexes.lsmtree;

import java.util.Optional;
import java.util.TreeMap;

/**
 * In-memory component of the LSM Tree.
 *
 * Uses a sentinel value {@link #TOMBSTONE} to distinguish between
 * "key deleted" and "key not present" — fixing the original null ambiguity
 * that caused deletions to be invisible (Bug 15).
 *
 * TreeMap keeps keys in sorted order for efficient SSTable flush.
 */
class MemoryTable {

    /** Sentinel: key is deleted. Must not be confused with a real offset. */
    static final Long TOMBSTONE = Long.MIN_VALUE;

    private final TreeMap<Integer, Long> table = new TreeMap<>();

    void put(int key, long value) {
        table.put(key, value);
    }

    /** Record a logical deletion. */
    void remove(int key) {
        table.put(key, TOMBSTONE);
    }

    /**
     * Look up a key.
     * @return Optional.empty()   — key not in MemTable at all (must check SSTables)
     *         Optional.of(TOMBSTONE) — key was deleted
     *         Optional.of(offset)   — key is alive with this file offset
     */
    Optional<Long> get(int key) {
        if (!table.containsKey(key)) return Optional.empty();
        return Optional.of(table.get(key));
    }

    TreeMap<Integer, Long> getTable() { return table; }

    int size() { return table.size(); }

    void clear() { table.clear(); }
}
