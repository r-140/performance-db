package com.iu.olap.storage;

import java.util.BitSet;

/**
 * Bloom filter used by each micro-partition for per-column membership checks.
 *
 * In Snowflake, every micro-partition stores a Bloom filter for each column.
 * Before reading a partition, the query engine checks:
 *   bloomFilter.mightContain(filterValue) → false → skip this partition entirely
 *
 * This is complementary to min/max pruning:
 *   - Min/max is perfect for range predicates (WHERE date BETWEEN x AND y)
 *     but misses point lookups in sparse data.
 *   - Bloom filter is perfect for equality predicates (WHERE id = 42)
 *     even when min/max spans a wide range.
 *
 * Combined, they prune the vast majority of partitions for typical OLAP queries.
 *
 * Uses double-hashing for k independent hash functions (same algorithm as
 * the OLTP BloomFilter in the indexes module, kept self-contained here).
 */
public class OlapBloomFilter {

    private final BitSet bits;
    private final int    m;
    private final int    k;

    public OlapBloomFilter(int expectedElements, double falsePositiveRate) {
        this.m    = optimalM(expectedElements, falsePositiveRate);
        this.k    = optimalK(m, expectedElements);
        this.bits = new BitSet(m);
    }

    public void add(Object value) {
        if (value == null) return;
        int code = value.hashCode();
        for (int i = 0; i < k; i++) bits.set(Math.abs(hash(code, i)) % m);
    }

    /**
     * @return false → value is DEFINITELY NOT in this partition (safe to skip)
     *         true  → value MIGHT be in this partition (must scan)
     */
    public boolean mightContain(Object value) {
        if (value == null) return false;
        int code = value.hashCode();
        for (int i = 0; i < k; i++) {
            if (!bits.get(Math.abs(hash(code, i)) % m)) return false;
        }
        return true;
    }

    private int hash(int key, int seed) {
        int h1 = murmur(key);
        int h2 = fnv1a(key);
        return h1 + seed * h2;
    }

    private static int murmur(int key) {
        key ^= key >>> 16; key *= 0x85ebca6b; key ^= key >>> 13;
        key *= 0xc2b2ae35; key ^= key >>> 16; return key;
    }

    private static int fnv1a(int key) {
        int hash = 0x811c9dc5;
        for (int i = 0; i < 4; i++) { hash ^= (key >> (i * 8)) & 0xFF; hash *= 0x01000193; }
        return hash;
    }

    private static int optimalM(int n, double p) {
        return (int) Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private static int optimalK(int m, int n) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }
}
