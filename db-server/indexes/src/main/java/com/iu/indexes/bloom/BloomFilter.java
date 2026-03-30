package com.iu.indexes.bloom;

import java.util.BitSet;

/**
 * Bloom filter — probabilistic set membership.
 *
 * HOW A BLOOM FILTER WORKS
 * ========================
 * A Bloom filter answers "is this key in the set?" with two possible answers:
 *   - "Definitely NOT in the set"  (no false negatives)
 *   - "Probably in the set"        (small false positive rate, e.g. 1%)
 *
 * Internally it is a BitSet of m bits and k independent hash functions.
 *
 * add(key):    compute k hash values → set bits[h1], bits[h2], …, bits[hk]
 * mightContain(key): compute same k hashes → return true if ALL bits are set
 *                    if ANY bit is 0: key is definitely absent
 *
 * Example (m=8 bits, k=3 hash functions):
 *
 *   After add("hello"):  bits = 00101001  (bits 0, 3, 5 set)
 *   After add("world"):  bits = 10111001  (bits 0, 3, 4, 5, 7 set)
 *   mightContain("hello") → bits 0,3,5 all set? → YES (probably present)
 *   mightContain("nope")  → bit 2 not set? → NO (definitely absent)
 *
 * WHY LSM TREES NEED BLOOM FILTERS
 * In an LSM Tree get(key) scans SSTables newest-first. Without a Bloom
 * filter, every SSTable file must be opened and binary-searched — O(N)
 * I/O on a cache miss. With a Bloom filter, each SSTable holds a filter
 * for its keys. get(key) checks the filter in O(k) bitset ops; only files
 * where the filter says "probably present" are actually read.
 *
 * This reduces I/O from O(SSTables) to O(1) amortised for keys that
 * don't exist in older SSTables — the common case after many writes.
 *
 * Optimal parameters: m = -n*ln(p) / (ln 2)², k = (m/n) * ln 2
 * where n = expected element count, p = desired false-positive rate.
 */
public class BloomFilter {

    private final BitSet  bits;
    private final int     m;          // number of bits
    private final int     k;          // number of hash functions
    private       int     count = 0;  // elements added

    /**
     * Create a Bloom filter sized for expectedElements at the given
     * false-positive rate.
     *
     * @param expectedElements   expected number of distinct keys
     * @param falsePositiveRate  desired false-positive rate (0.0–1.0), e.g. 0.01
     */
    public BloomFilter(int expectedElements, double falsePositiveRate) {
        this.m = optimalBits(expectedElements, falsePositiveRate);
        this.k = optimalHashes(m, expectedElements);
        this.bits = new BitSet(m);
    }

    /** Deserialisation constructor. */
    public BloomFilter(BitSet bits, int m, int k) {
        this.bits = bits; this.m = m; this.k = k;
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    public void add(int key) {
        for (int i = 0; i < k; i++) {
            bits.set(Math.abs(hash(key, i)) % m);
        }
        count++;
    }

    /**
     * @return false → key is DEFINITELY NOT in the set (safe to skip SSTable)
     *         true  → key is PROBABLY in the set (must check SSTable)
     */
    public boolean mightContain(int key) {
        for (int i = 0; i < k; i++) {
            if (!bits.get(Math.abs(hash(key, i)) % m)) return false;
        }
        return true;
    }

    public int count()   { return count; }
    public int bitCount(){ return m; }
    public int hashFunctions() { return k; }
    public BitSet rawBits()    { return (BitSet) bits.clone(); }

    /** Estimated false positive rate given current element count. */
    public double estimatedFalsePositiveRate() {
        double exponent = -(double) k * count / m;
        return Math.pow(1 - Math.exp(exponent), k);
    }

    // -----------------------------------------------------------------------
    // Hash functions — double-hashing technique (Kirsch & Mitzenmacher)
    // Uses two base hashes to simulate k independent hash functions cheaply.
    // -----------------------------------------------------------------------

    private int hash(int key, int i) {
        int h1 = murmur(key);
        int h2 = fnv1a(key);
        return h1 + i * h2;
    }

    private static int murmur(int key) {
        key ^= key >>> 16;
        key *= 0x85ebca6b;
        key ^= key >>> 13;
        key *= 0xc2b2ae35;
        key ^= key >>> 16;
        return key;
    }

    private static int fnv1a(int key) {
        int hash = 0x811c9dc5;
        for (int i = 0; i < 4; i++) {
            hash ^= (key >> (i * 8)) & 0xFF;
            hash *= 0x01000193;
        }
        return hash;
    }

    // -----------------------------------------------------------------------
    // Optimal parameter formulas
    // -----------------------------------------------------------------------

    private static int optimalBits(int n, double p) {
        return (int) Math.ceil(-n * Math.log(p) / (Math.log(2) * Math.log(2)));
    }

    private static int optimalHashes(int m, int n) {
        return Math.max(1, (int) Math.round((double) m / n * Math.log(2)));
    }
}
