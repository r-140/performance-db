package com.iu.indexes.bitmap;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Bitmap Index implementation.
 *
 * A Bitmap Index is optimal for low-cardinality columns (e.g. status, category,
 * boolean flags). Each distinct value gets one {@link BitSet} where
 * bit position N is set if document with id=N has that value.
 *
 * This maps cleanly onto the project's integer id space.  For a given value
 * lookup all matching doc ids is O(1); AND/OR across predicates is O(N/64) using
 * native 64-bit word operations on the BitSet.
 *
 * Disk layout (bitmapindex.dat):
 *   One line per distinct value:  <value>|<base64-encoded BitSet bytes>
 *
 * Limitation: bit positions are doc ids, so the BitSet size grows with the
 * maximum doc id, not the count of documents for a given value.  For this
 * project that is fine (ids are sequential integers).
 */
public class BitmapIndex {
    private static final Logger LOGGER = Logger.getLogger(BitmapIndex.class.getName());

    /** value → BitSet of document ids that have this value */
    private final Map<String, BitSet> bitmaps = new HashMap<>();

    /** id → value mapping so we can update on delete */
    private final Map<Integer, String> idToValue = new HashMap<>();

    private static final String LINE_SEP  = "\\|";
    private static final String FILE_SEP  = "|";

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Insert a document: mark bit docId in the bitmap for the given value.
     * @param docId   integer document id
     * @param value   the indexed column value (e.g. "active", "inactive")
     */
    public void insert(int docId, String value) {
        String v = normalise(value);
        bitmaps.computeIfAbsent(v, k -> new BitSet()).set(docId);
        idToValue.put(docId, v);
    }

    /**
     * Remove a document from the index.
     */
    public void delete(int docId) {
        String v = idToValue.remove(docId);
        if (v != null) {
            BitSet bs = bitmaps.get(v);
            if (bs != null) {
                bs.clear(docId);
                if (bs.isEmpty()) bitmaps.remove(v);
            }
        }
    }

    /**
     * Search for all document ids that have the given value.
     * @return list of matching document ids (never null)
     */
    public List<Integer> search(String value) {
        BitSet bs = bitmaps.get(normalise(value));
        if (bs == null) return Collections.emptyList();
        List<Integer> result = new ArrayList<>();
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
            result.add(i);
        }
        return result;
    }

    /**
     * AND query: returns ids matching ALL of the given values simultaneously.
     * (Useful for multi-column bitmap queries.)
     */
    public List<Integer> searchAnd(List<String> values) {
        BitSet result = null;
        for (String value : values) {
            BitSet bs = bitmaps.get(normalise(value));
            if (bs == null) return Collections.emptyList();
            if (result == null) {
                result = (BitSet) bs.clone();
            } else {
                result.and(bs);
            }
        }
        if (result == null) return Collections.emptyList();
        List<Integer> ids = new ArrayList<>();
        for (int i = result.nextSetBit(0); i >= 0; i = result.nextSetBit(i + 1)) {
            ids.add(i);
        }
        return ids;
    }

    /**
     * OR query: returns ids matching ANY of the given values.
     */
    public List<Integer> searchOr(List<String> values) {
        BitSet result = new BitSet();
        for (String value : values) {
            BitSet bs = bitmaps.get(normalise(value));
            if (bs != null) result.or(bs);
        }
        List<Integer> ids = new ArrayList<>();
        for (int i = result.nextSetBit(0); i >= 0; i = result.nextSetBit(i + 1)) {
            ids.add(i);
        }
        return ids;
    }

    /** Returns the set of distinct indexed values. */
    public Set<String> distinctValues() {
        return Collections.unmodifiableSet(bitmaps.keySet());
    }

    /** Returns the number of docs indexed under a value. */
    public int cardinality(String value) {
        BitSet bs = bitmaps.get(normalise(value));
        return bs != null ? bs.cardinality() : 0;
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    /** Persist the index to disk. */
    public void saveToDisk(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            for (Map.Entry<String, BitSet> entry : bitmaps.entrySet()) {
                byte[] bytes = entry.getValue().toByteArray();
                String encoded = Base64.getEncoder().encodeToString(bytes);
                writer.write(entry.getKey() + FILE_SEP + encoded);
                writer.newLine();
            }
        }
        LOGGER.log(Level.INFO, "BitmapIndex saved to " + filePath);
    }

    /** Load the index from disk (does NOT restore idToValue; used for read-only lookup). */
    public void loadFromDisk(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        if (!Files.exists(path)) return;
        bitmaps.clear();
        idToValue.clear();
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] parts = line.split(LINE_SEP, 2);
                if (parts.length < 2) continue;
                byte[] bytes = Base64.getDecoder().decode(parts[1]);
                bitmaps.put(parts[0], BitSet.valueOf(bytes));
            }
        }
        LOGGER.log(Level.INFO, "BitmapIndex loaded from " + filePath);
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private static String normalise(String value) {
        return value == null ? "" : value.trim().toLowerCase();
    }
}
