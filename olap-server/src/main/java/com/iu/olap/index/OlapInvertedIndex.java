package com.iu.olap.index;

import java.util.*;

/**
 * Column-level inverted index for OLAP text search.
 *
 * HOW SNOWFLAKE HANDLES TEXT SEARCH
 * ───────────────────────────────────
 * Snowflake does not have a traditional B+Tree index (there are no manual
 * CREATE INDEX statements). Instead it provides two mechanisms for fast
 * text lookup inside micro-partitions:
 *
 *  1. Search Optimisation Service (SOS) — builds a per-column inverted
 *     index (posting list) across all micro-partitions, stored separately
 *     from the table data. Activated with:
 *       ALTER TABLE t ADD SEARCH OPTIMIZATION ON EQUALITY(column)
 *
 *  2. Full-text indexing via SEARCH predicates (Snowflake Cortex, 2024+).
 *
 * This class simulates the SOS posting-list structure:
 *   token → { (partitionId, rowIndex), ... }
 *
 * At query time: find token → get partition set → scan only those partitions
 * instead of all partitions.  Combined with min/max pruning and Bloom filters,
 * the optimizer can skip > 99% of partitions for selective point-lookup queries.
 *
 * COMPARISON WITH OLTP GIN INDEX
 * ────────────────────────────────
 * OLTP GIN (db-server/indexes/gin/GINIndex.java):
 *   token → [ file_offset, file_offset, ... ]   (direct data-file pointers)
 *   Used for: fast exact-match lookups in a row-oriented data file
 *
 * OLAP Inverted Index (this class):
 *   token → [ (partition_id, row_index), ... ]   (partition + row pointers)
 *   Used for: locating micro-partitions to scan; the actual data still lives
 *   in the columnar ColumnStore. Row-level pointers are optional extras.
 *
 * The fundamental difference: OLTP inverted index points to byte offsets in
 * a flat file; OLAP inverted index points to partitions (logical data units).
 */
public class OlapInvertedIndex {

    /** Location of a matching row: which partition and which row within it. */
    public record RowLocation(String partitionId, int rowIndex) {}

    /** Posting list: token → all row locations containing that token. */
    private final Map<String, List<RowLocation>> postingLists = new TreeMap<>();

    /** Which columns this index covers. */
    private final Set<String> indexedColumns = new HashSet<>();

    // -----------------------------------------------------------------------
    // Build the index
    // -----------------------------------------------------------------------

    /**
     * Index a single cell value — called once per (column, row) during table load.
     *
     * @param column      column name being indexed
     * @param value       the cell value (tokenised if text)
     * @param partitionId micro-partition id
     * @param rowIndex    row within the partition
     */
    public void index(String column, Object value, String partitionId, int rowIndex) {
        if (value == null) return;
        indexedColumns.add(column);
        RowLocation loc = new RowLocation(partitionId, rowIndex);

        for (String token : tokenize(column, value)) {
            postingLists.computeIfAbsent(token, k -> new ArrayList<>()).add(loc);
        }
    }

    // -----------------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------------

    /**
     * Find all partition IDs that contain a given value in a given column.
     *
     * The returned set is the partition pruning hint: the query engine only
     * needs to scan these partitions. All others are guaranteed absent.
     * (No false negatives — inverted indexes are exact, unlike Bloom filters.)
     *
     * @return set of partition IDs to scan (empty = value not present anywhere)
     */
    public Set<String> partitionsContaining(String column, Object value) {
        Set<String> partitions = new HashSet<>();
        for (String token : tokenize(column, value)) {
            List<RowLocation> locs = postingLists.get(token);
            if (locs != null) locs.forEach(loc -> partitions.add(loc.partitionId()));
        }
        return partitions;
    }

    /**
     * Exact row locations for a given column and value.
     * Used when the query engine wants to fetch specific rows rather than
     * scanning an entire partition.
     */
    public List<RowLocation> rowLocations(String column, Object value) {
        List<RowLocation> result = new ArrayList<>();
        for (String token : tokenize(column, value)) {
            List<RowLocation> locs = postingLists.get(token);
            if (locs != null) result.addAll(locs);
        }
        return result;
    }

    /** True if this column has been indexed. */
    public boolean isIndexed(String column) { return indexedColumns.contains(column); }

    /** Number of distinct tokens in the index. */
    public int tokenCount() { return postingLists.size(); }

    // -----------------------------------------------------------------------
    // Tokenization
    // -----------------------------------------------------------------------

    /**
     * Tokenize a value for a given column.
     * Numeric columns produce a single token (the string representation).
     * String columns produce lowercase words split on whitespace/punctuation.
     */
    private static List<String> tokenize(String column, Object value) {
        String s = String.valueOf(value).toLowerCase().trim();
        if (s.matches("-?\\d+(\\.\\d+)?")) {
            return List.of(column + ":" + s); // prefix with column to avoid cross-column collisions
        }
        String[] parts = s.split("[\\s,;!?.]+");
        List<String> tokens = new ArrayList<>();
        for (String p : parts) {
            if (!p.isBlank()) tokens.add(column + ":" + p);
        }
        return tokens;
    }
}
