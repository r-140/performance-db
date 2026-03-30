package com.iu.olap.storage;

import java.util.*;

/**
 * Columnar (column-oriented) storage engine — the foundation of OLAP databases.
 *
 * ROW STORE vs COLUMN STORE
 * ─────────────────────────
 * Row store (OLTP — our existing DB):
 *   Row 0: [id=0, name="Alice", age=30, salary=90000]
 *   Row 1: [id=1, name="Bob",   age=25, salary=70000]
 *   Good for: INSERT/UPDATE one row at a time, SELECT *
 *   Bad for:  SELECT AVG(salary) — must read ALL columns of ALL rows
 *
 * Column store (OLAP — this class):
 *   id:     [0, 1, 2, 3, ...]
 *   name:   ["Alice", "Bob", "Carol", ...]
 *   salary: [90000, 70000, 85000, ...]
 *   Good for: SELECT AVG(salary) — reads ONLY the salary column
 *   Good for: compression (same-type values compress much better)
 *   Good for: vectorised SIMD execution over arrays of same type
 *   Bad for:  UPDATE a single row (must touch N separate column files)
 *
 * SNOWFLAKE MICRO-PARTITION SIMULATION
 * ─────────────────────────────────────
 * Snowflake splits tables into immutable micro-partitions (50–500 MB each).
 * Each micro-partition stores a range of rows for all columns PLUS metadata:
 *   - min and max value per column    → partition pruning
 *   - row count                       → query cost estimation
 *   - distinct value count per column → planner cardinality estimates
 *
 * This class represents one micro-partition.
 */
public class ColumnStore {

    /** Schema — ordered list of column names. */
    private final List<String> columns;

    /** Column data: column name → list of values (index = row number). */
    private final Map<String, List<Object>> data = new LinkedHashMap<>();

    /** Per-column min/max for partition pruning (micro-partition metadata). */
    private final Map<String, Object> colMin = new HashMap<>();
    private final Map<String, Object> colMax = new HashMap<>();

    /** Column whose sort order this partition is clustered on (null = heap order). */
    private String clusteredKey = null;

    private final String partitionId;

    public ColumnStore(String partitionId, List<String> columns) {
        this.partitionId = partitionId;
        this.columns     = List.copyOf(columns);
        for (String col : columns) data.put(col, new ArrayList<>());
    }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /**
     * Append one row. Values must be in schema column order.
     * Updates min/max statistics for partition pruning.
     */
    public void insertRow(List<Object> values) {
        if (values.size() != columns.size())
            throw new IllegalArgumentException(
                "Expected " + columns.size() + " values, got " + values.size());
        for (int i = 0; i < columns.size(); i++) {
            String col = columns.get(i);
            Object val = values.get(i);
            data.get(col).add(val);
            updateStats(col, val);
        }
    }

    /**
     * Sort all rows by the given column and mark this partition as clustered.
     *
     * CLUSTERED KEY EXPLAINED
     * ───────────────────────
     * By default rows arrive in insertion order, scattering values for any
     * given date/id/category across all partitions. With a clustered key, rows
     * with similar key values land in the same micro-partition.
     *
     * Effect: WHERE order_date = '2024-03-01'
     *   Before clustering: planner must scan every partition
     *   After clustering:  only 1–2 partitions contain that date → O(1) pruning
     *
     * Cost: Snowflake runs clustering as a background service. It pays off
     * when queries consistently filter on the same column AND the column has
     * high cardinality (many distinct values → fine-grained pruning).
     * Low-cardinality columns (true/false, status) benefit less because many
     * partitions still span the same value range.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void clusterBy(String keyColumn) {
        if (!data.containsKey(keyColumn))
            throw new IllegalArgumentException("Unknown column: " + keyColumn);
        this.clusteredKey = keyColumn;

        int n = rowCount();
        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < n; i++) order.add(i);
        order.sort((a, b) -> {
            Comparable ca = (Comparable) data.get(keyColumn).get(a);
            Comparable cb = (Comparable) data.get(keyColumn).get(b);
            return ca.compareTo(cb);
        });
        for (String col : columns) {
            List<Object> old    = data.get(col);
            List<Object> sorted = new ArrayList<>(n);
            for (int idx : order) sorted.add(old.get(idx));
            data.put(col, sorted);
        }
        rebuildStats();
    }

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    /**
     * Read all values for one column — zero overhead from other columns.
     * This is the core OLAP advantage: a 100-column table with 1B rows reads
     * only 1/100th of the data for a single-column aggregate.
     */
    public List<Object> readColumn(String column) {
        List<Object> col = data.get(column);
        if (col == null) throw new IllegalArgumentException("Unknown column: " + column);
        return Collections.unmodifiableList(col);
    }

    /** Full row scan over selected columns and row range. */
    public List<Map<String, Object>> scan(List<String> selectColumns, int fromRow, int toRow) {
        List<Map<String, Object>> result = new ArrayList<>();
        int end = Math.min(toRow, rowCount());
        for (int i = fromRow; i < end; i++) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (String col : selectColumns) row.put(col, data.get(col).get(i));
            result.add(row);
        }
        return result;
    }

    /** All rows as maps (for joins and tests). */
    public List<Map<String, Object>> scanAll(List<String> selectColumns) {
        return scan(selectColumns, 0, rowCount());
    }

    // -----------------------------------------------------------------------
    // Partition pruning
    // -----------------------------------------------------------------------

    /**
     * Can this entire partition be skipped for predicate "column = value"?
     *
     * Returns true when value lies outside [min, max] for this column — so
     * no row in this partition can possibly match. The partition is PRUNED
     * and never read from disk.
     *
     * Snowflake example with 1000 partitions and WHERE order_date = '2024-03-01':
     *   Without clustering: 1000 partitions scanned
     *   With clustering on order_date: ~2 partitions scanned (the rest are pruned)
     * → 500x speedup before any query optimisation.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public boolean canPrune(String column, Object value) {
        if (!colMin.containsKey(column)) return false;
        if (!(value instanceof Comparable)) return false;
        Comparable cv  = (Comparable) value;
        Comparable min = (Comparable) colMin.get(column);
        Comparable max = (Comparable) colMax.get(column);
        return cv.compareTo(min) < 0 || cv.compareTo(max) > 0;
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    public int         rowCount()        { return data.isEmpty() ? 0 : data.get(columns.get(0)).size(); }
    public String      partitionId()     { return partitionId; }
    public List<String> columns()        { return columns; }
    public String      clusteredKey()    { return clusteredKey; }
    public Object      colMin(String c)  { return colMin.get(c); }
    public Object      colMax(String c)  { return colMax.get(c); }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void updateStats(String col, Object val) {
        if (!(val instanceof Comparable)) return;
        Comparable cv = (Comparable) val;
        if (!colMin.containsKey(col) || cv.compareTo(colMin.get(col)) < 0) colMin.put(col, val);
        if (!colMax.containsKey(col) || cv.compareTo(colMax.get(col)) > 0) colMax.put(col, val);
    }

    private void rebuildStats() {
        colMin.clear(); colMax.clear();
        for (String col : columns)
            for (Object val : data.get(col)) updateStats(col, val);
    }
}
