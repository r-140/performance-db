package com.iu.olap.join;

import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * OLTP join strategies — used when row counts are small after index filtering.
 *
 * OLTP JOIN PHILOSOPHY
 * ────────────────────
 * In OLTP workloads, a query like:
 *   SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id
 *   WHERE o.id = 42
 * first uses an index to find order #42 (1 row), then does a single index
 * probe on customers for that customer_id. Total work: ~2 index lookups.
 *
 * The join algorithm matters only when the outer result set is large.
 * For OLTP it almost never is — if it is, you're probably running an
 * OLAP-style query on an OLTP system, which is why read replicas and
 * data warehouses exist.
 *
 * THREE OLTP JOIN ALGORITHMS
 * ──────────────────────────
 *
 * 1. Nested-Loop Join (NLJ)
 *    The simplest join: for every outer row, scan the entire inner table.
 *    Cost: O(N × M). Only acceptable when M is tiny (e.g. a lookup table
 *    with 10 rows) or when N is 1 (single row from index lookup).
 *
 *    Used by: PostgreSQL when the inner table is very small or no index exists.
 *
 * 2. Index Nested-Loop Join (INLJ)
 *    For every outer row, do an index lookup on the inner table instead of
 *    a full scan. Cost: O(N × log M). The dominant OLTP join pattern.
 *    Requires an index on the inner join column.
 *
 *    Used by: PostgreSQL, MySQL InnoDB on foreign key columns (usually indexed).
 *    Our OLTP DB uses this when B+Tree or Hash index exists on the join column.
 *
 * 3. Hash Join (in-memory)
 *    Build a hash map from the smaller table, then probe it for each row of
 *    the larger table. Cost: O(N + M). Better than NLJ for medium-sized tables.
 *    Used when no index is available and tables are too large for NLJ.
 *
 *    Used by: PostgreSQL for medium-sized joins without usable indexes.
 *
 * OLTP vs OLAP JOIN COMPARISON
 * ─────────────────────────────
 * | Feature           | OLTP (this class)      | OLAP (OlapJoin)            |
 * |-------------------|------------------------|----------------------------|
 * | Typical N         | 1–1000 rows            | millions–billions           |
 * | Index usage       | Essential (INLJ)       | Indexes rarely exist on facts|
 * | Data layout       | Row-oriented           | Column-oriented             |
 * | Parallelism       | Single-threaded        | Massively parallel          |
 * | Preferred algo    | INLJ > Hash > NLJ      | Broadcast > Hash > SortMerge|
 * | Memory requirement| Low (index, not data)  | High (hash partitions)      |
 * | Network cost      | N/A (single node)      | Shuffle dominates in MPP    |
 */
public class OltpJoin {
    private static final Logger LOGGER = Logger.getLogger(OltpJoin.class.getName());

    // -----------------------------------------------------------------------
    // 1. Nested-Loop Join — O(N × M)
    // -----------------------------------------------------------------------

    /**
     * Classic nested-loop join: for each outer row, scan all inner rows.
     *
     * This is the "last resort" join in OLTP — only acceptable when:
     *  - Inner table has very few rows (< ~100), OR
     *  - Outer result is a single row (index lookup produced 1 result)
     *
     * In our OLTP DB after `SELECT * FROM data WHERE id = 42` returns 1 row,
     * a NLJ against a 10-row lookup table is perfectly fine: 1 × 10 = 10 ops.
     *
     * @param outer       outer table rows (result of a previous query/scan)
     * @param outerKey    join column name in outer rows
     * @param inner       inner table rows (the "right" side)
     * @param innerKey    join column name in inner rows
     * @param selectCols  output column list
     */
    public List<Map<String, Object>> nestedLoopJoin(
            List<Map<String, Object>> outer, String outerKey,
            List<Map<String, Object>> inner, String innerKey,
            List<String> selectCols) {

        LOGGER.info(String.format("NestedLoopJoin: outer=%d inner=%d → O(%d)",
            outer.size(), inner.size(), outer.size() * inner.size()));

        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> outerRow : outer) {
            Object joinVal = outerRow.get(outerKey);
            for (Map<String, Object> innerRow : inner) {
                if (Objects.equals(joinVal, innerRow.get(innerKey))) {
                    result.add(mergeRow(outerRow, innerRow, selectCols));
                }
            }
        }
        LOGGER.info("NestedLoopJoin: produced " + result.size() + " rows");
        return result;
    }

    // -----------------------------------------------------------------------
    // 2. Index Nested-Loop Join — O(N × log M)
    // -----------------------------------------------------------------------

    /**
     * Index nested-loop join: for each outer row, do an O(1) or O(log M)
     * index lookup instead of scanning all inner rows.
     *
     * This is the PRIMARY join algorithm in OLTP systems when:
     *  - The inner join column has an index (foreign keys are usually indexed)
     *  - The outer result set is not enormous
     *
     * In our OLTP DB this corresponds to the QueryExecutor using a HashIndexScan
     * or BPlusTreeScan plan for each outer row's join key value.
     *
     * The indexFn parameter simulates the index lookup — in production this
     * would call IndexTypes.findAddrInIndex() then FileHelper.findLineByOffset().
     *
     * @param outer      outer rows
     * @param outerKey   outer join column
     * @param indexFn    index lookup function: joinValue → matching inner rows
     * @param selectCols output columns
     */
    public List<Map<String, Object>> indexNestedLoopJoin(
            List<Map<String, Object>> outer, String outerKey,
            Function<Object, List<Map<String, Object>>> indexFn,
            List<String> selectCols) {

        LOGGER.info("IndexNestedLoopJoin: outer=" + outer.size()
            + " → O(N × log M) with index probe");

        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> outerRow : outer) {
            Object joinVal = outerRow.get(outerKey);
            List<Map<String, Object>> innerMatches = indexFn.apply(joinVal);
            for (Map<String, Object> innerRow : innerMatches) {
                result.add(mergeRow(outerRow, innerRow, selectCols));
            }
        }
        LOGGER.info("IndexNestedLoopJoin: produced " + result.size() + " rows");
        return result;
    }

    // -----------------------------------------------------------------------
    // 3. In-Memory Hash Join — O(N + M)
    // -----------------------------------------------------------------------

    /**
     * In-memory hash join: build hash map from smaller table, probe with larger.
     *
     * Used in OLTP when:
     *  - No index on the inner join column
     *  - Tables are medium-sized (both fit in memory)
     *  - NLJ cost O(N × M) is prohibitive
     *
     * PostgreSQL calls this a "Hash Join" and uses it when the planner
     * estimates the NLJ cost exceeds the hash join cost (based on row counts
     * and memory availability).
     *
     * Key difference from OLAP hash join: this version works on already-fetched
     * row sets (post-index-scan results), not on full table scans. The OLAP
     * version partitions across virtual warehouses; this one is single-node.
     *
     * @param build       smaller table rows (becomes the hash map)
     * @param buildKey    join column in build table
     * @param probe       larger table rows (probes the hash map)
     * @param probeKey    join column in probe table
     * @param selectCols  output columns
     */
    public List<Map<String, Object>> hashJoin(
            List<Map<String, Object>> build, String buildKey,
            List<Map<String, Object>> probe, String probeKey,
            List<String> selectCols) {

        LOGGER.info(String.format("HashJoin: build=%d probe=%d → O(N+M)",
            build.size(), probe.size()));

        // Build phase: hash map from build table
        Map<Object, List<Map<String, Object>>> buildMap = new HashMap<>();
        for (Map<String, Object> row : build) {
            buildMap.computeIfAbsent(row.get(buildKey), k -> new ArrayList<>()).add(row);
        }

        // Probe phase
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map<String, Object> probeRow : probe) {
            List<Map<String, Object>> matches = buildMap.get(probeRow.get(probeKey));
            if (matches == null) continue;
            for (Map<String, Object> buildRow : matches) {
                result.add(mergeRow(probeRow, buildRow, selectCols));
            }
        }
        LOGGER.info("HashJoin: produced " + result.size() + " rows");
        return result;
    }

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------

    private Map<String, Object> mergeRow(Map<String, Object> left,
                                          Map<String, Object> right,
                                          List<String> cols) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (String col : cols) {
            if (left.containsKey(col))       row.put(col, left.get(col));
            else if (right.containsKey(col)) row.put(col, right.get(col));
        }
        return row;
    }
}
