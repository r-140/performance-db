package com.iu.sql;

/**
 * Physical execution plan — sealed hierarchy (Java 17/21).
 *
 * Plans for single-table scans:
 *   HashIndexScan, BPlusTreeScan, LSMTreeScan, GINScan, BitmapScan, FullScan
 *
 * Plans for joins (wraps a scan plan + join strategy):
 *   HashJoinPlan        — build hash map from right table, probe with left
 *   NestedLoopJoinPlan  — for each left row, scan/probe right table
 *   IndexJoinPlan       — for each left row, index-lookup into right table
 */
public sealed interface QueryPlan
        permits QueryPlan.HashIndexScan,
                QueryPlan.BPlusTreeScan,
                QueryPlan.LSMTreeScan,
                QueryPlan.GINScan,
                QueryPlan.BitmapScan,
                QueryPlan.FullScan,
                QueryPlan.HashJoinPlan,
                QueryPlan.NestedLoopJoinPlan,
                QueryPlan.IndexJoinPlan {

    // ── Single-table plans ──────────────────────────────────────────────────
    record HashIndexScan(int id)                implements QueryPlan {}
    record BPlusTreeScan(int id)                implements QueryPlan {}
    record LSMTreeScan(int id)                  implements QueryPlan {}
    record GINScan(String token)                implements QueryPlan {}
    record BitmapScan(String value)             implements QueryPlan {}
    record FullScan(String field, String value)  implements QueryPlan {}

    // ── Join plans ──────────────────────────────────────────────────────────

    /**
     * Hash join:
     *   outer scan via outerPlan → build rows
     *   inner = full scan of rightTable
     *   hash map on innerJoinCol, probe with outerJoinCol
     *
     * Best when: no index on the join column, moderate table sizes.
     * Cost: O(N + M)
     */
    record HashJoinPlan(
            QueryPlan outerPlan,
            String    rightTable,
            String    outerJoinCol,
            String    innerJoinCol
    ) implements QueryPlan {}

    /**
     * Nested-loop join:
     *   outer scan → for each row, full scan of rightTable
     *
     * Best when: outer result is tiny (1–few rows after index lookup).
     * Cost: O(N × M) — only acceptable for very small N.
     */
    record NestedLoopJoinPlan(
            QueryPlan outerPlan,
            String    rightTable,
            String    outerJoinCol,
            String    innerJoinCol
    ) implements QueryPlan {}

    /**
     * Index nested-loop join:
     *   outer scan → for each row, index lookup into rightTable
     *
     * Best when: rightTable has a hash/bplustree index on the join column.
     * Cost: O(N × log M)
     */
    record IndexJoinPlan(
            QueryPlan outerPlan,
            String    rightTable,
            String    outerJoinCol,
            String    innerJoinCol,
            String    innerIndexType   // which index to use on the right table
    ) implements QueryPlan {}
}
