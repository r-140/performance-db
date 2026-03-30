package com.iu.sql;

/**
 * Physical execution plan chosen by the planner.
 *
 * Sealed + records (Java 17/21): the executor uses a switch expression over
 * this hierarchy so adding a new plan type is a compile error until handled.
 *
 * Plans, in order of preference (cheapest first):
 *   HashIndexScan  — O(1), best for exact id lookup when hash index exists
 *   BPlusTreeScan  — O(log N), best for id or range when B+ index exists
 *   GINScan        — O(log T + k), best for non-id field lookup via GIN tokens
 *   BitmapScan     — O(1), best for low-cardinality value lookup
 *   FullScan       — O(N), fallback when no index matches the predicate
 */
public sealed interface QueryPlan
        permits QueryPlan.HashIndexScan,
                QueryPlan.BPlusTreeScan,
                QueryPlan.GINScan,
                QueryPlan.BitmapScan,
                QueryPlan.FullScan {

    record HashIndexScan(int id)               implements QueryPlan {}
    record BPlusTreeScan(int id)               implements QueryPlan {}
    record GINScan(String token)               implements QueryPlan {}
    record BitmapScan(String value)            implements QueryPlan {}
    record FullScan(String field, String value) implements QueryPlan {} // field=null → fetch all
}
