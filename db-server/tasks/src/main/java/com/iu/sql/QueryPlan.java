package com.iu.sql;

/**
 * Physical execution plan — sealed hierarchy.
 *
 * Plans in preference order (cheapest first for point lookups):
 *
 *   HashIndexScan  O(1)          — ConcurrentHashMap lookup, no disk I/O
 *   BPlusTreeScan  O(log N)      — tree walk, disk-based nodes
 *   LSMTreeScan    O(1) amortised— MemTable hit O(log M); SSTable with Bloom filter
 *   GINScan        O(log T + k)  — inverted index posting list
 *   BitmapScan     O(cardinality)— BitSet AND/OR
 *   FullScan       O(N)          — sequential file scan, no index
 *
 * Note on LSM vs B+Tree order:
 *   LSM is O(1) when the key is in the MemTable (recent writes).
 *   LSM can be O(L × k) Bloom-filter ops for older keys (L SSTables).
 *   B+Tree is always O(log N) regardless of recency.
 *   The planner prefers LSM after B+Tree because LSM's write path is cheaper
 *   and the Bloom filter makes reads competitive for write-heavy workloads.
 */
public sealed interface QueryPlan
        permits QueryPlan.HashIndexScan,
                QueryPlan.BPlusTreeScan,
                QueryPlan.LSMTreeScan,
                QueryPlan.GINScan,
                QueryPlan.BitmapScan,
                QueryPlan.FullScan {

    record HashIndexScan(int id)                implements QueryPlan {}
    record BPlusTreeScan(int id)                implements QueryPlan {}
    record LSMTreeScan(int id)                  implements QueryPlan {}
    record GINScan(String token)                implements QueryPlan {}
    record BitmapScan(String value)             implements QueryPlan {}
    record FullScan(String field, String value) implements QueryPlan {}
}
