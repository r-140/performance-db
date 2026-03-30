package com.iu.olap.join;

import com.iu.olap.storage.OlapTable;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * OLAP join strategies — how Snowflake and other MPP databases join large tables.
 *
 * JOIN TYPES IN OLAP vs OLTP
 * ──────────────────────────
 * OLTP (our OLTP db, PostgreSQL, MySQL):
 *   Joins happen on small result sets after index lookups reduce row counts.
 *   Nested-loop join on indexed columns is fast when the inner table is tiny.
 *   e.g. "Find order #42 and its customer" — inner table = 1 row after index.
 *
 * OLAP (Snowflake, Redshift, BigQuery):
 *   Joins happen on large tables (millions/billions of rows).
 *   Index-based nested loop is impractical — we can't build a B+tree over
 *   a 10B-row fact table and probe it row-by-row from the outer table.
 *   Instead:
 *     - Broadcast join: replicate the small dimension table to every node.
 *     - Hash join: partition both tables on the join key and process matching
 *       partitions together (Shuffle join / Redistributed join).
 *     - Sort-merge join: sort both sides then merge — good when already sorted
 *       by a clustered key.
 *
 * THREE STRATEGIES IMPLEMENTED
 * ────────────────────────────
 * 1. BroadcastJoin  — best when one table is small enough to fit in memory.
 *                     In Snowflake the "small" table is typically < 8 MB.
 *                     All compute nodes receive the full small table, then
 *                     each scans its local partition of the large table.
 *
 * 2. HashJoin       — both tables are large. Each table is hashed on the
 *                     join key into buckets. Matching buckets are joined.
 *                     Snowflake calls this "redistribution join".
 *                     Cost: O(N + M), N/M = row counts of outer/inner.
 *
 * 3. SortMergeJoin  — both tables are sorted on the join key (e.g. via a
 *                     clustered key) and merged in one pass.
 *                     Cost: O(N + M) for already-sorted input, O(N log N + M log M) if not.
 *                     Snowflake uses this automatically when tables are pre-sorted.
 */
public class OlapJoin {
    private static final Logger LOGGER = Logger.getLogger(OlapJoin.class.getName());

    // -----------------------------------------------------------------------
    // Strategy 1: Broadcast Join
    // -----------------------------------------------------------------------

    /**
     * Broadcast join — replicate the inner (small) table into a hash map,
     * then probe it for every row of the outer (large) table.
     *
     * WHEN SNOWFLAKE CHOOSES BROADCAST JOIN
     * ──────────────────────────────────────
     * The query optimiser estimates the inner table size. If it fits in
     * node memory (Snowflake default ~8 MB, configurable) it broadcasts.
     * This avoids shuffling the large outer table over the network.
     *
     * Performance:
     *   Build phase:  scan inner table → hash map  O(M)
     *   Probe phase:  scan outer table → lookup    O(N)
     *   Total:        O(N + M),  inner table in memory
     *
     * @param outer       large fact table
     * @param outerKey    join column on outer table
     * @param inner       small dimension table
     * @param innerKey    join column on inner table
     * @param selectCols  columns to include in output (from both tables)
     */
    public List<Map<String, Object>> broadcastJoin(
            OlapTable outer, String outerKey,
            OlapTable inner, String innerKey,
            List<String> selectCols) {

        LOGGER.info("BroadcastJoin: outer=" + outer.tableName()
            + "(" + outer.totalRows() + " rows)"
            + " inner=" + inner.tableName()
            + "(" + inner.totalRows() + " rows)");

        // ── Build phase: load inner table into hash map keyed on innerKey ──
        List<String> innerCols = inner.schema();
        Map<Object, List<Map<String, Object>>> buildSide = new HashMap<>();

        for (Map<String, Object> row : inner.partitions().stream()
                .flatMap(p -> p.scanAll(innerCols).stream()).toList()) {
            Object key = row.get(innerKey);
            buildSide.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        LOGGER.info("BroadcastJoin: build side has " + buildSide.size() + " distinct keys");

        // ── Probe phase: scan outer table, probe the hash map ──
        List<String> outerCols = outer.schema();
        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> outerRow : outer.partitions().stream()
                .flatMap(p -> p.scanAll(outerCols).stream()).toList()) {
            Object joinVal = outerRow.get(outerKey);
            List<Map<String, Object>> matches = buildSide.get(joinVal);
            if (matches == null) continue;

            for (Map<String, Object> innerRow : matches) {
                Map<String, Object> joined = new LinkedHashMap<>();
                for (String col : selectCols) {
                    if (outerRow.containsKey(col)) joined.put(col, outerRow.get(col));
                    else if (innerRow.containsKey(col)) joined.put(col, innerRow.get(col));
                }
                result.add(joined);
            }
        }

        LOGGER.info("BroadcastJoin: produced " + result.size() + " rows");
        return result;
    }

    // -----------------------------------------------------------------------
    // Strategy 2: Hash Join (Redistribute / Shuffle)
    // -----------------------------------------------------------------------

    /**
     * Hash join — partition both tables on the join key then join matching partitions.
     *
     * WHEN SNOWFLAKE CHOOSES HASH JOIN
     * ─────────────────────────────────
     * When neither table is small enough to broadcast. Snowflake hashes
     * both tables on the join key across virtual warehouses (nodes). Each
     * node then runs a broadcast join on its local sub-partitions.
     *
     * This simulation uses a single process with in-memory hash buckets.
     *
     * Performance:
     *   Partition both tables: O(N + M)
     *   Join each bucket pair: O(bucket_N + bucket_M) per bucket
     *   Total: O(N + M) expected
     *
     * @param numBuckets  number of hash buckets (simulates degree of parallelism)
     */
    public List<Map<String, Object>> hashJoin(
            OlapTable left,  String leftKey,
            OlapTable right, String rightKey,
            List<String> selectCols,
            int numBuckets) {

        LOGGER.info("HashJoin: left=" + left.tableName()
            + "(" + left.totalRows() + " rows)"
            + " right=" + right.tableName()
            + "(" + right.totalRows() + " rows)"
            + " buckets=" + numBuckets);

        // ── Partition both tables into buckets by hash of join key ──
        @SuppressWarnings("unchecked")
        List<Map<String, Object>>[] leftBuckets  = new List[numBuckets];
        @SuppressWarnings("unchecked")
        List<Map<String, Object>>[] rightBuckets = new List[numBuckets];
        for (int i = 0; i < numBuckets; i++) {
            leftBuckets[i]  = new ArrayList<>();
            rightBuckets[i] = new ArrayList<>();
        }

        List<String> leftCols  = left.schema();
        List<String> rightCols = right.schema();

        left.partitions().stream()
            .flatMap(p -> p.scanAll(leftCols).stream())
            .forEach(row -> {
                int bucket = bucket(row.get(leftKey), numBuckets);
                leftBuckets[bucket].add(row);
            });

        right.partitions().stream()
            .flatMap(p -> p.scanAll(rightCols).stream())
            .forEach(row -> {
                int bucket = bucket(row.get(rightKey), numBuckets);
                rightBuckets[bucket].add(row);
            });

        // ── Join each matching bucket pair ──
        List<Map<String, Object>> result = new ArrayList<>();
        for (int b = 0; b < numBuckets; b++) {
            // Build phase on right bucket
            Map<Object, List<Map<String, Object>>> buildMap = new HashMap<>();
            for (Map<String, Object> row : rightBuckets[b]) {
                buildMap.computeIfAbsent(row.get(rightKey), k -> new ArrayList<>()).add(row);
            }
            // Probe with left bucket
            for (Map<String, Object> leftRow : leftBuckets[b]) {
                List<Map<String, Object>> matches = buildMap.get(leftRow.get(leftKey));
                if (matches == null) continue;
                for (Map<String, Object> rightRow : matches) {
                    Map<String, Object> joined = new LinkedHashMap<>();
                    for (String col : selectCols) {
                        if (leftRow.containsKey(col))  joined.put(col, leftRow.get(col));
                        else if (rightRow.containsKey(col)) joined.put(col, rightRow.get(col));
                    }
                    result.add(joined);
                }
            }
        }

        LOGGER.info("HashJoin: produced " + result.size() + " rows");
        return result;
    }

    // -----------------------------------------------------------------------
    // Strategy 3: Sort-Merge Join
    // -----------------------------------------------------------------------

    /**
     * Sort-merge join — sort both sides on the join key then merge in one pass.
     *
     * WHEN SNOWFLAKE CHOOSES SORT-MERGE JOIN
     * ─────────────────────────────────────
     * When one or both tables are already sorted on the join key (e.g. via
     * CLUSTER BY) or when the join is on a range predicate (BETWEEN, >=).
     * Avoids the hash table overhead; good for secondary sort requirements.
     *
     * When tables are clustered on the join key, Snowflake's micro-partition
     * min/max metadata means only the relevant partitions are read (pruning)
     * AND the data is pre-sorted — essentially free sort phase.
     *
     * Performance:
     *   Sort: O(N log N + M log M) if unsorted; O(1) if clustered
     *   Merge: O(N + M)
     *
     * @param leftAlreadySorted   true if the left table is clustered on leftKey
     * @param rightAlreadySorted  true if the right table is clustered on rightKey
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Map<String, Object>> sortMergeJoin(
            OlapTable left,  String leftKey,  boolean leftAlreadySorted,
            OlapTable right, String rightKey, boolean rightAlreadySorted,
            List<String> selectCols) {

        LOGGER.info("SortMergeJoin: left=" + left.tableName()
            + "(sorted=" + leftAlreadySorted + ")"
            + " right=" + right.tableName()
            + "(sorted=" + rightAlreadySorted + ")");

        // ── Sort phase (skipped for pre-clustered tables) ──
        List<Map<String, Object>> leftRows = left.partitions().stream()
            .flatMap(p -> p.scanAll(left.schema()).stream()).collect(Collectors.toList());
        List<Map<String, Object>> rightRows = right.partitions().stream()
            .flatMap(p -> p.scanAll(right.schema()).stream()).collect(Collectors.toList());

        if (!leftAlreadySorted) {
            leftRows.sort((a, b) -> ((Comparable) a.get(leftKey)).compareTo(b.get(leftKey)));
            LOGGER.fine("SortMergeJoin: sorted left table");
        }
        if (!rightAlreadySorted) {
            rightRows.sort((a, b) -> ((Comparable) a.get(rightKey)).compareTo(b.get(rightKey)));
            LOGGER.fine("SortMergeJoin: sorted right table");
        }

        // ── Merge phase — two-pointer walk ──
        List<Map<String, Object>> result = new ArrayList<>();
        int li = 0, ri = 0;

        while (li < leftRows.size() && ri < rightRows.size()) {
            Comparable lKey = (Comparable) leftRows.get(li).get(leftKey);
            Comparable rKey = (Comparable) rightRows.get(ri).get(rightKey);

            int cmp = lKey.compareTo(rKey);
            if (cmp < 0) {
                li++;
            } else if (cmp > 0) {
                ri++;
            } else {
                // Keys match — collect all right rows with this key
                int riStart = ri;
                while (ri < rightRows.size()
                        && lKey.compareTo(rightRows.get(ri).get(rightKey)) == 0) {
                    ri++;
                }
                // Emit for every left row with this key
                int liStart = li;
                while (li < leftRows.size()
                        && leftRows.get(li).get(leftKey).equals(lKey)) {
                    for (int r = riStart; r < ri; r++) {
                        Map<String, Object> joined = new LinkedHashMap<>();
                        Map<String, Object> leftRow  = leftRows.get(li);
                        Map<String, Object> rightRow = rightRows.get(r);
                        for (String col : selectCols) {
                            if (leftRow.containsKey(col))  joined.put(col, leftRow.get(col));
                            else if (rightRow.containsKey(col)) joined.put(col, rightRow.get(col));
                        }
                        result.add(joined);
                    }
                    li++;
                }
            }
        }

        LOGGER.info("SortMergeJoin: produced " + result.size() + " rows");
        return result;
    }

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------

    private static int bucket(Object key, int n) {
        return Math.abs(Objects.hashCode(key)) % n;
    }
}
