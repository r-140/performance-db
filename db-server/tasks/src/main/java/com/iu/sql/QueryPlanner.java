package com.iu.sql;

import com.iu.indexes.IndexTypes;
import com.iu.worker.util.IndexHelper;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.PATH_TO_INDEX_REGISTRY;

/**
 * Rule-based query planner — single-table and join queries.
 *
 * SINGLE-TABLE PLAN SELECTION (cheapest first):
 *   WHERE id = N  → Hash(O1) > B+Tree(O log N) > LSM(O1 amortised) > FullScan
 *   WHERE f = V   → GIN(O log T) > Bitmap(O cardinality) > FullScan
 *   No WHERE      → FullScan
 *
 * JOIN PLAN SELECTION:
 *   Outer plan first (from WHERE / primary key index).
 *   Then for the join:
 *     1. If hint = NESTED_LOOP → NestedLoopJoinPlan
 *     2. If hint = HASH        → HashJoinPlan
 *     3. AUTO: if right table has an index on the join col → IndexJoinPlan (O N×log M)
 *              else if outer result is small               → NestedLoopJoinPlan (O N×M)
 *              else                                        → HashJoinPlan (O N+M)
 *
 * OLTP JOIN SEMANTICS vs OLAP
 * ────────────────────────────
 * OLTP (here): joins happen AFTER index-driven row count reduction.
 *   "Find order #42 and enrich with customer name."
 *   → Index lookup on orders → 1 row → 1 index probe on customers
 *   → Total: 2 index lookups, not a full-table join
 *
 * OLAP (OlapJoin): joins happen on FULL TABLES, millions of rows.
 *   → Broadcast (replicate small dim), Hash (shuffle both), SortMerge (pre-sorted)
 *   → Network-aware, partition-local, vectorised
 */
public class QueryPlanner {
    private static final Logger LOGGER = Logger.getLogger(QueryPlanner.class.getName());

    /** Outer result count threshold below which NLJ beats hash join. */
    private static final int NLJ_THRESHOLD = 100;

    public QueryPlan plan(SqlNode.SelectStatement stmt) {
        QueryPlan outerPlan = planScan(stmt.where());

        if (stmt.join() == null) return outerPlan;

        return planJoin(outerPlan, stmt.join(), outerPlan);
    }

    // -----------------------------------------------------------------------
    // Scan planning
    // -----------------------------------------------------------------------

    private QueryPlan planScan(SqlNode.WhereClause where) {
        if (where == null) return new QueryPlan.FullScan(null, null);

        String field = where.predicate().field();
        String value = where.predicate().value();

        return switch (field) {
            case "id" -> planForId(value);
            default   -> planForField(field, value);
        };
    }

    private QueryPlan planForId(String value) {
        int id;
        try { id = Integer.parseInt(value); }
        catch (NumberFormatException e) { return new QueryPlan.FullScan("id", value); }

        if (indexExists(IndexTypes.HASH_INDEX.getIndexType())) {
            LOGGER.info("Plan: HashIndexScan(id=" + id + ")");
            return new QueryPlan.HashIndexScan(id);
        }
        if (indexExists(IndexTypes.BPLUSTREE.getIndexType())) {
            LOGGER.info("Plan: BPlusTreeScan(id=" + id + ")");
            return new QueryPlan.BPlusTreeScan(id);
        }
        if (indexExists(IndexTypes.LSMTREE.getIndexType())) {
            LOGGER.info("Plan: LSMTreeScan(id=" + id + ")");
            return new QueryPlan.LSMTreeScan(id);
        }
        LOGGER.info("Plan: FullScan(id)");
        return new QueryPlan.FullScan("id", value);
    }

    private QueryPlan planForField(String field, String value) {
        if (indexExists(IndexTypes.GIN.getIndexType())) {
            LOGGER.info("Plan: GINScan(" + field + "=" + value + ")");
            return new QueryPlan.GINScan(value);
        }
        if (indexExists(IndexTypes.BITMAP.getIndexType())) {
            LOGGER.info("Plan: BitmapScan(" + value + ")");
            return new QueryPlan.BitmapScan(value);
        }
        LOGGER.info("Plan: FullScan(" + field + ")");
        return new QueryPlan.FullScan(field, value);
    }

    // -----------------------------------------------------------------------
    // Join planning
    // -----------------------------------------------------------------------

    private QueryPlan planJoin(QueryPlan outerPlan, SqlNode.JoinClause join,
                                QueryPlan contextPlan) {
        String rightTable  = join.rightTable();
        String outerJoinCol = join.leftCol();
        String innerJoinCol = join.rightCol();

        // Explicit hint overrides the cost model
        if (join.joinType() == SqlNode.JoinType.NESTED_LOOP) {
            LOGGER.info("Join hint: NESTED_LOOP on " + rightTable);
            return new QueryPlan.NestedLoopJoinPlan(outerPlan, rightTable, outerJoinCol, innerJoinCol);
        }
        if (join.joinType() == SqlNode.JoinType.HASH) {
            LOGGER.info("Join hint: HASH on " + rightTable);
            return new QueryPlan.HashJoinPlan(outerPlan, rightTable, outerJoinCol, innerJoinCol);
        }

        // AUTO: check if right table has a usable index on the join column
        if ("id".equals(innerJoinCol)) {
            if (indexExists(IndexTypes.HASH_INDEX.getIndexType())) {
                LOGGER.info("Join: IndexJoinPlan (hash index on " + rightTable + "." + innerJoinCol + ")");
                return new QueryPlan.IndexJoinPlan(outerPlan, rightTable, outerJoinCol,
                    innerJoinCol, IndexTypes.HASH_INDEX.getIndexType());
            }
            if (indexExists(IndexTypes.BPLUSTREE.getIndexType())) {
                LOGGER.info("Join: IndexJoinPlan (B+tree on " + rightTable + "." + innerJoinCol + ")");
                return new QueryPlan.IndexJoinPlan(outerPlan, rightTable, outerJoinCol,
                    innerJoinCol, IndexTypes.BPLUSTREE.getIndexType());
            }
        }

        // No index available — choose NLJ for small outer, hash join otherwise
        boolean outerIsSmall = outerPlan instanceof QueryPlan.HashIndexScan
                            || outerPlan instanceof QueryPlan.BPlusTreeScan
                            || outerPlan instanceof QueryPlan.LSMTreeScan;

        if (outerIsSmall) {
            LOGGER.info("Join: NestedLoopJoin (outer is index scan → small result)");
            return new QueryPlan.NestedLoopJoinPlan(outerPlan, rightTable, outerJoinCol, innerJoinCol);
        }

        LOGGER.info("Join: HashJoinPlan (no index, large outer)");
        return new QueryPlan.HashJoinPlan(outerPlan, rightTable, outerJoinCol, innerJoinCol);
    }

    private boolean indexExists(String indexType) {
        try {
            return IndexHelper.checkIndexExistence(PATH_TO_INDEX_REGISTRY, indexType);
        } catch (IOException e) {
            return false;
        }
    }
}
