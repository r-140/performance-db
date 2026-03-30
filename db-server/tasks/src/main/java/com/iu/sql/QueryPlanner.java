package com.iu.sql;

import com.iu.indexes.IndexTypes;
import com.iu.worker.util.IndexHelper;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.PATH_TO_INDEX_REGISTRY;

/**
 * Rule-based query planner.
 *
 * Selects the cheapest physical plan for a given SELECT statement by checking
 * which indexes currently exist in the index registry file.
 *
 * For WHERE id = N (point lookups on the primary key):
 *   1. Hash index   → HashIndexScan  O(1) hash map, no disk I/O
 *   2. B+ tree      → BPlusTreeScan  O(log N) tree walk
 *   3. LSM tree     → LSMTreeScan    O(1) MemTable or Bloom-filtered SSTable scan
 *   4. No index     → FullScan       O(N) sequential
 *
 * For WHERE field = value (non-key predicate):
 *   5. GIN index    → GINScan        O(log T + k) posting list
 *   6. Bitmap index → BitmapScan     O(cardinality) BitSet ops
 *   7. No index     → FullScan       O(N) with field filter
 *
 * LSM is now in the chain: before it was completely invisible to the SQL layer,
 * meaning `SELECT * FROM data WHERE id = 5` would never benefit from an LSM
 * index even if one existed.
 */
public class QueryPlanner {
    private static final Logger LOGGER = Logger.getLogger(QueryPlanner.class.getName());

    public QueryPlan plan(SqlNode.SelectStatement stmt) {
        if (stmt.where() == null) {
            LOGGER.log(Level.FINE, "No WHERE → FullScan");
            return new QueryPlan.FullScan(null, null);
        }

        SqlNode.EqPredicate pred  = stmt.where().predicate();
        String              field = pred.field();
        String              value = pred.value();

        LOGGER.log(Level.FINE, "Planning WHERE " + field + " = " + value);

        return switch (field) {
            case "id" -> planForId(value);
            default   -> planForField(field, value);
        };
    }

    // -----------------------------------------------------------------------
    // Point-lookup on primary key
    // -----------------------------------------------------------------------

    private QueryPlan planForId(String value) {
        int id;
        try {
            id = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return new QueryPlan.FullScan("id", value);
        }

        if (indexExists(IndexTypes.HASH_INDEX.getIndexType())) {
            LOGGER.info("Plan: HashIndexScan(id=" + id + ")");
            return new QueryPlan.HashIndexScan(id);
        }
        if (indexExists(IndexTypes.BPLUSTREE.getIndexType())) {
            LOGGER.info("Plan: BPlusTreeScan(id=" + id + ")");
            return new QueryPlan.BPlusTreeScan(id);
        }
        if (indexExists(IndexTypes.LSMTREE.getIndexType())) {
            LOGGER.info("Plan: LSMTreeScan(id=" + id
                + ") — Bloom filters skip irrelevant SSTables");
            return new QueryPlan.LSMTreeScan(id);
        }
        LOGGER.info("Plan: FullScan — no id index available");
        return new QueryPlan.FullScan("id", value);
    }

    // -----------------------------------------------------------------------
    // Non-key predicate
    // -----------------------------------------------------------------------

    private QueryPlan planForField(String field, String value) {
        if (indexExists(IndexTypes.GIN.getIndexType())) {
            LOGGER.info("Plan: GINScan(token=" + value + ")");
            return new QueryPlan.GINScan(value);
        }
        if (indexExists(IndexTypes.BITMAP.getIndexType())) {
            LOGGER.info("Plan: BitmapScan(value=" + value + ")");
            return new QueryPlan.BitmapScan(value);
        }
        LOGGER.info("Plan: FullScan(field=" + field + ") — no matching index");
        return new QueryPlan.FullScan(field, value);
    }

    private boolean indexExists(String indexType) {
        try {
            return IndexHelper.checkIndexExistence(PATH_TO_INDEX_REGISTRY, indexType);
        } catch (IOException e) {
            return false;
        }
    }
}
