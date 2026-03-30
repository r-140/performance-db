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
 * Given a parsed SELECT statement, chooses the cheapest physical plan by
 * checking which indexes currently exist.
 *
 * Decision logic (applied in cost order, cheapest first):
 *
 *  1. No WHERE clause → FullScan(field=null) — fetch all rows
 *  2. WHERE id = N, hash index exists → HashIndexScan (O(1))
 *  3. WHERE id = N, B+ tree exists   → BPlusTreeScan (O(log N))
 *  4. WHERE id = N, no index         → FullScan on id
 *  5. WHERE <field> = <value>, GIN index exists → GINScan (O(log T + k))
 *  6. WHERE <field> = <value>, Bitmap index exists → BitmapScan (O(1) bitset)
 *  7. Fallback                        → FullScan on field
 *
 * Uses pattern-matching instanceof (JEP 394, Java 16) and switch expression
 * for clean, exhaustive dispatch.
 */
public class QueryPlanner {
    private static final Logger LOGGER = Logger.getLogger(QueryPlanner.class.getName());

    public QueryPlan plan(SqlNode.SelectStatement stmt) {
        if (stmt.where() == null) {
            LOGGER.log(Level.FINE, "No WHERE clause → full scan");
            return new QueryPlan.FullScan(null, null);
        }

        SqlNode.EqPredicate pred = stmt.where().predicate();
        String field = pred.field();
        String value = pred.value();

        LOGGER.log(Level.FINE, "Planning for WHERE " + field + " = " + value);

        return switch (field) {
            case "id" -> planForId(value);
            default   -> planForField(field, value);
        };
    }

    private QueryPlan planForId(String value) {
        int id;
        try {
            id = Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return new QueryPlan.FullScan("id", value);
        }

        // Check indexes cheapest first
        if (indexExists(IndexTypes.HASH_INDEX.getIndexType())) {
            LOGGER.info("Planner chose: HashIndexScan(id=" + id + ")");
            return new QueryPlan.HashIndexScan(id);
        }
        if (indexExists(IndexTypes.BPLUSTREE.getIndexType())) {
            LOGGER.info("Planner chose: BPlusTreeScan(id=" + id + ")");
            return new QueryPlan.BPlusTreeScan(id);
        }
        LOGGER.info("Planner chose: FullScan(id=" + id + ") — no suitable index");
        return new QueryPlan.FullScan("id", value);
    }

    private QueryPlan planForField(String field, String value) {
        if (indexExists(IndexTypes.GIN.getIndexType())) {
            LOGGER.info("Planner chose: GINScan(token=" + value + ")");
            return new QueryPlan.GINScan(value);
        }
        if (indexExists(IndexTypes.BITMAP.getIndexType())) {
            LOGGER.info("Planner chose: BitmapScan(value=" + value + ")");
            return new QueryPlan.BitmapScan(value);
        }
        LOGGER.info("Planner chose: FullScan(field=" + field + ") — no suitable index");
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
