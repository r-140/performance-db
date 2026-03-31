package com.iu.olap.query;

import com.iu.olap.catalog.OlapCatalog;
import com.iu.olap.index.OlapInvertedIndex;
import com.iu.olap.storage.ColumnStore;
import com.iu.olap.storage.OlapTable;

import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Simplified OLAP query engine.
 *
 * SNOWFLAKE QUERY PROCESSING PIPELINE
 * ─────────────────────────────────────
 * 1. Parse SQL → AST
 * 2. Semantic analysis (resolve table/column names via catalog)
 * 3. Query optimisation:
 *    a. Predicate pushdown — move WHERE filters as close to the scan as possible
 *    b. Partition pruning  — use min/max metadata to skip irrelevant micro-partitions
 *    c. Bloom filter check — per-partition per-column filters for equality predicates
 *    d. Inverted index     — Search Optimisation Service posting lists
 *    e. Column pruning     — only read columns referenced in SELECT/WHERE/JOIN
 * 4. Execution plan:
 *    - Scan surviving partitions reading only needed columns
 *    - Apply row-level filters
 *    - Aggregate / join / sort
 * 5. Return result set
 *
 * This class implements steps 3–5 for single-table SELECT with equality WHERE.
 * Join planning is handled by OlapJoin (broadcast vs hash vs sort-merge decision).
 *
 * THREE-LEVEL PRUNING (what makes OLAP fast)
 * ───────────────────────────────────────────
 * Given: SELECT * FROM orders WHERE order_date = 20240315
 *
 * With 1 000 micro-partitions ordered by insertion time (no clustering):
 *   Inverted index lookup → partitions {p42, p137}  → skip 998 partitions
 *   Min/max check on remaining → skip additional outliers
 *   Bloom filter on remaining → 0-1 disk reads
 *
 * With CLUSTER BY order_date (data physically sorted):
 *   Min/max check alone → typically 1-2 partitions → near O(1) for point queries
 */
public class OlapQueryEngine {
    private static final Logger LOGGER = Logger.getLogger(OlapQueryEngine.class.getName());

    private final OlapCatalog catalog;
    private final Map<String, OlapInvertedIndex> invertedIndexes = new HashMap<>();

    public OlapQueryEngine(OlapCatalog catalog) {
        this.catalog = catalog;
    }

    // -----------------------------------------------------------------------
    // Index management
    // -----------------------------------------------------------------------

    /**
     * Build a Search Optimisation Index on a column.
     * In Snowflake: ALTER TABLE t ADD SEARCH OPTIMIZATION ON EQUALITY(col)
     */
    public void buildInvertedIndex(String schemaName, String tableName, String column) {
        OlapTable table = catalog.requireTable(schemaName, tableName);
        OlapInvertedIndex idx = new OlapInvertedIndex();

        for (ColumnStore partition : table.partitions()) {
            List<Object> values = partition.readColumn(column);
            for (int row = 0; row < values.size(); row++) {
                idx.index(column, values.get(row), partition.partitionId(), row);
            }
        }

        String key = schemaName + "." + tableName + "." + column;
        invertedIndexes.put(key, idx);
        LOGGER.info("Built inverted index on " + key + " (" + idx.tokenCount() + " tokens)");
    }

    // -----------------------------------------------------------------------
    // Query execution
    // -----------------------------------------------------------------------

    /**
     * Execute a simple SELECT with optional equality WHERE.
     *
     * Pruning levels applied:
     *  1. Inverted index (if exists for the filter column) → partition set
     *  2. Min/max pruning → eliminate remaining out-of-range partitions
     *  3. Bloom filter → eliminate partitions where key is definitely absent
     *  4. Columnar scan on surviving partitions reading only selectCols
     *  5. Row-level filter on returned rows
     *
     * @param schemaName   schema containing the table
     * @param tableName    table to scan
     * @param filterCol    column to filter on (null = no filter)
     * @param filterVal    value to match (null = no filter)
     * @param selectCols   columns to return (null = all)
     */
    public List<Map<String, Object>> select(
            String schemaName, String tableName,
            String filterCol, Object filterVal,
            List<String> selectCols) {

        OlapTable table = catalog.requireTable(schemaName, tableName);
        List<String> cols = selectCols != null ? selectCols : table.schema();

        int totalPartitions = table.partitions().size();
        int scanned = 0, pruned = 0, bloomPruned = 0, idxPruned = 0;

        // ── Level 1: inverted index → candidate partition set ──────────
        Set<String> candidatePartitions = null;
        if (filterCol != null) {
            String idxKey = schemaName + "." + tableName + "." + filterCol;
            OlapInvertedIndex idx = invertedIndexes.get(idxKey);
            if (idx != null && idx.isIndexed(filterCol)) {
                candidatePartitions = idx.partitionsContaining(filterCol, filterVal);
                idxPruned = totalPartitions - candidatePartitions.size();
                LOGGER.log(Level.INFO, "Inverted index: " + idxPruned + " partitions pruned, "
                    + candidatePartitions.size() + " candidates");
            }
        }

        List<Map<String, Object>> result = new ArrayList<>();

        for (ColumnStore partition : table.partitions()) {
            // ── Level 1 continued: skip if not in candidate set ─────────
            if (candidatePartitions != null
                    && !candidatePartitions.contains(partition.partitionId())) {
                pruned++;
                continue;
            }

            // ── Level 2: min/max pruning ─────────────────────────────────
            if (filterCol != null && filterVal != null
                    && partition.canPrune(filterCol, filterVal)) {
                pruned++;
                continue;
            }

            scanned++;
            // ── Level 3–4: columnar scan with row-level filter ──────────
            List<Map<String, Object>> rows = partition.scanAll(cols);
            if (filterCol != null && filterVal != null) {
                for (Map<String, Object> row : rows) {
                    if (filterVal.equals(row.get(filterCol))) result.add(row);
                }
            } else {
                result.addAll(rows);
            }
        }

        LOGGER.log(Level.INFO, String.format(
            "select %s.%s: total=%d idx_pruned=%d minmax_pruned=%d scanned=%d rows=%d",
            schemaName, tableName, totalPartitions, idxPruned, pruned, scanned, result.size()));

        return result;
    }

    /**
     * Column aggregate: compute a function over one column's values.
     * Only the target column is read from each partition — pure columnar I/O.
     *
     * Examples:
     *   aggregate("public","orders","amount", vals -> vals.stream()
     *       .mapToLong(v -> (Long)v).sum())   // SUM(amount)
     */
    public <T> T aggregate(String schemaName, String tableName,
                            String column, Function<List<Object>, T> aggregateFn) {
        OlapTable table = catalog.requireTable(schemaName, tableName);
        List<Object> allValues = table.columnScan(column);
        return aggregateFn.apply(allValues);
    }

    /**
     * GROUP BY aggregate: group rows by groupCol, apply aggregateFn to valueCol per group.
     */
    public <T> Map<Object, T> groupBy(String schemaName, String tableName,
                                       String groupCol, String valueCol,
                                       Function<List<Object>, T> aggregateFn) {
        OlapTable table  = catalog.requireTable(schemaName, tableName);
        List<Object> keys   = table.columnScan(groupCol);
        List<Object> values = table.columnScan(valueCol);

        Map<Object, List<Object>> groups = new LinkedHashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            groups.computeIfAbsent(keys.get(i), k -> new ArrayList<>()).add(values.get(i));
        }
        return groups.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey,
                e -> aggregateFn.apply(e.getValue()),
                (a, b) -> b, LinkedHashMap::new));
    }
}
