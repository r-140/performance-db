package com.iu.sql;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.iu.indexes.IndexKeeper;
import com.iu.indexes.bitmap.BitmapIndex;
import com.iu.indexes.gin.GINIndex;
import com.iu.indexes.lsmtree.LSMTreeIndex;
import com.util.CommonConsts;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.*;

/**
 * SQL executor — handles single-table scans AND joins.
 *
 * OLTP JOIN EXECUTION
 * ────────────────────
 * All three join strategies map to the algorithms in OltpJoin,
 * but are implemented inline here to avoid circular dependencies
 * between the tasks and olap-server modules.
 *
 * Hash join (HashJoinPlan):
 *   1. Execute outer plan → list of matching lines
 *   2. Full scan right table → all lines
 *   3. Build hash map: rightJoinValue → rightLines
 *   4. Probe: for each outer line, look up joinValue in hash map
 *
 * Nested-loop join (NestedLoopJoinPlan):
 *   1. Execute outer plan → list of matching lines
 *   2. For each outer line, full-scan right table and test the join condition
 *   Note: "right table" in our single-file DB is always "data" — this is
 *   a self-join, or the right side is a small in-memory lookup.
 *
 * Index join (IndexJoinPlan):
 *   1. Execute outer plan → list of matching lines
 *   2. For each outer line, extract the join column value
 *   3. Use the specified index to probe the right table directly
 */
public class QueryExecutor {
    private static final Logger LOGGER = Logger.getLogger(QueryExecutor.class.getName());

    private final String dataFile = PATH_TO_DATA_FILE;

    public List<String> execute(QueryPlan plan, int limit) throws IOException {
        List<String> rows = executeInner(plan);
        return applyLimit(rows, limit);
    }

    // -----------------------------------------------------------------------
    // Main dispatch — exhaustive switch over sealed QueryPlan
    // -----------------------------------------------------------------------

    private List<String> executeInner(QueryPlan plan) throws IOException {
        return switch (plan) {

            case QueryPlan.HashIndexScan s -> {
                Object raw = IndexTypes.HASH_INDEX.findAddrInIndex(s.id());
                yield offsetToRows(raw instanceof Long l ? l : null);
            }

            case QueryPlan.BPlusTreeScan s -> {
                Long off = (Long) IndexTypes.BPLUSTREE.findAddrInIndex(s.id());
                yield offsetToRows(off);
            }

            case QueryPlan.LSMTreeScan s -> {
                String lsmFile = DISC_PATH + "/" + IndexTypes.LSMTREE.getIndexFileName();
                LSMTreeIndex lsm = IndexKeeper.INSTANCE.getLsmTreeIndexes().get(lsmFile);
                if (lsm == null) yield List.of();
                Long off = lsm.get(s.id());
                LOGGER.log(Level.FINE, "LSMScan id=" + s.id() + " offset=" + off
                    + " bloom_hits=" + lsm.bloomFilterHits());
                yield offsetToRows(off);
            }

            case QueryPlan.GINScan s -> {
                String ginFile = DISC_PATH + "/" + IndexTypes.GIN.getIndexFileName();
                GINIndex gin = IndexKeeper.INSTANCE.getGINIndexes().get(ginFile);
                yield gin == null ? List.of() : offsetsToRows(gin.search(s.token()));
            }

            case QueryPlan.BitmapScan s -> {
                String bmpFile = DISC_PATH + "/" + IndexTypes.BITMAP.getIndexFileName();
                BitmapIndex bmp = IndexKeeper.INSTANCE.getBitmapIndexes().get(bmpFile);
                yield bmp == null ? List.of() : idsToRows(bmp.search(s.value()));
            }

            case QueryPlan.FullScan s -> {
                var allOffsets = FileHelper.readFile(dataFile, false);
                var result     = new ArrayList<String>();
                for (long off : allOffsets.values()) {
                    String line = FileHelper.findLineByOffset(dataFile, off);
                    if (line == null) continue;
                    if (s.field() == null || matchesField(line, s.field(), s.value()))
                        result.add(line);
                }
                yield result;
            }

            // ── Join plans ─────────────────────────────────────────────────

            case QueryPlan.HashJoinPlan j -> executeHashJoin(j);

            case QueryPlan.NestedLoopJoinPlan j -> executeNestedLoopJoin(j);

            case QueryPlan.IndexJoinPlan j -> executeIndexJoin(j);
        };
    }

    // -----------------------------------------------------------------------
    // Hash join — O(N + M)
    // -----------------------------------------------------------------------

    /**
     * Build hash map from the right (inner) table, probe with outer rows.
     *
     * In a single-file OLTP DB the "right table" is also the data file.
     * This is effectively a self-join or an enrichment join where the right
     * side is filtered by a different condition (simulated here as full scan).
     *
     * OLTP use case: outer rows come from a fast index scan (small N).
     * The hash map over the right table is built once — O(M).
     * Each outer row probes the map — O(1) per probe → total O(N + M).
     */
    private List<String> executeHashJoin(QueryPlan.HashJoinPlan j) throws IOException {
        List<String> outer = executeInner(j.outerPlan());
        LOGGER.info("HashJoin: outer=" + outer.size() + " rows, scanning right table");

        // Build: right table → hash map keyed on innerJoinCol
        Map<String, List<String>> buildMap = new HashMap<>();
        var allOffsets = FileHelper.readFile(dataFile, false);
        for (long off : allOffsets.values()) {
            String line = FileHelper.findLineByOffset(dataFile, off);
            if (line == null) continue;
            String key = extractField(line, j.innerJoinCol());
            if (key != null) buildMap.computeIfAbsent(key, k -> new ArrayList<>()).add(line);
        }
        LOGGER.info("HashJoin: build map has " + buildMap.size() + " distinct keys");

        // Probe: for each outer row find matches
        List<String> result = new ArrayList<>();
        for (String outerLine : outer) {
            String outerKey = extractField(outerLine, j.outerJoinCol());
            List<String> matches = buildMap.get(outerKey);
            if (matches != null) result.addAll(matches);
        }
        LOGGER.info("HashJoin: produced " + result.size() + " joined rows");
        return result;
    }

    // -----------------------------------------------------------------------
    // Nested-loop join — O(N × M)
    // -----------------------------------------------------------------------

    /**
     * For each outer row, scan the entire inner table.
     *
     * Acceptable ONLY when outer N is tiny (1–few rows from an index lookup).
     * If outer has 1 row (e.g. WHERE id = 42 → index → 1 row), this is
     * equivalent to M comparisons — exactly what happens in a "primary key
     * lookup + foreign key enrichment" OLTP query.
     */
    private List<String> executeNestedLoopJoin(QueryPlan.NestedLoopJoinPlan j) throws IOException {
        List<String> outer = executeInner(j.outerPlan());
        LOGGER.info("NestedLoopJoin: outer=" + outer.size() + " × inner=full_scan O(N×M)");

        var allOffsets = FileHelper.readFile(dataFile, false);
        List<String> inner = new ArrayList<>();
        for (long off : allOffsets.values()) {
            String line = FileHelper.findLineByOffset(dataFile, off);
            if (line != null) inner.add(line);
        }

        List<String> result = new ArrayList<>();
        for (String outerLine : outer) {
            String outerKey = extractField(outerLine, j.outerJoinCol());
            for (String innerLine : inner) {
                if (outerKey != null && outerKey.equals(extractField(innerLine, j.innerJoinCol()))) {
                    result.add(outerLine + "|JOIN|" + innerLine);
                }
            }
        }
        LOGGER.info("NestedLoopJoin: produced " + result.size() + " rows");
        return result;
    }

    // -----------------------------------------------------------------------
    // Index nested-loop join — O(N × log M)
    // -----------------------------------------------------------------------

    /**
     * For each outer row, use an index lookup into the right table.
     *
     * This is the dominant OLTP join pattern for foreign key relationships:
     *   order.customer_id → index lookup in customers → O(log M) per probe
     *
     * Compared to hash join: no upfront O(M) build phase; good when N is
     * small and M is large (e.g. orders → customers where customers is big).
     */
    private List<String> executeIndexJoin(QueryPlan.IndexJoinPlan j) throws IOException {
        List<String> outer = executeInner(j.outerPlan());
        LOGGER.info("IndexJoin: outer=" + outer.size() + " × index_probe O(N×log M)");

        IndexTypes indexType = IndexTypes.getIndexByType(j.innerIndexType());
        List<String> result = new ArrayList<>();

        for (String outerLine : outer) {
            String keyStr = extractField(outerLine, j.outerJoinCol());
            if (keyStr == null) continue;
            try {
                int id = Integer.parseInt(keyStr);
                Long off = (Long) indexType.findAddrInIndex(id);
                if (off != null) {
                    String innerLine = FileHelper.findLineByOffset(dataFile, off);
                    if (innerLine != null) result.add(outerLine + "|JOIN|" + innerLine);
                }
            } catch (NumberFormatException e) {
                // non-integer join key — fall through (no match)
            }
        }
        LOGGER.info("IndexJoin: produced " + result.size() + " rows");
        return result;
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private List<String> offsetToRows(Long offset) throws IOException {
        if (offset == null) return List.of();
        String line = FileHelper.findLineByOffset(dataFile, offset);
        return line != null ? List.of(line) : List.of();
    }

    private List<String> offsetsToRows(List<Long> offsets) throws IOException {
        var rows = new ArrayList<String>();
        for (long off : offsets) {
            String line = FileHelper.findLineByOffset(dataFile, off);
            if (line != null) rows.add(line);
        }
        return rows;
    }

    private List<String> idsToRows(List<Integer> ids) throws IOException {
        var allOffsets = FileHelper.readFile(dataFile, false);
        var rows = new ArrayList<String>();
        for (int id : ids) {
            Long off = allOffsets.get(id);
            if (off != null) {
                String line = FileHelper.findLineByOffset(dataFile, off);
                if (line != null) rows.add(line);
            }
        }
        return rows;
    }

    /** Check if a data-file line matches field=value. */
    private boolean matchesField(String line, String field, String value) {
        int comma = line.indexOf(CommonConsts.ID_SEPARATOR);
        if (comma < 0) return false;
        if ("id".equals(field)) return line.substring(0, comma).equals(value);
        String json = line.substring(comma + 1);
        return json.contains("\"" + field + "\":\"" + value + "\"")
            || json.contains("\"" + field + "\":" + value);
    }

    /** Extract a field value from a data-file line "id,{json}". */
    private String extractField(String line, String field) {
        if (line == null) return null;
        int comma = line.indexOf(CommonConsts.ID_SEPARATOR);
        if (comma < 0) return null;
        if ("id".equals(field)) return line.substring(0, comma);
        String json = line.substring(comma + 1);
        // Simple extraction: find "field":value or "field":"value"
        String key1 = "\"" + field + "\":\"";
        String key2 = "\"" + field + "\":";
        int idx = json.indexOf(key1);
        if (idx >= 0) {
            int start = idx + key1.length();
            int end   = json.indexOf('"', start);
            return end >= 0 ? json.substring(start, end) : null;
        }
        idx = json.indexOf(key2);
        if (idx >= 0) {
            int start = idx + key2.length();
            int end   = Math.min(
                json.indexOf(',', start) >= 0 ? json.indexOf(',', start) : json.length(),
                json.indexOf('}', start) >= 0 ? json.indexOf('}', start) : json.length());
            return json.substring(start, end).trim();
        }
        return null;
    }

    private List<String> applyLimit(List<String> rows, int limit) {
        if (limit < 0 || limit >= rows.size()) return rows;
        return rows.subList(0, limit);
    }
}
