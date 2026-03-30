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
 * SQL query executor — executes a QueryPlan and returns matching document lines.
 *
 * Pattern-matching switch over the sealed QueryPlan hierarchy (Java 21).
 * Adding a new plan type without updating this switch is a compile error.
 *
 * LSMTreeScan is now handled: the executor calls LSMTreeIndex.get(id) which
 * internally checks the MemTable then scans SSTables with Bloom-filter pruning.
 */
public class QueryExecutor {
    private static final Logger LOGGER = Logger.getLogger(QueryExecutor.class.getName());

    private final String dataFile = PATH_TO_DATA_FILE;

    /**
     * Execute the plan and return raw data-file lines: "id,{json}".
     * @param limit  max rows to return; -1 = unlimited
     */
    public List<String> execute(QueryPlan plan, int limit) throws IOException {
        List<String> rows = switch (plan) {

            case QueryPlan.HashIndexScan s -> {
                Object raw = IndexTypes.HASH_INDEX.findAddrInIndex(s.id());
                yield offsetToRows(raw instanceof Long l ? l : null);
            }

            case QueryPlan.BPlusTreeScan s -> {
                Long offset = (Long) IndexTypes.BPLUSTREE.findAddrInIndex(s.id());
                yield offsetToRows(offset);
            }

            case QueryPlan.LSMTreeScan s -> {
                // LSMTreeIndex.get() checks MemTable first, then SSTables with
                // per-SSTable Bloom filter — no file is opened unless the filter
                // says the key might be present.
                String lsmFile = DISC_PATH + "/" + IndexTypes.LSMTREE.getIndexFileName();
                LSMTreeIndex lsm = IndexKeeper.INSTANCE.getLsmTreeIndexes().get(lsmFile);
                if (lsm == null) { yield List.of(); }
                Long offset = lsm.get(s.id());
                LOGGER.log(Level.FINE, "LSMTreeScan id=" + s.id() + " offset=" + offset
                    + " bloom_hits=" + lsm.bloomFilterHits()
                    + " bloom_misses=" + lsm.bloomFilterMisses());
                yield offsetToRows(offset);
            }

            case QueryPlan.GINScan s -> {
                String ginFile = DISC_PATH + "/" + IndexTypes.GIN.getIndexFileName();
                GINIndex gin = IndexKeeper.INSTANCE.getGINIndexes().get(ginFile);
                if (gin == null) { yield List.of(); }
                yield offsetsToRows(gin.search(s.token()));
            }

            case QueryPlan.BitmapScan s -> {
                String bmpFile = DISC_PATH + "/" + IndexTypes.BITMAP.getIndexFileName();
                BitmapIndex bmp = IndexKeeper.INSTANCE.getBitmapIndexes().get(bmpFile);
                if (bmp == null) { yield List.of(); }
                yield idsToRows(bmp.search(s.value()));
            }

            case QueryPlan.FullScan s -> {
                var allOffsets = FileHelper.readFile(dataFile, false);
                var result     = new ArrayList<String>();
                for (long offset : allOffsets.values()) {
                    String line = FileHelper.findLineByOffset(dataFile, offset);
                    if (line == null) continue;
                    if (s.field() == null || matchesField(line, s.field(), s.value()))
                        result.add(line);
                }
                yield result;
            }
        };

        return applyLimit(rows, limit);
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
        var rows       = new ArrayList<String>();
        for (int id : ids) {
            Long off = allOffsets.get(id);
            if (off != null) {
                String line = FileHelper.findLineByOffset(dataFile, off);
                if (line != null) rows.add(line);
            }
        }
        return rows;
    }

    private boolean matchesField(String line, String field, String value) {
        int comma = line.indexOf(CommonConsts.ID_SEPARATOR);
        if (comma < 0) return false;
        if ("id".equals(field)) return line.substring(0, comma).equals(value);
        String json = line.substring(comma + 1);
        return json.contains("\"" + field + "\":\"" + value + "\"")
            || json.contains("\"" + field + "\":" + value);
    }

    private List<String> applyLimit(List<String> rows, int limit) {
        if (limit < 0 || limit >= rows.size()) return rows;
        return rows.subList(0, limit);
    }
}
