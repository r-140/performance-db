package com.iu.sql;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.iu.indexes.bitmap.BitmapIndex;
import com.iu.indexes.gin.GINIndex;
import com.iu.indexes.IndexKeeper;
import com.util.CommonConsts;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.iu.worker.AbstractTask.*;

/**
 * SQL query executor.
 *
 * Takes a QueryPlan and returns a list of JSON document strings.
 *
 * Uses pattern-matching switch (JEP 441, Java 21) over the sealed QueryPlan
 * hierarchy — exhaustive, no default needed, compile error if a new plan
 * type isn't handled here.
 *
 * Also uses Stream.toList() (Java 16), var (Java 10), and text blocks (Java 15).
 */
public class QueryExecutor {
    private static final Logger LOGGER = Logger.getLogger(QueryExecutor.class.getName());

    private final String dataFile;

    public QueryExecutor() {
        this.dataFile = PATH_TO_DATA_FILE;
    }

    /**
     * Execute the plan and return matching document lines.
     * Each returned string is the raw line from the data file: "id,{json}".
     */
    public List<String> execute(QueryPlan plan, int limit) throws IOException {
        // Pattern-matching switch over sealed QueryPlan (Java 21)
        List<String> rows = switch (plan) {

            case QueryPlan.HashIndexScan s -> {
                Long offset = IndexTypes.HASH_INDEX.findAddrInIndex(s.id()) instanceof Long l ? l : null;
                yield offsetToRows(offset);
            }

            case QueryPlan.BPlusTreeScan s -> {
                Long offset = (Long) IndexTypes.BPLUSTREE.findAddrInIndex(s.id());
                yield offsetToRows(offset);
            }

            case QueryPlan.GINScan s -> {
                String ginFile = DISC_PATH + "/" + IndexTypes.GIN.getIndexFileName();
                GINIndex idx = IndexKeeper.INSTANCE.getGINIndexes().get(ginFile);
                if (idx == null) { yield List.of(); }
                List<Long> offsets = idx.search(s.token());
                yield offsetsToRows(offsets);
            }

            case QueryPlan.BitmapScan s -> {
                String bmpFile = DISC_PATH + "/" + IndexTypes.BITMAP.getIndexFileName();
                BitmapIndex idx = IndexKeeper.INSTANCE.getBitmapIndexes().get(bmpFile);
                if (idx == null) { yield List.of(); }
                List<Integer> ids = idx.search(s.value());
                yield idsToRows(ids);
            }

            case QueryPlan.FullScan s -> {
                // Load all offsets from the data file and optionally filter
                var allOffsets = FileHelper.readFile(dataFile, false);
                var allRows = new ArrayList<String>();
                for (long offset : allOffsets.values()) {
                    String line = FileHelper.findLineByOffset(dataFile, offset);
                    if (line == null) continue;
                    if (s.field() == null || matchesField(line, s.field(), s.value())) {
                        allRows.add(line);
                    }
                }
                yield allRows;
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
        var rows = new ArrayList<String>();
        for (int id : ids) {
            Long offset = allOffsets.get(id);
            if (offset != null) {
                String line = FileHelper.findLineByOffset(dataFile, offset);
                if (line != null) rows.add(line);
            }
        }
        return rows;
    }

    /** Checks if a data-file line (id,json) contains field=value. */
    private boolean matchesField(String line, String field, String value) {
        // Line format: "42,{\"data\":\"testdata42\",\"id\":42}"
        int comma = line.indexOf(CommonConsts.ID_SEPARATOR);
        if (comma < 0) return false;

        if ("id".equals(field)) {
            return line.substring(0, comma).equals(value);
        }
        // Simple substring check for other fields
        String json = line.substring(comma + 1);
        return json.contains("\"" + field + "\":\"" + value + "\"")
            || json.contains("\"" + field + "\":" + value);
    }

    private List<String> applyLimit(List<String> rows, int limit) {
        if (limit < 0 || limit >= rows.size()) return rows;
        return rows.subList(0, limit);
    }
}
