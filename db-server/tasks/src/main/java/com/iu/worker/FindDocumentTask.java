package com.iu.worker;

import com.iu.sql.*;
import com.json.JsonHelper;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles the legacy "find" wire protocol.
 *
 * WHY THIS STILL EXISTS
 * ─────────────────────
 * The "find" task pre-dates the SQL engine. It accepted a JSON payload:
 *   {"id": 42, "indexType": "lsmtree"}
 * and hard-coded the index dispatch in its own loop.
 *
 * Problems with the old approach:
 *  - Every new index type had to be added here manually.
 *  - LSM index was wired in IndexTypes but QueryPlanner never considered it,
 *    so SQL queries never used LSM even when it was the only index available.
 *  - Bloom filter optimisation in LSMTreeIndex.get() was bypassed when the
 *    caller went directly through IndexTypes.findAddrInIndex().
 *
 * NEW APPROACH — delegate to the SQL engine
 * ─────────────────────────────────────────
 * FindDocumentTask now translates the legacy payload into a SQL SELECT and
 * passes it through SqlQueryTask. This means:
 *  - The QueryPlanner automatically selects the best available index,
 *    including LSM with Bloom-filter-guarded SSTable reads.
 *  - All future plan types (range scans, multi-predicate etc.) are
 *    immediately available via "find" without changing this class.
 *  - A single code path handles both protocols — no duplication.
 *
 * The "indexType" field in the payload is now ADVISORY: if that index
 * exists, the planner will prefer it naturally (it's in the registry).
 * If not, the planner picks the next best alternative automatically.
 *
 * Backward compatibility: the response format is unchanged — the first
 * matching document line is returned as a plain string (not wrapped in JSON
 * array), matching what FindDataHelper and Cucumber tests expect.
 */
class FindDocumentTask extends AbstractTask {

    private static final Logger LOGGER = Logger.getLogger(FindDocumentTask.class.getName());

    /** Shared read lock — allows concurrent reads alongside other read tasks. */
    static final StampedLock lock = new StampedLock();

    private final SqlParser     parser   = new SqlParser();
    private final QueryPlanner  planner  = new QueryPlanner();
    private final QueryExecutor executor = new QueryExecutor();

    FindDocumentTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.readLock();
        try {
            LOGGER.log(Level.INFO, "FindDocumentTask: " + taskPayload);

            // Extract id from legacy payload {"id":42, "indexType":"lsmtree"}
            int id;
            try {
                Object raw = com.json.JsonHelper.getValueFromJsonByKey(taskPayload, "id");
                id = (Integer) raw;
            } catch (Exception e) {
                writeResponse(connection, null);
                return null;
            }

            // Translate to SQL and run through the full planner → executor pipeline
            String sql = "SELECT * FROM data WHERE id = " + id + " LIMIT 1";
            LOGGER.log(Level.FINE, "Delegating to SQL: " + sql);

            SqlNode.SelectStatement stmt = parser.parse(sql);
            QueryPlan plan = planner.plan(stmt);
            LOGGER.log(Level.INFO, "FindDocumentTask plan chosen: " + plan.getClass().getSimpleName());

            List<String> rows = executor.execute(plan, 1);

            // Return the raw "id,{json}" line or null — same as the old protocol
            String result = rows.isEmpty() ? null : rows.get(0);
            LOGGER.log(Level.INFO, "FindDocumentTask result: " + result);
            writeResponse(connection, result);

        } catch (IOException e) {
            writeResponse(connection, JsonHelper.buildErrorResponse(
                    ErrorCode.IOEXCEPTION.getErrorCode(),
                    ErrorCode.IOEXCEPTION.getErrorMessage(), e.getMessage()));
            LOGGER.log(Level.SEVERE, "FindDocumentTask IO error", e);
        } finally {
            lock.unlockRead(stamp);
            closeQuietly(connection);
        }
        return null;
    }
}
