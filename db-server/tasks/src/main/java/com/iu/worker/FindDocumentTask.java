package com.iu.worker;

import com.iu.sql.*;
import com.json.JsonHelper;

import java.net.Socket;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.SharedLock.lock;

/**
 * Legacy "find" protocol — delegates to the SQL engine.
 *
 * Translates {"id": 42, "indexType": "lsmtree"} into
 * SELECT * FROM data WHERE id = 42 LIMIT 1 and routes it through
 * QueryPlanner, which automatically picks the best available index
 * (including LSM with Bloom-filter-guarded SSTables).
 */
class FindDocumentTask extends AbstractTask {
    private static final Logger LOGGER = Logger.getLogger(FindDocumentTask.class.getName());

    private final SqlParser     parser   = new SqlParser();
    private final QueryPlanner  planner  = new QueryPlanner();
    private final QueryExecutor executor = new QueryExecutor();

    FindDocumentTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    protected void execute() throws Exception {
        long stamp = lock.readLock();
        try {
            int id;
            try {
                id = (Integer) JsonHelper.getValueFromJsonByKey(taskPayload, "id");
            } catch (Exception e) {
                writeResponse((String) null);
                return;
            }

            String sql  = "SELECT * FROM data WHERE id = " + id + " LIMIT 1";
            LOGGER.log(Level.FINE, "FindDocumentTask → SQL: " + sql);

            SqlNode.SelectStatement stmt = parser.parse(sql);
            QueryPlan plan = planner.plan(stmt);
            LOGGER.log(Level.INFO, "Plan: " + plan.getClass().getSimpleName());

            List<String> rows = executor.execute(plan, 1);
            writeResponse(rows.isEmpty() ? null : rows.get(0));
        } finally {
            lock.unlockRead(stamp);
            closeQuietly();
        }
    }
}
