package com.iu.worker;

import com.iu.sql.*;
import com.json.JsonHelper;

import java.net.Socket;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.SharedLock.lock;

class SqlQueryTask extends AbstractTask {
    private static final Logger LOGGER = Logger.getLogger(SqlQueryTask.class.getName());

    private final SqlParser     parser   = new SqlParser();
    private final QueryPlanner  planner  = new QueryPlanner();
    private final QueryExecutor executor = new QueryExecutor();

    SqlQueryTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    protected void execute() throws Exception {
        long stamp = lock.readLock();
        try {
            LOGGER.log(Level.INFO, "SQL: " + taskPayload);
            SqlNode.SelectStatement stmt = parser.parse(taskPayload);
            QueryPlan plan = planner.plan(stmt);
            LOGGER.log(Level.INFO, "Plan: " + plan);

            List<String> rows = executor.execute(plan, stmt.limit());

            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < rows.size(); i++) {
                if (i > 0) sb.append(",");
                int comma = rows.get(i).indexOf(',');
                sb.append(comma >= 0 ? rows.get(i).substring(comma + 1) : rows.get(i));
            }
            writeResponse(sb.append("]").toString());

        } catch (SqlParser.SqlParseException e) {
            writeResponse(JsonHelper.buildErrorResponse("SQL-001", e.getMessage(), ""));
        } finally {
            lock.unlockRead(stamp);
            closeQuietly();
        }
    }
}
