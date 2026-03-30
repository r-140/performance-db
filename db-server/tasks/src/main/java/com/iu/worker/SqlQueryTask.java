package com.iu.worker;

import com.iu.sql.*;
import com.json.JsonHelper;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.FindDocumentTask.lock;

/**
 * Handles "sql" task type — executes a SELECT query.
 *
 * Pipeline: SQL string → SqlParser → SqlNode → QueryPlanner → QueryPlan → QueryExecutor → rows
 *
 * The planner automatically selects the best available index.
 * The response is a JSON array of matching document lines.
 */
class SqlQueryTask extends AbstractTask {
    private static final Logger LOGGER = Logger.getLogger(SqlQueryTask.class.getName());

    private final SqlParser    parser   = new SqlParser();
    private final QueryPlanner planner  = new QueryPlanner();
    private final QueryExecutor executor = new QueryExecutor();

    SqlQueryTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public void execute() {
        long stamp = lock.readLock(); // SELECT is read-only
        try {
            LOGGER.log(Level.INFO, "SQL query: " + taskPayload);

            SqlNode.SelectStatement stmt = parser.parse(taskPayload);
            QueryPlan plan = planner.plan(stmt);

            LOGGER.log(Level.INFO, "Plan chosen: " + plan);

            List<String> rows = executor.execute(plan, stmt.limit());

            // Serialize as JSON array
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < rows.size(); i++) {
                if (i > 0) sb.append(",");
                // Extract the JSON part after "id,"
                int comma = rows.get(i).indexOf(',');
                sb.append(comma >= 0 ? rows.get(i).substring(comma + 1) : rows.get(i));
            }
            sb.append("]");

            writeResponse(connection, sb.toString());

        } catch (SqlParser.SqlParseException e) {
            writeResponse(connection, JsonHelper.buildErrorResponse("SQL-001", e.getMessage(), ""));
        } catch (IOException e) {
            writeResponse(connection, JsonHelper.buildErrorResponse(
                    ErrorCode.IOEXCEPTION.getErrorCode(),
                    ErrorCode.IOEXCEPTION.getErrorMessage(), e.getMessage()));
            LOGGER.log(Level.SEVERE, "SqlQueryTask IO error", e);
        } finally {
            lock.unlockRead(stamp);
            closeQuietly(connection);
        }
    }
}
