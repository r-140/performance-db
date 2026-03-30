package com.iu.dbclient.sql;

import com.iu.dbclient.DBConnection;
import com.message.MessageBean;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.dbclient.DBHelper.DB_PORT;
import static com.iu.dbclient.DBHelper.DB_URL;

/**
 * Client helper for SQL queries. Server auto-selects the best index.
 *
 * Examples:
 *   SqlHelper.query("SELECT * FROM data");
 *   SqlHelper.query("SELECT * FROM data WHERE id = 42");
 *   SqlHelper.query("SELECT * FROM data WHERE data = 'testdata5'");
 *   SqlHelper.query("SELECT * FROM data WHERE id = 10 LIMIT 5");
 */
public class SqlHelper {
    private static final Logger LOGGER = Logger.getLogger(SqlHelper.class.getName());

    public static String query(String sql) {
        var connection = new DBConnection(DB_URL, DB_PORT);
        try {
            return connection.send(new MessageBean("sql", sql));
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "SQL query failed: " + sql, e);
            return null;
        }
    }
}
