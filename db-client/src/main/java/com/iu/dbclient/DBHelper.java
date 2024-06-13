package com.iu.dbclient;

public class DBHelper {
    public static final String DB_URL = "localhost";

    public static final Integer DB_PORT = 5555;

    public static DBConnection getConnection() {
        return DbConnector.INSTANCE.getConnection(DB_URL, DB_PORT);
    }
}
