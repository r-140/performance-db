package com.iu.dbclient;


public enum DbConnector {
    INSTANCE;

    public DBConnection getConnection(final String host, final Integer port) {
        return new DBConnection(host, port);
    }
}
