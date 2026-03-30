package com.iu.dbclient;

import com.message.MessageBean;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Single-use DB connection. Creates a fresh Socket per request.
 *
 * Updated for Java 21: MessageBean is now a record — accessor is
 * bean.taskType() not bean.getTaskType(), but that's internal to this class.
 */
public class DBConnection {

    private final String host;
    private final int    port;

    public DBConnection(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String send(MessageBean bean) throws IOException, ClassNotFoundException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5_000);
            socket.setSoTimeout(30_000);

            new ObjectOutputStream(socket.getOutputStream()).writeObject(bean);
            return (String) new ObjectInputStream(socket.getInputStream()).readObject();
        }
    }

    // Backward-compatible convenience methods
    public String appendData(MessageBean bean)  throws IOException, ClassNotFoundException { return send(bean); }
    public String readData(MessageBean bean)     throws IOException, ClassNotFoundException { return send(bean); }
    public String createIndex(MessageBean bean)  throws IOException, ClassNotFoundException { return send(bean); }
    public void   close() { /* socket is closed after each request */ }
}
