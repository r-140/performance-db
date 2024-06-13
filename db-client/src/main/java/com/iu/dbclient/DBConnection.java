package com.iu.dbclient;

import com.message.MessageBean;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;


public class DBConnection {

    private final Socket socket;

    private final String host;

    private final Integer port;

    public DBConnection(String host, Integer port) {
        this.host = host;
        this.port = port;
        this.socket = new Socket();
    }


    public synchronized String appendData(MessageBean bean) throws IOException, ClassNotFoundException {
        return handleRequest(bean);
    }

    public synchronized String readData(MessageBean bean) throws IOException, ClassNotFoundException {
        return handleRequest(bean);
    }

    public synchronized String createIndex(MessageBean bean) throws IOException, ClassNotFoundException {
        return handleRequest(bean);
    }

    private synchronized String handleRequest(final MessageBean bean) throws IOException, ClassNotFoundException {
        bindConnection();
//        todo specify logic
        ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
        System.out.println("Sending request to Socket Server");
        oos.writeObject(bean);

        ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
        String message = (String) ois.readObject();
        System.out.println("Message: " + message);

        return message;
    }

    public void close() {
        if (!socket.isClosed())
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    private void bindConnection() {
        final SocketAddress address = new InetSocketAddress(host, port);
        try {
            socket.connect(address);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open port " + port, e);
        }
    }
}
