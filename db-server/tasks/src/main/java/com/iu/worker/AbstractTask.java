package com.iu.worker;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.Callable;

public class AbstractTask implements Callable<Void> {

    //todo move it to property file
    public static final String DISC_PATH = "./data";
    public static final String PATH_TO_DATA_FILE = DISC_PATH + "/data.dat";
    public static final String PATH_TO_INDEX_REGISTRY = DISC_PATH + "/index_registry.dat";
    public static final String SNAPSHOT_FILE = DISC_PATH + "/hashindex_snapshot.dat";
    Socket connection;
    String taskPayload;

    public AbstractTask(Socket connection, String taskPayload) {
        this.connection = connection;
        this.taskPayload = taskPayload;
    }

    @Override
    public Void call() throws Exception {
        return null;
    }

    protected void writeResponse(Socket connection, String body) {
        try (ObjectOutputStream oos = new ObjectOutputStream(connection.getOutputStream())) {
            oos.writeObject(body);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //ignore
    }
}
