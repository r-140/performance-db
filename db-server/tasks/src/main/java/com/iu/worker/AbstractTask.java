package com.iu.worker;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AbstractTask implements Callable<Void> {

    private static final Logger LOGGER = Logger.getLogger(AbstractTask.class.getName());

    public static final String DISC_PATH             = "./data";
    public static final String PATH_TO_DATA_FILE     = DISC_PATH + "/data.dat";
    public static final String PATH_TO_INDEX_REGISTRY = DISC_PATH + "/index_registry.dat";
    public static final String SNAPSHOT_FILE         = DISC_PATH + "/hashindex_snapshot.dat";

    Socket connection;
    String taskPayload;

    public AbstractTask(Socket connection, String taskPayload) {
        this.connection  = connection;
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
            LOGGER.log(Level.WARNING, "Failed to write response", e);
        }
    }

    /** Close without throwing — use in finally blocks. */
    protected void closeQuietly(Socket s) {
        if (s != null && !s.isClosed()) {
            try { s.close(); } catch (IOException ignore) {}
        }
    }
}
