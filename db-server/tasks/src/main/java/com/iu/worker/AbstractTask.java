package com.iu.worker;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base class for all DB tasks executed by the virtual-thread server.
 *
 * Lifecycle:
 *   call()    — called by the executor; handles exception wrapping (final)
 *   execute() — override in subclasses; contains the actual business logic
 *
 * This split keeps VirtualThreadDbServer.handleConnection() clean:
 *   ScopedValue.where(CURRENT_CONNECTION, connection)
 *              .run(() -> task.call());   // no checked exceptions leak out
 *
 * The connection is available two ways:
 *   1. Via the constructor field `this.connection` (direct parameter passing)
 *   2. Via ScopedValue.get(VirtualThreadDbServer.CURRENT_CONNECTION) from any
 *      helper called within this virtual thread's subtree — no need to thread
 *      the Socket through every method signature.
 */
public abstract class AbstractTask implements Callable<Void> {

    private static final Logger LOGGER = Logger.getLogger(AbstractTask.class.getName());

    public static final String DISC_PATH              = "./data";
    public static final String PATH_TO_DATA_FILE      = DISC_PATH + "/data.dat";
    public static final String PATH_TO_INDEX_REGISTRY = DISC_PATH + "/index_registry.dat";
    public static final String SNAPSHOT_FILE          = DISC_PATH + "/hashindex_snapshot.dat";

    protected final Socket connection;
    protected final String taskPayload;

    protected AbstractTask(Socket connection, String taskPayload) {
        this.connection  = connection;
        this.taskPayload = taskPayload;
    }

    /**
     * Called by the executor. Wraps execute() so checked exceptions never
     * escape into the virtual-thread runner — they become RuntimeExceptions
     * that the server's catch(Exception) picks up and logs.
     */
    @Override
    public final Void call() {
        try {
            execute();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, getClass().getSimpleName() + " failed", e);
            throw new RuntimeException(e);
        }
        return null;
    }

    /**
     * Subclasses override this to implement their logic.
     * May throw any checked exception — call() will wrap it.
     */
    protected void execute() throws Exception {
        throw new IllegalStateException("execute() not implemented in " + getClass().getSimpleName());
    }

    // ── Shared utilities ───────────────────────────────────────────────────

    protected void writeResponse(String body) {
        try (ObjectOutputStream oos = new ObjectOutputStream(connection.getOutputStream())) {
            oos.writeObject(body);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failed to write response", e);
        }
    }

    protected void closeQuietly() {
        if (connection != null && !connection.isClosed()) {
            try { connection.close(); } catch (IOException ignore) {}
        }
    }

    // ── Keep old signature for any callers that haven't been updated yet ──

    /** @deprecated Use {@link #writeResponse(String)} */
    protected void writeResponse(Socket ignored, String body) { writeResponse(body); }
    /** @deprecated Use {@link #closeQuietly()} */
    protected void closeQuietly(Socket ignored) { closeQuietly(); }
}
