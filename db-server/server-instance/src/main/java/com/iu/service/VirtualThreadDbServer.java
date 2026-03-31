package com.iu.service;

import com.iu.worker.AbstractTask;
import com.iu.worker.HashIndexRecoverTask;
import com.iu.worker.SequenceRecoverTask;
import com.iu.worker.TaskType;
import com.message.MessageBean;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.files.FileHelper.createDirectoryIfNotExist;

/**
 * Virtual-thread DB server (Java 21).
 *
 * DESIGN
 * ──────
 * Each accepted connection runs in its own virtual thread. Virtual threads
 * are cheap enough that no pool sizing is needed — the JVM schedules them
 * onto carrier (platform) threads transparently.
 *
 * The connection Socket is passed two ways:
 *   1. Explicitly via AbstractTask constructor (direct parameter)
 *   2. Via ScopedValue for any helper deeper in the call tree that needs it
 *      without a Socket parameter threaded through every method.
 *
 * AbstractTask.call() is now final and wraps execute() — so the lambda here
 * stays a clean single expression with no checked-exception handling needed.
 *
 * Startup uses StructuredTaskScope so both recovery tasks (HashIndex + Sequence)
 * run concurrently and any failure aborts startup immediately.
 */
public class VirtualThreadDbServer {
    private static final Logger LOGGER = Logger.getLogger(VirtualThreadDbServer.class.getName());

    /** Scoped value carrying the active Socket for the current virtual thread. */
    public static final ScopedValue<Socket> CURRENT_CONNECTION = ScopedValue.newInstance();

    private final int    port;
    private final String discPath;
    private volatile boolean running = true;

    public VirtualThreadDbServer(int port, int core, int max, int keepAlive,
                                  int queueCap, String discPath) {
        this.port     = port;
        this.discPath = discPath;
    }

    public void start() throws IOException {
        createDirectoryIfNotExist(discPath);
        runStartupTasks();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
             ServerSocket server      = new ServerSocket(port)) {

            LOGGER.info("DB server listening on port " + port
                + " (virtual threads, Java " + Runtime.version().feature() + ")");

            while (running) {
                Socket connection = server.accept();
                executor.submit(() -> handleConnection(connection));
            }
        }
    }

    private void handleConnection(Socket connection) {
        try {
            MessageBean bean     = readMessage(connection.getInputStream());
            TaskType    taskType = TaskType.getTaskByType(bean.taskType());

            if (taskType == null) {
                writeError(connection, "Unknown task type: " + bean.taskType());
                return;
            }

            AbstractTask task = taskType.getTask(connection, bean.payload());

            // Bind the connection as a scoped value for this virtual thread's subtree.
            // task.call() is final in AbstractTask — it wraps execute() and converts
            // checked exceptions to RuntimeException, so no checked throws here.
            ScopedValue.where(CURRENT_CONNECTION, connection)
                       .run(task::call);

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error handling connection", e);
        } finally {
            try { connection.close(); } catch (IOException ignored) {}
        }
    }

    /** Structured concurrency: both startup tasks run concurrently, either failure aborts. */
    private void runStartupTasks() {
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            scope.fork(() -> { new HashIndexRecoverTask().call(); return null; });
            scope.fork(() -> { new SequenceRecoverTask().call();  return null; });
            scope.join();
            scope.throwIfFailed();
            LOGGER.info("Startup recovery complete");
        } catch (Exception e) {
            throw new RuntimeException("Startup recovery failed", e);
        }
    }

    private static MessageBean readMessage(InputStream is) throws IOException, ClassNotFoundException {
        return (MessageBean) new ObjectInputStream(is).readObject();
    }

    private static void writeError(Socket s, String msg) {
        try (ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream())) {
            oos.writeObject(msg);
        } catch (IOException ignored) {}
    }

    public void stop() { running = false; }
}
