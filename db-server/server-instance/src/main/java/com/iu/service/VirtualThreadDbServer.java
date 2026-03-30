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
 * DB server rebuilt for Java 21.
 *
 * Key changes from the original ThreadPoolInstance approach:
 *
 * 1. VIRTUAL THREADS (JEP 444)
 *    Each accepted connection is handed off to a virtual thread via
 *    Executors.newVirtualThreadPerTaskExecutor(). Virtual threads are
 *    extremely cheap (hundreds of thousands fit in normal heap) and
 *    block without consuming a platform thread, making them ideal for
 *    the socket-blocking pattern this server uses.
 *
 *    Old:  ThreadPoolExecutor with corePoolSize/maxPoolSize/queue
 *    New:  newVirtualThreadPerTaskExecutor() — no pool sizing needed
 *
 * 2. STRUCTURED CONCURRENCY (JEP 453, Java 21 preview → 480 final in 24)
 *    Startup recovery tasks (hash index + sequence) run inside a
 *    StructuredTaskScope so both must succeed before the server starts
 *    accepting connections. If either fails the scope propagates the
 *    exception immediately — no more busy-wait or manual future.get().
 *
 * 3. SCOPED VALUES (JEP 446, Java 21 preview)
 *    The request-scoped connection context is bound via ScopedValue
 *    instead of passing it through method parameters, making it
 *    accessible to any helper called within the virtual thread without
 *    ThreadLocal's inheritance/cleanup problems.
 */
public class VirtualThreadDbServer {
    private static final Logger LOGGER = Logger.getLogger(VirtualThreadDbServer.class.getName());

    /**
     * Scoped value — carries the active connection for the duration of
     * a single virtual thread's request. Replaces passing Socket as a
     * parameter through the task hierarchy.
     */
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

        // Virtual-thread-per-task executor — no pool sizing, no queue limits
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
            MessageBean bean = readMessage(connection.getInputStream());
            TaskType taskType = TaskType.getTaskByType(bean.taskType());

            if (taskType == null) {
                LOGGER.warning("Unknown task type: " + bean.taskType());
                writeError(connection, "Unknown task type: " + bean.taskType());
                return;
            }

            // Bind the connection as a scoped value for this virtual thread's subtree
            ScopedValue.where(CURRENT_CONNECTION, connection)
                       .run(() -> taskType.getTask(connection, bean.payload()).call());

        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error handling connection", e);
        } finally {
            try { connection.close(); } catch (IOException ignored) {}
        }
    }

    /**
     * Structured concurrency startup (JEP 453).
     *
     * Both recovery tasks must succeed. ShutdownOnFailure ensures that
     * if either throws, the other is cancelled and the exception re-thrown.
     * This replaces the original busy-wait + sequential future.get().
     */
    private void runStartupTasks() {
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            StructuredTaskScope.Subtask<Void> indexRecovery =
                    scope.fork(() -> { new HashIndexRecoverTask().call(); return null; });
            StructuredTaskScope.Subtask<Void> seqRecovery =
                    scope.fork(() -> { new SequenceRecoverTask().call();  return null; });

            scope.join();           // wait for both
            scope.throwIfFailed();  // re-throw if either failed

            LOGGER.info("Startup recovery complete — hash index and sequence restored");
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
