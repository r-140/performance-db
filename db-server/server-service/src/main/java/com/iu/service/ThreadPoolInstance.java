package com.iu.service;

import com.iu.worker.AbstractTask;
import com.iu.worker.HashIndexRecoverTask;
import com.iu.worker.SaveHashIndexSnapshotTask;
import com.iu.worker.SequenceRecoverTask;

import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.files.FileHelper.createDirectoryIfNotExist;

public enum ThreadPoolInstance {

    INSTANCE;

    private static final Logger LOGGER = Logger.getLogger(ThreadPoolInstance.class.getName());

    private ThreadPoolExecutor executorPool;
    private ScheduledExecutorService scheduledExecutor;
    private MonitorThread monitor;
    private Thread monitorThread;

    public void init(int corePoolSize, int maxPoolSize, int keepAliveTime,
                     int maxQueueCapacity, String discPath) {
        initializeDisk(discPath);

        executorPool = new ThreadPoolExecutor(
                corePoolSize, maxPoolSize, keepAliveTime, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(maxQueueCapacity),
                Executors.defaultThreadFactory(),
                new RejectedExecutionHandlerImpl());

        // Use future.get() instead of a busy-wait spin loop
        executeAndWait(new HashIndexRecoverTask(), "HashIndexRecoverTask");
        executeAndWait(new SequenceRecoverTask(),  "SequenceRecoverTask");

        ThreadFactory tf = Executors.defaultThreadFactory();
        scheduledExecutor = Executors.newScheduledThreadPool(1, tf);
        scheduledExecutor.scheduleAtFixedRate(
                new SaveHashIndexSnapshotTask(), 2, 5, TimeUnit.SECONDS);

        monitor = new MonitorThread(executorPool, 3);
        monitorThread = new Thread(monitor, "db-pool-monitor");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    /**
     * Submit a startup task and block until it completes.
     * Uses {@code future.get()} — no CPU-spinning.
     */
    private void executeAndWait(Callable<?> task, String taskName) {
        Future<?> future = executorPool.submit(task);
        try {
            future.get(60, TimeUnit.SECONDS);
            LOGGER.log(Level.INFO, taskName + " completed");
        } catch (TimeoutException e) {
            LOGGER.log(Level.SEVERE, taskName + " timed out during startup");
            future.cancel(true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.SEVERE, taskName + " interrupted");
        } catch (ExecutionException e) {
            LOGGER.log(Level.SEVERE, taskName + " threw an exception: " + e.getCause());
        }
    }

    public void submitTask(AbstractTask task) {
        executorPool.submit(task);
    }

    /** Graceful shutdown — call shutdown() THEN awaitTermination(). */
    public void shutdown() throws InterruptedException {
        monitor.shutdown();

        scheduledExecutor.shutdown();
        scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS);

        executorPool.shutdown();          // stop accepting new tasks
        if (!executorPool.awaitTermination(30, TimeUnit.SECONDS)) {
            executorPool.shutdownNow();   // cancel running tasks
        }
    }

    private void initializeDisk(String diskPath) {
        boolean created = createDirectoryIfNotExist(diskPath);
        LOGGER.info("Directory " + diskPath + (created ? " created" : " already exists"));
    }
}
