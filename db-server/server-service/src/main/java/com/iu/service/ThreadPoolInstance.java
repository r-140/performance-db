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

    public void init(int corePoolSize, int maxPoolSize, int keepAliveTime, int maxQueueCapacity, String discPath) {

        initializeDisk(discPath);
        //RejectedExecutionHandler implementation
        RejectedExecutionHandlerImpl rejectionHandler = new RejectedExecutionHandlerImpl();
        //Get the ThreadFactory implementation to use
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        //creating the ThreadPoolExecutor
        executorPool = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime,
                TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(maxQueueCapacity), threadFactory, rejectionHandler);

        executeRecoverHashIndexTask();

        executeRecoverSequenceTask();

        scheduledExecutor = Executors.newScheduledThreadPool(1, threadFactory);
        executeSaveHashIndexTask();

        //start the monitoring thread
        monitor = new MonitorThread(executorPool, 3);
        Thread monitorThread = new Thread(monitor);
        monitorThread.start();
    }

    // TODO: 4/30/2019 read delay and period from property files
    private void executeSaveHashIndexTask() {
        LOGGER.log(Level.FINE, "executeSaveHashIndexTask()");
        SaveHashIndexSnapshotTask task = new SaveHashIndexSnapshotTask();

        LOGGER.log(Level.FINE, "executeSaveHashIndexTask() SaveHashIndexSnapshotTask has been created");
        ScheduledFuture<?> result = scheduledExecutor.scheduleAtFixedRate(task, 2, 5, TimeUnit.SECONDS);


    }

    private void initializeDisk(String diskPath) {
        boolean isDirectoryCreated = createDirectoryIfNotExist(diskPath);

        LOGGER.info(String.format("Directory %s has been created %s", diskPath, isDirectoryCreated));
    }

    public void submitTask(AbstractTask task) {
        executorPool.submit(task);
    }

    private void executeRecoverHashIndexTask() {
        HashIndexRecoverTask recoverTask = new HashIndexRecoverTask();
        Future<?> future = executorPool.submit(recoverTask);


        while (!future.isDone()) {
            LOGGER.log(Level.FINE, "executeRecoverHashIndexTask(): waiting while indexes recovering will be finished");
        }

        if (future.isDone()) {
            LOGGER.log(Level.INFO, "executeRecoverHashIndexTask(): hash index has been recovered");
        }

    }

    private void executeRecoverSequenceTask() {
        SequenceRecoverTask sequenceRecoverTask = new SequenceRecoverTask();
        Future<?> future = executorPool.submit(sequenceRecoverTask);


        while (!future.isDone()) {
            LOGGER.log(Level.FINE, "recoverSequenceTask(): waiting while sequence recovering will be finished");
        }

        if (future.isDone()) {
            LOGGER.log(Level.INFO, "recoverSequenceTask(): sequences has been recovered");
        }

    }

    public void shutdown() throws InterruptedException {

        executorPool.awaitTermination(10, TimeUnit.SECONDS);
        executorPool.shutdown();

        monitor.shutdown();
    }
}
