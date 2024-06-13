package com.iu.service;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

class MonitorThread implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(MonitorThread.class.getName());
    private final ThreadPoolExecutor executor;
    private final int seconds;
    private boolean run = true;

    MonitorThread(ThreadPoolExecutor executor, int delay) {
        this.executor = executor;
        this.seconds = delay;
    }

    void shutdown() {
        this.run = false;
    }

    @Override
    public void run() {
        while (run) {
            LOGGER.log(Level.FINEST,
                    String.format("[monitor] [%d/%d] Active: %d, Completed: %d, Task: %d, isShutdown: %s, isTerminated: %s",
                            this.executor.getPoolSize(),
                            this.executor.getCorePoolSize(),
                            this.executor.getActiveCount(),
                            this.executor.getCompletedTaskCount(),
                            this.executor.getTaskCount(),
                            this.executor.isShutdown(),
                            this.executor.isTerminated()));
            try {
                Thread.sleep(seconds * 1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}