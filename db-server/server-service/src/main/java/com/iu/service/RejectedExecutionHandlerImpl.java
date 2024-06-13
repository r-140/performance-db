package com.iu.service;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;

class RejectedExecutionHandlerImpl implements RejectedExecutionHandler {
    private static final Logger LOGGER = Logger.getLogger(RejectedExecutionHandlerImpl.class.getName());

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        LOGGER.log(Level.FINER, String.format("%s is rejected", r.toString()));
    }

}