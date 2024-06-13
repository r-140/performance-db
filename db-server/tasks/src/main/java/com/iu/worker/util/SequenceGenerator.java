package com.iu.worker.util;

import java.util.concurrent.atomic.AtomicInteger;

public enum SequenceGenerator {
    INSTANCE;

    private AtomicInteger uniqueId;

    public void init(int initialValue) {
        uniqueId = new AtomicInteger(initialValue);
    }

    public int generateId() {
        return uniqueId.getAndIncrement();
    }
}
