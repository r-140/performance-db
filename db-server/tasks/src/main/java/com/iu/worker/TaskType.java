package com.iu.worker;

import java.net.Socket;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;


public enum TaskType {
    APPEND("append") {
        @Override
        public AbstractTask getTask(Socket connection, String taskPayload) {
            return new AppendDocumentTask(connection, taskPayload);
        }
    },
    FIND("find") {
        @Override
        public AbstractTask getTask(Socket connection, String taskPayload) {
            return new FindDocumentTask(connection, taskPayload);
        }
    },
    CREATE_INDEX("createIndex") {
        @Override
        public AbstractTask getTask(Socket connection, String taskPayload) {
            return new CreateIndexTask(connection, taskPayload);
        }
    };

    private final String taskType;

    TaskType(String taskType) {
        this.taskType = taskType;
    }

    public static TaskType getTaskByType(final String taskType) {
        for (TaskType taskType1 : TaskType.values()) {
            if (taskType1.getTaskType().equals(taskType))
                return taskType1;
        }
        return null;
    }

    public String getTaskType() {
        return taskType;
    }

    public abstract AbstractTask getTask(Socket connection, String taskPayload);
}
