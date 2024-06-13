package com.message;

import java.io.Serializable;

public class MessageBean implements Serializable {

    private final String taskType;
    private final String payload;

    public MessageBean(String taskType, String payload) {
        this.taskType = taskType;
        this.payload = payload;
    }

    public String getTaskType() {
        return taskType;
    }

    public String getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "MessageBean{" +
                "taskType='" + taskType + '\'' +
                ", payload='" + payload + '\'' +
                '}';
    }
}
