package com.message;

import java.io.Serial;
import java.io.Serializable;

/**
 * Wire message sent over the socket.
 *
 * Changed from a hand-rolled POJO to a Java record (JEP 395, Java 16).
 * Records are:
 *  - Immutable by default (all fields final)
 *  - Auto-generate constructor, accessors, equals, hashCode, toString
 *  - Implement Serializable so existing ObjectOutputStream protocol is unchanged
 *
 * Accessor names change: getTaskType() → taskType(), getPayload() → payload().
 * All callers updated accordingly.
 */
public record MessageBean(String taskType, String payload) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    /** Compact canonical constructor — validates invariants at construction time. */
    public MessageBean {
        if (taskType == null || taskType.isBlank())
            throw new IllegalArgumentException("taskType must not be blank");
        if (payload == null)
            throw new IllegalArgumentException("payload must not be null");
    }
}
