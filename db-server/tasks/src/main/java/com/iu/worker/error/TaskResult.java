package com.iu.worker.error;

/**
 * Sealed result type for task execution (Java 17/21 sealed + records).
 *
 * Tasks currently return a raw String or null, making it impossible for the
 * compiler to distinguish "operation succeeded with this string" from
 * "operation failed". TaskResult makes the distinction explicit and forces
 * callers to handle both cases via pattern-matching switch.
 *
 * Usage:
 *   TaskResult result = computeSomething();
 *   String wire = switch (result) {
 *       case TaskResult.Ok ok       -> ok.body();
 *       case TaskResult.Failure f   -> f.error().toJson();
 *   };
 */
public sealed interface TaskResult
        permits TaskResult.Ok, TaskResult.Failure {

    record Ok(String body)       implements TaskResult {}
    record Failure(DbError error) implements TaskResult {}

    static TaskResult ok(String body)        { return new Ok(body); }
    static TaskResult fail(DbError error)    { return new Failure(error); }

    /** Converts to the wire string in one line. */
    default String toWire() {
        return switch (this) {
            case Ok ok         -> ok.body();
            case Failure f     -> f.error().toJson();
        };
    }
}
