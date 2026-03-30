package com.iu.worker;

import java.net.Socket;

public enum TaskType {
    APPEND      ("append"),
    FIND        ("find"),
    CREATE_INDEX("createIndex"),
    DELETE_INDEX("deleteIndex"),
    DELETE_DOC  ("delete"),
    DELETE_DB   ("deleteDb"),
    SQL         ("sql");

    private final String wire;
    TaskType(String wire) { this.wire = wire; }
    public String getTaskType() { return wire; }

    public static TaskType getTaskByType(String taskType) {
        return switch (taskType) {
            case "append"      -> APPEND;
            case "find"        -> FIND;
            case "createIndex" -> CREATE_INDEX;
            case "deleteIndex" -> DELETE_INDEX;
            case "delete"      -> DELETE_DOC;
            case "deleteDb"    -> DELETE_DB;
            case "sql"         -> SQL;
            default            -> null;
        };
    }

    public AbstractTask getTask(Socket connection, String taskPayload) {
        return switch (this) {
            case APPEND       -> new AppendDocumentTask(connection, taskPayload);
            case FIND         -> new FindDocumentTask(connection, taskPayload);
            case CREATE_INDEX -> new CreateIndexTask(connection, taskPayload);
            case DELETE_INDEX -> new DeleteIndexTask(connection, taskPayload);
            case DELETE_DOC   -> new DeleteDocumentTask(connection, taskPayload);
            case DELETE_DB    -> new DeleteDBTask(connection, taskPayload);
            case SQL          -> new SqlQueryTask(connection, taskPayload);
        };
    }
}
