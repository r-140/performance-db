package com.iu.worker;

import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.FindDocumentTask.lock;
import static com.iu.worker.util.IndexHelper.checkIndexExistence;
import static com.iu.worker.util.IndexHelper.deleteIndexFromRegistry;

class DeleteIndexTask extends AbstractTask {
    private static final Logger LOGGER = Logger.getLogger(DeleteIndexTask.class.getName());
    private static final String REPLACE_INDEX_TYPE_PATTERN = "{indexType}";

    DeleteIndexTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.writeLock();
        try {
            LOGGER.log(Level.INFO, "DeleteIndexTask: " + taskPayload);
            IndexTypes indexType = IndexTypes.getIndexByType(taskPayload);
            if (indexType == null || IndexTypes.NONE.equals(indexType)) {
                writeResponse(connection, JsonHelper.buildErrorResponse(
                        ErrorCode.UNEXPECTEDINDEXTYPE.getErrorCode(),
                        ErrorCode.UNEXPECTEDINDEXTYPE.getErrorMessage(), ""));
                return null;
            }
            boolean exists = checkIndexExistence(PATH_TO_INDEX_REGISTRY, taskPayload);
            if (exists) {
                indexType.deleteIndex();
                deleteIndexFromRegistry(PATH_TO_INDEX_REGISTRY, taskPayload);
                writeResponse(connection, "Index with the type " + taskPayload + " has been deleted");
            } else {
                String msg = ErrorCode.INDEXDOESNOTEXIST.getErrorMessage()
                        .replace(REPLACE_INDEX_TYPE_PATTERN, taskPayload);
                writeResponse(connection, JsonHelper.buildErrorResponse(
                        ErrorCode.INDEXDOESNOTEXIST.getErrorCode(), msg, ""));
            }
        } catch (IOException e) {
            writeResponse(connection, JsonHelper.buildErrorResponse(
                    ErrorCode.IOEXCEPTION.getErrorCode(),
                    ErrorCode.IOEXCEPTION.getErrorMessage(), e.getMessage()));
            LOGGER.log(Level.SEVERE, "DeleteIndexTask IO error", e);
        } finally {
            lock.unlockWrite(stamp);
            closeQuietly(connection);
        }
        return null;
    }
}
