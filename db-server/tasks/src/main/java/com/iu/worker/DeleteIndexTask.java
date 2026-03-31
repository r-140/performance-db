package com.iu.worker;

import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.SharedLock.lock;
import static com.iu.worker.util.IndexHelper.checkIndexExistence;
import static com.iu.worker.util.IndexHelper.deleteIndexFromRegistry;

class DeleteIndexTask extends AbstractTask {
    private static final Logger LOGGER = Logger.getLogger(DeleteIndexTask.class.getName());

    DeleteIndexTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    protected void execute() throws Exception {
        long stamp = lock.writeLock();
        try {
            IndexTypes indexType = IndexTypes.getIndexByType(taskPayload);
            if (indexType == null || IndexTypes.NONE.equals(indexType)) {
                writeResponse(JsonHelper.buildErrorResponse(
                    ErrorCode.UNEXPECTEDINDEXTYPE.getErrorCode(),
                    ErrorCode.UNEXPECTEDINDEXTYPE.getErrorMessage(), ""));
                return;
            }
            if (checkIndexExistence(PATH_TO_INDEX_REGISTRY, taskPayload)) {
                indexType.deleteIndex();
                deleteIndexFromRegistry(PATH_TO_INDEX_REGISTRY, taskPayload);
                writeResponse("Index with the type " + taskPayload + " has been deleted");
            } else {
                writeResponse(JsonHelper.buildErrorResponse(
                    ErrorCode.INDEXDOESNOTEXIST.getErrorCode(),
                    ErrorCode.INDEXDOESNOTEXIST.getErrorMessage()
                        .replace("{indexType}", taskPayload), ""));
            }
        } finally {
            lock.unlockWrite(stamp);
            closeQuietly();
        }
    }
}
