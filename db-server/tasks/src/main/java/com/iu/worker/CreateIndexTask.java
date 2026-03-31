package com.iu.worker;

import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.SharedLock.lock;
import static com.iu.worker.util.IndexHelper.addIndexToRegistry;
import static com.iu.worker.util.IndexHelper.checkIndexExistence;

class CreateIndexTask extends AbstractTask {
    private static final Logger LOGGER = Logger.getLogger(CreateIndexTask.class.getName());

    CreateIndexTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    protected void execute() throws Exception {
        long stamp = lock.writeLock();
        try {
            LOGGER.log(Level.INFO, "CreateIndexTask: " + taskPayload);
            IndexTypes indexType = IndexTypes.getIndexByType(taskPayload);
            if (indexType == null || IndexTypes.NONE.equals(indexType)) {
                writeResponse(JsonHelper.buildErrorResponse(
                    ErrorCode.UNEXPECTEDINDEXTYPE.getErrorCode(),
                    ErrorCode.UNEXPECTEDINDEXTYPE.getErrorMessage(), ""));
                return;
            }
            if (!checkIndexExistence(PATH_TO_INDEX_REGISTRY, taskPayload)) {
                indexType.createIndex(DISC_PATH + "/" + indexType.getIndexFileName());
                addIndexToRegistry(PATH_TO_INDEX_REGISTRY, taskPayload);
                writeResponse("Index with the type " + taskPayload + " has been created");
            } else {
                writeResponse(JsonHelper.buildErrorResponse(
                    ErrorCode.INDEXEXIST.getErrorCode(),
                    ErrorCode.INDEXEXIST.getErrorMessage()
                        .replace("{indexType}", taskPayload), ""));
            }
        } finally {
            lock.unlockWrite(stamp);
            closeQuietly();
        }
    }
}
