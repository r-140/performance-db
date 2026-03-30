package com.iu.worker;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.FindDocumentTask.lock;

class DeleteDBTask extends AbstractTask {
    private static final Logger LOGGER = Logger.getLogger(DeleteDBTask.class.getName());

    DeleteDBTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.writeLock();
        try {
            LOGGER.log(Level.INFO, "DeleteDBTask");
            FileHelper.removeFile(PATH_TO_DATA_FILE);
            for (IndexTypes it : IndexTypes.values()) {
                if (!it.equals(IndexTypes.NONE)) {
                    try { it.deleteIndex(); } catch (Exception e) {
                        LOGGER.log(Level.WARNING, "deleteIndex failed for " + it.getIndexType(), e);
                    }
                }
            }
            writeResponse(connection, "the database was deleted");
        } catch (IOException e) {
            writeResponse(connection, JsonHelper.buildErrorResponse(
                    ErrorCode.IOEXCEPTION.getErrorCode(),
                    ErrorCode.IOEXCEPTION.getErrorMessage(), e.getMessage()));
            LOGGER.log(Level.SEVERE, "DeleteDBTask IO error", e);
        } finally {
            lock.unlockWrite(stamp);
            closeQuietly(connection);
        }
        return null;
    }
}
