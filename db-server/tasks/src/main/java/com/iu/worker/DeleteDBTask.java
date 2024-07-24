package com.iu.worker;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.util.IndexHelper.checkIndexExistence;
import static com.iu.worker.util.IndexHelper.deleteIndexFromRegistry;

class DeleteDBTask extends AbstractTask {

    private static final Logger LOGGER = Logger.getLogger(DeleteDBTask.class.getName());

    private static final StampedLock lock = new StampedLock();

    DeleteDBTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.writeLock();
        try {
            LOGGER.log(Level.INFO, String.format("Delete db task %s", taskPayload));

            FileHelper.removeFile(PATH_TO_DATA_FILE);

            for (IndexTypes indexTypes : IndexTypes.values()) {
                if (indexTypes.equals(IndexTypes.NONE)) {
                    continue;
                }
                indexTypes.deleteIndex();
            }

            writeResponse(connection, "the database was deleted");

        } catch (IOException e) {
            //report exception somewhere.
            writeResponse(connection, JsonHelper.buildErrorResponse(ErrorCode.IOEXCEPTION.getErrorCode(),
                    ErrorCode.IOEXCEPTION.getErrorMessage(), e.getMessage()));
            e.printStackTrace();
        } finally {
            lock.unlockWrite(stamp);
            try {
                connection.close();
            } catch (IOException e) {
// ignore;
            }
        }
        return null;
    }


}
