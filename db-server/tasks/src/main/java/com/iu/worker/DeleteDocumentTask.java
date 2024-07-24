package com.iu.worker;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Level;
import java.util.logging.Logger;

class DeleteDocumentTask extends AbstractTask {

    private static final Logger LOGGER = Logger.getLogger(DeleteDocumentTask.class.getName());

    private static final String REPLACE_ID_PATTERN = "{id}";

    private static final StampedLock lock = new StampedLock();

    DeleteDocumentTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.writeLock();
        try {
            LOGGER.log(Level.INFO, String.format("Delete document task: %s", taskPayload));
            final Integer idVal = (Integer) JsonHelper.getValueFromJsonByKey(taskPayload, "id");

            final String found = FileHelper.findLineInFileByIdField(PATH_TO_DATA_FILE, idVal);
            if(found == null || found.isEmpty()) {
                String errorMessage = ErrorCode.DOCUMENTNOTFOUND.getErrorMessage().replace(REPLACE_ID_PATTERN, taskPayload);
                String responseBody = JsonHelper.buildErrorResponse(ErrorCode.DOCUMENTNOTFOUND.getErrorCode(), errorMessage, "");
                writeResponse(connection, responseBody);
            } else {

                LOGGER.log(Level.INFO, String.format("Delete document: found document %s", found));
                FileHelper.removeLineFromFile(PATH_TO_DATA_FILE, found);

                for (IndexTypes indexTypes : IndexTypes.values()) {
                    if (indexTypes.equals(IndexTypes.NONE)) {
                        continue;
                    }
                    indexTypes.deleteAddrFromIndex(idVal);
                }
            }
            writeResponse(connection, found);
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
