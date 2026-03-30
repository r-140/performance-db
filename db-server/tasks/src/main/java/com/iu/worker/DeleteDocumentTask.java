package com.iu.worker;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.FindDocumentTask.lock;

class DeleteDocumentTask extends AbstractTask {
    private static final Logger LOGGER = Logger.getLogger(DeleteDocumentTask.class.getName());
    private static final String REPLACE_ID_PATTERN = "{id}";

    DeleteDocumentTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.writeLock();
        try {
            LOGGER.log(Level.INFO, "DeleteDocumentTask: " + taskPayload);
            final Integer idVal = (Integer) JsonHelper.getValueFromJsonByKey(taskPayload, "id");
            final String found  = FileHelper.findLineInFileByIdField(PATH_TO_DATA_FILE, idVal);

            if (found == null || found.isEmpty()) {
                String msg = ErrorCode.DOCUMENTNOTFOUND.getErrorMessage()
                        .replace(REPLACE_ID_PATTERN, String.valueOf(idVal));
                writeResponse(connection, JsonHelper.buildErrorResponse(
                        ErrorCode.DOCUMENTNOTFOUND.getErrorCode(), msg, ""));
                return null;
            }

            FileHelper.removeLineFromFile(PATH_TO_DATA_FILE, found);
            for (IndexTypes it : IndexTypes.values()) {
                if (!it.equals(IndexTypes.NONE)) it.deleteAddrFromIndex(idVal);
            }
            writeResponse(connection, found);
        } catch (IOException e) {
            writeResponse(connection, JsonHelper.buildErrorResponse(
                    ErrorCode.IOEXCEPTION.getErrorCode(),
                    ErrorCode.IOEXCEPTION.getErrorMessage(), e.getMessage()));
            LOGGER.log(Level.SEVERE, "DeleteDocumentTask IO error", e);
        } finally {
            lock.unlockWrite(stamp);
            closeQuietly(connection);
        }
        return null;
    }
}
