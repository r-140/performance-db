package com.iu.worker;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.SharedLock.lock;

class DeleteDocumentTask extends AbstractTask {
    private static final Logger LOGGER = Logger.getLogger(DeleteDocumentTask.class.getName());

    DeleteDocumentTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    protected void execute() throws Exception {
        long stamp = lock.writeLock();
        try {
            final Integer idVal = (Integer) JsonHelper.getValueFromJsonByKey(taskPayload, "id");
            final String  found = FileHelper.findLineInFileByIdField(PATH_TO_DATA_FILE, idVal);

            if (found == null || found.isEmpty()) {
                writeResponse(JsonHelper.buildErrorResponse(
                    ErrorCode.DOCUMENTNOTFOUND.getErrorCode(),
                    ErrorCode.DOCUMENTNOTFOUND.getErrorMessage().replace("{id}", String.valueOf(idVal)), ""));
                return;
            }
            FileHelper.removeLineFromFile(PATH_TO_DATA_FILE, found);
            for (IndexTypes it : IndexTypes.values()) {
                if (!it.equals(IndexTypes.NONE)) it.deleteAddrFromIndex(idVal);
            }
            writeResponse(found);
        } finally {
            lock.unlockWrite(stamp);
            closeQuietly();
        }
    }
}
