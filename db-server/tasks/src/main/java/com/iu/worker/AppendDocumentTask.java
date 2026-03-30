package com.iu.worker;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.iu.worker.util.SequenceGenerator;
import com.json.JsonHelper;
import com.util.CommonConsts;

import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.FindDocumentTask.lock;
import static com.iu.worker.util.IndexHelper.checkIndexExistence;

class AppendDocumentTask extends AbstractTask {

    private static final Logger LOGGER = Logger.getLogger(AppendDocumentTask.class.getName());

    AppendDocumentTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.writeLock();
        try {
            final int    id     = SequenceGenerator.INSTANCE.generateId();
            final String docStr = id + CommonConsts.ID_SEPARATOR + taskPayload;
            LOGGER.log(Level.INFO, "AppendDocumentTask id=" + id);

            long offset = FileHelper.writeToFile(PATH_TO_DATA_FILE, docStr, true);

            for (IndexTypes indexTypes : IndexTypes.values()) {
                if (indexTypes.equals(IndexTypes.NONE)) continue;
                boolean exists = checkIndexExistence(PATH_TO_INDEX_REGISTRY, indexTypes.getIndexType());
                if (exists) {
                    Integer idVal = (Integer) JsonHelper.getValueFromJsonByKey(taskPayload, "id");
                    indexTypes.addValueToIndex(idVal, offset);
                }
            }

            writeResponse(connection, docStr);
        } catch (IOException e) {
            writeResponse(connection, JsonHelper.buildErrorResponse(
                    ErrorCode.IOEXCEPTION.getErrorCode(),
                    ErrorCode.IOEXCEPTION.getErrorMessage(), e.getMessage()));
            LOGGER.log(Level.SEVERE, "AppendDocumentTask IO error", e);
        } finally {
            lock.unlockWrite(stamp);
            closeQuietly(connection);
        }
        return null;
    }
}
