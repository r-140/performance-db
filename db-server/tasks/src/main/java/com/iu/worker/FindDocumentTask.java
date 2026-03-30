package com.iu.worker;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles "find" requests.
 *
 * Uses a READ stamp so multiple concurrent finds can proceed in parallel.
 * The original code used writeLock() for reads, which serialised all queries.
 */
class FindDocumentTask extends AbstractTask {

    private static final Logger LOGGER = Logger.getLogger(FindDocumentTask.class.getName());

    /** Shared with write tasks — same lock instance via static field. */
    static final StampedLock lock = new StampedLock();

    FindDocumentTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        // READ lock — allows concurrent reads
        long stamp = lock.readLock();
        try {
            LOGGER.log(Level.INFO, "Find document task: " + taskPayload);
            final Integer idVal    = (Integer) JsonHelper.getValueFromJsonByKey(taskPayload, "id");
            final String indexType = (String)  JsonHelper.getValueFromJsonByKey(taskPayload, "indexType");

            LOGGER.log(Level.FINE, "Find id=" + idVal + " indexType=" + indexType);

            IndexTypes indexTypes = IndexTypes.getIndexByType(indexType);
            String result;

            if (indexTypes == null || indexTypes.equals(IndexTypes.NONE)) {
                // Full file scan — O(N)
                result = FileHelper.findLineInFileByIdField(PATH_TO_DATA_FILE, idVal);
            } else {
                Long offset = (Long) indexTypes.findAddrInIndex(idVal);
                LOGGER.log(Level.INFO, "Index offset=" + offset);
                result = offset != null
                        ? FileHelper.findLineByOffset(PATH_TO_DATA_FILE, offset)
                        : FileHelper.findLineInFileByIdField(PATH_TO_DATA_FILE, idVal);
            }

            LOGGER.log(Level.INFO, "Find result: " + result);
            writeResponse(connection, result);

        } catch (IOException e) {
            writeResponse(connection, JsonHelper.buildErrorResponse(
                    ErrorCode.IOEXCEPTION.getErrorCode(),
                    ErrorCode.IOEXCEPTION.getErrorMessage(), e.getMessage()));
            LOGGER.log(Level.SEVERE, "FindDocumentTask IO error", e);
        } finally {
            lock.unlockRead(stamp);
            closeQuietly(connection);
        }
        return null;
    }
}
