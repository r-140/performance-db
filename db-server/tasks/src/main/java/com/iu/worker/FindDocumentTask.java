package com.iu.worker;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Level;
import java.util.logging.Logger;

class FindDocumentTask extends AbstractTask {

    private static final Logger LOGGER = Logger.getLogger(FindDocumentTask.class.getName());

    private static final StampedLock lock = new StampedLock();

    FindDocumentTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.writeLock();
        try {
            LOGGER.log(Level.INFO, String.format("Find document task: %s", taskPayload));
            final Integer idVal = (Integer) JsonHelper.getValueFromJsonByKey(taskPayload, "id");

            final String indexType = (String) JsonHelper.getValueFromJsonByKey(taskPayload, "indexType");

            LOGGER.log(Level.FINE, String.format("Find document task: id %s, indexType %s", idVal, indexType));

            IndexTypes indexTypes = IndexTypes.getIndexByType(indexType);
            String result;
            if (indexTypes == null || indexTypes.equals(IndexTypes.NONE)) {
                result = FileHelper.findLineInFileByIdField(PATH_TO_DATA_FILE, idVal);
            } else {
                Long offset = (Long) indexTypes.findAddrInIndex(idVal);

                LOGGER.log(Level.INFO, String.format("find document offset %s", offset));

                result = offset != null ? FileHelper.findLineByOffset(PATH_TO_DATA_FILE, offset)
                        : FileHelper.findLineInFileByIdField(PATH_TO_DATA_FILE, idVal);
            }

            LOGGER.log(Level.INFO, String.format("Find document: found result %s", result));
            writeResponse(connection, result);

        } catch (IOException e) {
            //report exception somewhere.
            writeResponse(connection, e.getMessage());
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
