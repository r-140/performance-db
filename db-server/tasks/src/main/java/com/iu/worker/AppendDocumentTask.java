package com.iu.worker;

import com.files.FileHelper;
import com.iu.indexes.IndexService;
import com.iu.indexes.IndexTypes;
import com.iu.worker.util.SequenceGenerator;
import com.json.JsonHelper;
import com.util.CommonConsts;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.util.IndexHelper.checkIndexExistence;

class AppendDocumentTask extends AbstractTask {

    private static final Logger LOGGER = Logger.getLogger(AppendDocumentTask.class.getName());

    private static final StampedLock lock = new StampedLock();

    AppendDocumentTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.writeLock();
        try {
            final int id = SequenceGenerator.INSTANCE.generateId();
            LOGGER.log(Level.INFO, String.format("Append task %s, id %s", taskPayload, id));
            final String docStr = generateStringToSave(id, taskPayload);
            LOGGER.log(Level.FINE, String.format("Append task docStr %s", docStr));
            long offset = FileHelper.writeToFile(PATH_TO_DATA_FILE, docStr, true);
            LOGGER.log(Level.FINEST, String.format("offset %s", offset));
//            updating indexes
            for (IndexTypes indexTypes : IndexTypes.values()) {
                if (indexTypes.equals(IndexTypes.NONE))
                    continue;
                final String indexType = indexTypes.getIndexType();
                boolean isIndexExist = checkIndexExistence(PATH_TO_INDEX_REGISTRY, indexType);
                LOGGER.log(Level.INFO, String.format("Append task index type %s exists ? %s", indexType, isIndexExist));

                if (isIndexExist) {
                    Integer idVal = (Integer) JsonHelper.getValueFromJsonByKey(taskPayload, "id");
                    indexTypes.addValueToIndex(idVal, offset);
                }
            }

            writeResponse(connection, String.format("Document with id %s has been created", id));
            LOGGER.log(Level.FINEST, String.format("Request processed offset: %s", offset));
        } catch (IOException e) {
            //report exception somewhere.
            e.printStackTrace();
        } finally {
            lock.unlockWrite(stamp);
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    private String generateStringToSave(final int id, final String baseString) {
        return id + CommonConsts.ID_SEPARATOR + baseString;
    }
}
