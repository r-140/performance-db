package com.iu.worker;

import com.iu.indexes.IndexTypes;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.util.IndexHelper.addIndexToRegistry;
import static com.iu.worker.util.IndexHelper.checkIndexExistence;

class CreateIndexTask extends AbstractTask {

    private static final Logger LOGGER = Logger.getLogger(CreateIndexTask.class.getName());

    private static final StampedLock lock = new StampedLock();

    CreateIndexTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.writeLock();
        try {
            LOGGER.log(Level.INFO, String.format("Create index task %s", taskPayload));
            IndexTypes indexType = IndexTypes.getIndexByType(taskPayload);
            if(indexType != null && !IndexTypes.NONE.equals(indexType)) {
                boolean isIndexExist = checkIndexExistence(PATH_TO_INDEX_REGISTRY, taskPayload);
                LOGGER.log(Level.INFO, String.format("CreateIndexTask: is index exists ?  %s", isIndexExist));

                String responseBody;
                if (!isIndexExist) {
                    indexType.createIndex(DISC_PATH + "/" + indexType.getIndexFileName());

                    addIndexToRegistry(PATH_TO_INDEX_REGISTRY, taskPayload);
                    responseBody = "Index with the type " + taskPayload + " has been created";
                } else {
                    responseBody = "Index with the type " + taskPayload + " already exist";
                }
                writeResponse(connection, responseBody);
            } else {
                LOGGER.info("Unexpected index type");
                writeResponse(connection, "Unexpected Index Type");
            }
        } catch (IOException e) {
            //report exception somewhere.
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
