package com.iu.worker;

import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.util.IndexHelper.*;

class DeleteIndexTask extends AbstractTask {

    private static final Logger LOGGER = Logger.getLogger(DeleteIndexTask.class.getName());

    private static final String REPLACE_INDEX_TYPE_PATTERN = "{indexType}";

    private static final StampedLock lock = new StampedLock();

    DeleteIndexTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.writeLock();
        try {
            LOGGER.log(Level.INFO, String.format("Delete index task %s", taskPayload));
            IndexTypes indexType = IndexTypes.getIndexByType(taskPayload);
            if(indexType != null && !IndexTypes.NONE.equals(indexType)) {
                boolean isIndexExist = checkIndexExistence(PATH_TO_INDEX_REGISTRY, taskPayload);
                LOGGER.log(Level.INFO, String.format("DeleteIndexTask: is index exists ?  %s", isIndexExist));

                String responseBody;
                if (isIndexExist) {
                    indexType.deleteIndex();
                    deleteIndexFromRegistry(PATH_TO_INDEX_REGISTRY, taskPayload);
                    responseBody =  "Index with the type " + taskPayload + " has been deleted";

                } else {
                    String errorMessage = ErrorCode.INDEXDOESNOTEXIST.getErrorMessage().replace(REPLACE_INDEX_TYPE_PATTERN, taskPayload);
                    responseBody = JsonHelper.buildErrorResponse(ErrorCode.INDEXDOESNOTEXIST.getErrorCode(), errorMessage, "");
                }

                writeResponse(connection, responseBody);

            } else {
                LOGGER.info("Unexpected index type");
                writeResponse(connection, JsonHelper.buildErrorResponse(ErrorCode.UNEXPECTEDINDEXTYPE.getErrorCode(),
                        ErrorCode.UNEXPECTEDINDEXTYPE.getErrorMessage(), ""));
            }
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
