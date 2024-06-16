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

class DeleteDocumentTask extends AbstractTask {

    private static final Logger LOGGER = Logger.getLogger(DeleteDocumentTask.class.getName());

    private static final StampedLock lock = new StampedLock();

    DeleteDocumentTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    public Void call() {
        long stamp = lock.writeLock();
        try {
            LOGGER.log(Level.INFO, String.format("Find document task: %s", taskPayload));
            final Integer idVal = (Integer) JsonHelper.getValueFromJsonByKey(taskPayload, "id");

            String result = FileHelper.findLineInFileByIdField(PATH_TO_DATA_FILE, idVal);

            LOGGER.log(Level.INFO, String.format("Find document: found result %s", result));
            FileHelper.removeLineFromFile(PATH_TO_DATA_FILE, result);

            for (IndexTypes indexTypes : IndexTypes.values()) {
                if(indexTypes.equals(IndexTypes.NONE)) {
                    continue;
                }
                indexTypes.deleteAddrFromIndex(idVal);
            }
            ObjectOutputStream oos = new ObjectOutputStream(connection.getOutputStream());
            if (result != null && !result.isEmpty())
                oos.writeObject(result);
            else
                oos.writeObject("");
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
