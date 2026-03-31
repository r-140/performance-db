package com.iu.worker;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.json.JsonHelper;

import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.SharedLock.lock;

class DeleteDBTask extends AbstractTask {
    private static final Logger LOGGER = Logger.getLogger(DeleteDBTask.class.getName());

    DeleteDBTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    protected void execute() throws Exception {
        long stamp = lock.writeLock();
        try {
            FileHelper.removeFile(PATH_TO_DATA_FILE);
            for (IndexTypes it : IndexTypes.values()) {
                if (!it.equals(IndexTypes.NONE)) {
                    try { it.deleteIndex(); }
                    catch (Exception e) { LOGGER.log(Level.WARNING, "deleteIndex failed for " + it.getIndexType(), e); }
                }
            }
            writeResponse("the database was deleted");
        } finally {
            lock.unlockWrite(stamp);
            closeQuietly();
        }
    }
}
