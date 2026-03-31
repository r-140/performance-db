package com.iu.worker;

import com.files.FileHelper;
import com.iu.indexes.IndexTypes;
import com.iu.worker.util.SequenceGenerator;
import com.json.JsonHelper;
import com.util.CommonConsts;

import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.SharedLock.lock;
import static com.iu.worker.util.IndexHelper.checkIndexExistence;

class AppendDocumentTask extends AbstractTask {
    private static final Logger LOGGER = Logger.getLogger(AppendDocumentTask.class.getName());

    AppendDocumentTask(Socket connection, String taskPayload) {
        super(connection, taskPayload);
    }

    @Override
    protected void execute() throws Exception {
        long stamp = lock.writeLock();
        try {
            final int    id     = SequenceGenerator.INSTANCE.generateId();
            final String docStr = id + CommonConsts.ID_SEPARATOR + taskPayload;
            LOGGER.log(Level.INFO, "AppendDocumentTask id=" + id);

            long offset = FileHelper.writeToFile(PATH_TO_DATA_FILE, docStr, true);

            for (IndexTypes idx : IndexTypes.values()) {
                if (idx.equals(IndexTypes.NONE)) continue;
                if (checkIndexExistence(PATH_TO_INDEX_REGISTRY, idx.getIndexType())) {
                    Integer idVal = (Integer) JsonHelper.getValueFromJsonByKey(taskPayload, "id");
                    idx.addValueToIndex(idVal, offset);
                }
            }
            writeResponse(docStr);
        } finally {
            lock.unlockWrite(stamp);
            closeQuietly();
        }
    }
}
