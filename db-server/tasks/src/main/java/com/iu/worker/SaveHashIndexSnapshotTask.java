package com.iu.worker;

import com.iu.indexes.IndexTypes;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.PATH_TO_INDEX_REGISTRY;
import static com.iu.worker.AbstractTask.SNAPSHOT_FILE;
import static com.iu.worker.util.IndexHelper.checkIndexExistence;
import static com.iu.worker.util.IndexHelper.writeHashIndexToDisc;

public class SaveHashIndexSnapshotTask implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(SaveHashIndexSnapshotTask.class.getName());

    public SaveHashIndexSnapshotTask() {
    }

    @Override
    public void run() {
        try {
            boolean isIndexExist = checkIndexExistence(PATH_TO_INDEX_REGISTRY, IndexTypes.HASH_INDEX.getIndexType());

            LOGGER.log(Level.INFO, String.format("SaveHashIndexSnapshotTask(): isIndexExist ? %s", isIndexExist));

            if (isIndexExist) {
                writeHashIndexToDisc(SNAPSHOT_FILE);
            }

        } catch (IOException e) {
            //report exception somewhere.
            e.printStackTrace();
        }
    }
}
