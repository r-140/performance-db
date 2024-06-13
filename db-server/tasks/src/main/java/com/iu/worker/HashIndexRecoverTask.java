package com.iu.worker;

import com.iu.indexes.IndexTypes;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.PATH_TO_INDEX_REGISTRY;
import static com.iu.worker.util.IndexHelper.checkIndexExistence;

public class HashIndexRecoverTask implements Callable<Void> {
    private static final Logger LOGGER = Logger.getLogger(HashIndexRecoverTask.class.getName());

    public HashIndexRecoverTask() {
    }

    @Override
    public Void call() {

        try {
            boolean isRecoveryNeeded = checkIndexExistence(PATH_TO_INDEX_REGISTRY, IndexTypes.HASH_INDEX.getIndexType());

            LOGGER.log(Level.INFO, String.format("HashIndexRecoverTask(): isRecoveryNeeded ? %s", isRecoveryNeeded));

            if (isRecoveryNeeded) {
                IndexTypes.HASH_INDEX.recoverIndex(AbstractTask.SNAPSHOT_FILE);

                LOGGER.log(Level.INFO, "HashIndexRecoverTask(): index recovered");
            } else {
                LOGGER.log(Level.INFO, "hash indexes did not exist");
            }
        } catch (IOException e) {
            //report exception somewhere.
            e.printStackTrace();
        }
        return null;
    }
}
