package com.iu.worker;

import com.iu.indexes.IndexTypes;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.PATH_TO_INDEX_REGISTRY;
import static com.iu.worker.AbstractTask.SNAPSHOT_FILE;
import static com.iu.worker.util.IndexHelper.checkIndexExistence;

/**
 * Recover the hash index from its snapshot file on server restart.
 *
 * Only runs if the index registry shows the hash index existed before the
 * server stopped. If neither the registry nor the snapshot exists this is
 * a fresh start — no-op.
 */
public class HashIndexRecoverTask implements Callable<Void> {
    private static final Logger LOGGER = Logger.getLogger(HashIndexRecoverTask.class.getName());

    @Override
    public Void call() {
        try {
            boolean needed = checkIndexExistence(PATH_TO_INDEX_REGISTRY,
                IndexTypes.HASH_INDEX.getIndexType());
            LOGGER.log(Level.INFO, "HashIndexRecoverTask: recovery needed = " + needed);

            if (needed) {
                IndexTypes.HASH_INDEX.recoverIndex(SNAPSHOT_FILE);
                LOGGER.log(Level.INFO, "HashIndexRecoverTask: hash index recovered");
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING,
                "HashIndexRecoverTask: could not recover hash index (may not exist yet)", e);
        }
        return null;
    }
}
