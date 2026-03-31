package com.iu.worker;

import com.files.FileHelper;
import com.iu.worker.util.SequenceGenerator;
import com.util.CollectionsUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.DISC_PATH;
import static com.iu.worker.AbstractTask.PATH_TO_DATA_FILE;

/**
 * Recover the sequence generator on server restart.
 *
 * Reads the existing data file (if any) to find the highest existing id
 * so the next generated id does not collide with persisted data.
 *
 * If the data file does not exist yet (fresh start), initialises the
 * sequence from 0 and creates the data directory.
 */
public class SequenceRecoverTask implements Callable<Void> {
    private static final Logger LOGGER = Logger.getLogger(SequenceRecoverTask.class.getName());

    @Override
    public Void call() {
        // Ensure the data directory exists
        FileHelper.createDirectoryIfNotExist(DISC_PATH);

        if (!FileHelper.isFileExist(PATH_TO_DATA_FILE)) {
            LOGGER.log(Level.INFO, "SequenceRecoverTask: data file absent — starting sequence from 0");
            SequenceGenerator.INSTANCE.init(0);
            return null;
        }

        try {
            Map<Integer, Long> data = FileHelper.readFile(PATH_TO_DATA_FILE, false);
            int maxValue  = CollectionsUtil.findMaxKeyInMap(data); // returns -1 if empty
            int nextValue = maxValue + 1;
            LOGGER.log(Level.INFO, "SequenceRecoverTask: next id = " + nextValue
                + " (data file has " + data.size() + " docs)");
            SequenceGenerator.INSTANCE.init(nextValue);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "SequenceRecoverTask: could not read data file, starting from 0", e);
            SequenceGenerator.INSTANCE.init(0);
        }
        return null;
    }
}
