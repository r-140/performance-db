package com.iu.worker;

import com.files.FileHelper;
import com.iu.worker.util.SequenceGenerator;
import com.util.CollectionsUtil;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.PATH_TO_DATA_FILE;

public class SequenceRecoverTask implements Callable<Void> {
    private static final Logger LOGGER = Logger.getLogger(SequenceRecoverTask.class.getName());

    public SequenceRecoverTask() {
    }

    @Override
    public Void call() {
        try {
            // TODO: 8/17/21 if files dont exists, create it
            Map<Integer, Long> datas = FileHelper.readFile(PATH_TO_DATA_FILE, false);
            LOGGER.log(Level.INFO, String.format("SequenceRecoverTask(): data size %s, finding max number", datas.size()));
            int maxValue = CollectionsUtil.findMaxKeyInMap(datas);
            int nextValue = maxValue + 1;
            LOGGER.log(Level.INFO, String.format("SequenceRecoverTask(): next value is %s", nextValue));
            SequenceGenerator.INSTANCE.init(nextValue);
        } catch (IOException e) {
            //report exception somewhere.
            e.printStackTrace();
        }
        return null;
    }
}
