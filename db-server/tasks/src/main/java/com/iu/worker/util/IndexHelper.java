package com.iu.worker.util;

import com.files.FileHelper;
import com.iu.indexes.IndexKeeper;

import java.io.IOException;

//todo add unit tests
public class IndexHelper {
    public static void addIndexToRegistry(String file, String indexType) throws IOException {
        FileHelper.writeToFile(file, indexType, true);
    }

    public static void deleteIndexFromRegistry(String file, String lineToRemove) throws IOException {
        FileHelper.removeLineFromFile(file, lineToRemove);
    }

    public static boolean checkIndexExistence(String file, String indexType) throws IOException {
        return FileHelper.isFileExist(file) &&
                FileHelper.findLineInFile(file, indexType);
    }

    public static void writeHashIndexToDisc(String snapshotFile) throws IOException {
        FileHelper.writeHashIndexToDisc(IndexKeeper.INSTANCE.getHashIndex(), snapshotFile);
    }
}
