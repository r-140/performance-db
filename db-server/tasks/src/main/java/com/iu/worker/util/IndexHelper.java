package com.iu.worker.util;

import com.files.FileHelper;
import com.iu.indexes.IndexKeeper;

import java.io.IOException;


public class IndexHelper {
    public static void addIndexToRegistry(String file, String indexType) throws IOException {
        FileHelper.writeToFile(file, indexType, true);
    }

    public static boolean checkIndexExistence(String file, String indexType) throws IOException {
        return FileHelper.isFileExist(file) &&
                FileHelper.isLineInFileExist(file, indexType);
    }

    public static void writeHashIndexToDisc(String snapshotFile) throws IOException {
        FileHelper.writeHashIndexToDisc(IndexKeeper.INSTANCE.getHashIndex(), snapshotFile);
    }
}
