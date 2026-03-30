package com.iu.indexes.impl;

import com.files.FileHelper;
import com.iu.indexes.TreesIndexService;
import com.iu.indexes.IndexKeeper;
import com.iu.indexes.gin.GINIndex;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.PATH_TO_DATA_FILE;
import static com.iu.worker.AbstractTask.PATH_TO_INDEX_REGISTRY;

/**
 * Service layer for the GIN (Generalized Inverted Index).
 *
 * At creation time every existing document in the data file is read;
 * its full JSON string is tokenised and inserted into the GIN so that
 * any token can be searched in O(log T + k) time.
 *
 * For the standard "find by id" path (used by existing tasks) the GIN
 * stores the file offset for the first token found.  Full-text search
 * across all tokens is available via {@link GINIndex#search(String)} or
 * {@link GINIndex#searchAll(List)}.
 */
public class GINIndexServiceImpl implements TreesIndexService {
    private static final Logger LOGGER = Logger.getLogger(GINIndexServiceImpl.class.getName());

    // -----------------------------------------------------------------------
    // TreesIndexService
    // -----------------------------------------------------------------------

    @Override
    public void createIndex(String file) throws IOException {
        LOGGER.log(Level.INFO, "GINIndexServiceImpl.createIndex: " + file);
        GINIndex index = new GINIndex();

        // Read every doc from the data file and index its full JSON value
        Map<Integer, Long> docOffsets = FileHelper.readFile(PATH_TO_DATA_FILE, false);
        for (Map.Entry<Integer, Long> entry : docOffsets.entrySet()) {
            long offset = entry.getValue();
            String line = FileHelper.findLineByOffset(PATH_TO_DATA_FILE, offset);
            if (line != null) {
                index.insert(line, offset);
            }
        }

        index.saveToDisk(file);
        IndexKeeper.INSTANCE.getGINIndexes().put(file, index);
        LOGGER.log(Level.INFO, "GINIndex created with " + index.tokenCount() + " tokens");
    }

    @Override
    public Object findAddrInIndex(String file, Object id) throws IOException {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("id must be Integer");

        GINIndex index = IndexKeeper.INSTANCE.getGINIndexes().get(file);
        if (index == null) return null;

        // Convert int id to its string token form to find matching offsets
        String token = String.valueOf(id);
        List<Long> offsets = index.search(token);
        return offsets.isEmpty() ? null : offsets.get(0);
    }

    @Override
    public void addValueToIndex(String file, Object id, Object value) throws IOException {
        GINIndex index = IndexKeeper.INSTANCE.getGINIndexes().get(file);
        if (index == null) return;
        // value is the file offset from the caller; we need to look up the doc text
        long offset = (Long) value;
        String line = FileHelper.findLineByOffset(PATH_TO_DATA_FILE, offset);
        if (line != null) {
            index.insert(line, offset);
        }
        index.saveToDisk(file);
    }

    @Override
    public void deleteValueFromIndex(String file, Object id) throws IOException {
        GINIndex index = IndexKeeper.INSTANCE.getGINIndexes().get(file);
        if (index == null) return;
        // We don't know the exact offset here, so re-build is the safe approach.
        // In a production GIN you'd maintain a reverse map id→offsets.
        // For this educational project we rebuild, which is acceptable.
        createIndex(file);
    }

    @Override
    public void deleteIndex(String file) throws IOException {
        IndexKeeper.INSTANCE.getGINIndexes().remove(file);
        if (FileHelper.isFileExist(file)) FileHelper.removeFile(file);
        FileHelper.removeLineFromFile(PATH_TO_INDEX_REGISTRY, "gin");
    }
}
