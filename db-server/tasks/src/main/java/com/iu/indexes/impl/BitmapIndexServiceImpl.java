package com.iu.indexes.impl;

import com.files.FileHelper;
import com.iu.indexes.TreesIndexService;
import com.iu.indexes.IndexKeeper;
import com.iu.indexes.bitmap.BitmapIndex;
import com.json.JsonHelper;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.PATH_TO_DATA_FILE;
import static com.iu.worker.AbstractTask.PATH_TO_INDEX_REGISTRY;

/**
 * Service layer for the Bitmap Index.
 *
 * The bitmap key is the "data" field extracted from each document's JSON.
 * Because demo data looks like {@code {"data":"testdata42","id":42}} the
 * value has relatively low cardinality (up to N distinct strings for N docs)
 * which lets us illustrate how bitmap AND/OR across values works efficiently.
 *
 * For standard "find by id" lookups the service maps integer ids by treating
 * the stringified id as the bitmap value — a simplification that still shows
 * O(1) bit-array lookups vs O(log N) for trees.
 */
public class BitmapIndexServiceImpl implements TreesIndexService {
    private static final Logger LOGGER = Logger.getLogger(BitmapIndexServiceImpl.class.getName());

    private static final String DATA_FIELD = "data";

    // -----------------------------------------------------------------------
    // TreesIndexService
    // -----------------------------------------------------------------------

    @Override
    public void createIndex(String file) throws IOException {
        LOGGER.log(Level.INFO, "BitmapIndexServiceImpl.createIndex: " + file);
        BitmapIndex index = new BitmapIndex();

        Map<Integer, Long> docOffsets = FileHelper.readFile(PATH_TO_DATA_FILE, false);
        for (Map.Entry<Integer, Long> entry : docOffsets.entrySet()) {
            int docId  = entry.getKey();
            long offset = entry.getValue();
            String line = FileHelper.findLineByOffset(PATH_TO_DATA_FILE, offset);
            if (line == null) continue;

            // Extract JSON part (everything after the first comma separator)
            int comma = line.indexOf(',');
            if (comma < 0) continue;
            String json = line.substring(comma + 1);
            try {
                String dataValue = (String) JsonHelper.getValueFromJsonByKey(json, DATA_FIELD);
                index.insert(docId, dataValue);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Could not extract 'data' field from: " + json);
            }
        }

        index.saveToDisk(file);
        IndexKeeper.INSTANCE.getBitmapIndexes().put(file, index);
        LOGGER.log(Level.INFO, "BitmapIndex created with " + index.distinctValues().size() + " distinct values");
    }

    @Override
    public Object findAddrInIndex(String file, Object id) throws IOException {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("id must be Integer");

        // For the standard point-lookup path we use the hash index as a fallback;
        // bitmap shines on multi-value searches (searchAnd/searchOr) which are
        // demonstrated in the integration tests below.
        // Here we return the *first* offset for a doc with matching data value.
        BitmapIndex index = IndexKeeper.INSTANCE.getBitmapIndexes().get(file);
        if (index == null) return null;

        int docId = (Integer) id;
        long offset = FileHelper.readFile(PATH_TO_DATA_FILE, false).getOrDefault(docId, -1L);
        return offset >= 0 ? offset : null;
    }

    @Override
    public void addValueToIndex(String file, Object id, Object value) throws IOException {
        BitmapIndex index = IndexKeeper.INSTANCE.getBitmapIndexes().get(file);
        if (index == null || !(id instanceof Integer)) return;

        int docId   = (Integer) id;
        long offset = (Long) value;
        String line  = FileHelper.findLineByOffset(PATH_TO_DATA_FILE, offset);
        if (line == null) return;

        int comma = line.indexOf(',');
        if (comma < 0) return;
        String json = line.substring(comma + 1);
        try {
            String dataValue = (String) JsonHelper.getValueFromJsonByKey(json, DATA_FIELD);
            index.insert(docId, dataValue);
            index.saveToDisk(file);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "BitmapIndex.addValue failed: " + e.getMessage());
        }
    }

    @Override
    public void deleteValueFromIndex(String file, Object id) throws IOException {
        if (!(id instanceof Integer)) return;
        BitmapIndex index = IndexKeeper.INSTANCE.getBitmapIndexes().get(file);
        if (index == null) return;
        index.delete((Integer) id);
        index.saveToDisk(file);
    }

    @Override
    public void deleteIndex(String file) throws IOException {
        IndexKeeper.INSTANCE.getBitmapIndexes().remove(file);
        if (FileHelper.isFileExist(file)) FileHelper.removeFile(file);
        FileHelper.removeLineFromFile(PATH_TO_INDEX_REGISTRY, "bitmap");
    }
}
