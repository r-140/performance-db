package com.iu.indexes.impl;

import com.files.FileHelper;
import com.iu.indexes.IndexKeeper;
import com.iu.indexes.TreesIndexService;
import com.iu.indexes.lsmtree.LSMTreeIndex;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.PATH_TO_DATA_FILE;
import static com.iu.worker.AbstractTask.PATH_TO_INDEX_REGISTRY;

public class LSMTreeServiceImpl  implements TreesIndexService {
    private static final Logger LOGGER = Logger.getLogger(LSMTreeServiceImpl.class.getName());
    @Override
    public void createIndex(String file) throws IOException {
        LOGGER.log(Level.INFO, String.format("createIndex: file %s", file));
//        todo add MEMTABLE_LIMIT as a parameter
        LSMTreeIndex index = new LSMTreeIndex(file);
//        read all data from datafile
        Map<Integer, Long> indexCandidate = FileHelper.readFile(PATH_TO_DATA_FILE, false);
        for (Map.Entry<Integer, Long> entry : indexCandidate.entrySet()) {
            Integer K = entry.getKey();
            Long V = entry.getValue();
            index.put(K, V);
        }

        IndexKeeper.INSTANCE.getLsmTreeIndexes().put(file, index);
    }

    @Override
    public Object findAddrInIndex(String file, Object id) throws IOException {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("Object id has to be an Integer type");

        LSMTreeIndex index = IndexKeeper.INSTANCE.getLsmTreeIndexes().get(file);

        return index.get((int)id);
    }

    @Override
    public void addValueToIndex(String file, Object id, Object value) throws IOException {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("Object id has to be an Integer type");
//        todo read index from file
        LSMTreeIndex index = IndexKeeper.INSTANCE.getLsmTreeIndexes().get(file);
        index.put((int)id, (Long) value);
    }

    @Override
    public void deleteValueFromIndex(String file, Object id) throws IOException {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("Object id has to be an Integer type");
        LSMTreeIndex index = IndexKeeper.INSTANCE.getLsmTreeIndexes().get(file);
        index.remove((Integer) id);
    }

    @Override
    public void deleteIndex(String fileDir) throws IOException {
        FileHelper.deleteFilesWithPattern(fileDir, "lsmtree");
        FileHelper.removeLineFromFile(PATH_TO_INDEX_REGISTRY, "lsmtree");
    }

}
