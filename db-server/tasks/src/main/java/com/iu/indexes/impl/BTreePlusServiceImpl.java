package com.iu.indexes.impl;


import com.files.FileHelper;
import com.iu.indexes.TreesIndexService;
import com.iu.indexes.IndexKeeper;
import com.iu.indexes.btreebased.bplustree.BPlusTreeIndex;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.PATH_TO_DATA_FILE;
import static com.iu.worker.AbstractTask.PATH_TO_INDEX_REGISTRY;

public class BTreePlusServiceImpl implements TreesIndexService {
    private static final int DEFAULT_MIN_DEGREE = 3;
    private static final Logger LOGGER = Logger.getLogger(BTreePlusServiceImpl.class.getName());

    @Override
    public void createIndex(String file) throws IOException {
        LOGGER.log(Level.INFO, String.format("createIndex: file %s, ", file));
        BPlusTreeIndex index = new BPlusTreeIndex(file, DEFAULT_MIN_DEGREE);
//        read all data from datafile
        Map<Integer, Long> indexCandidate = FileHelper.readFile(PATH_TO_DATA_FILE, false);
        for (Map.Entry<Integer, Long> entry : indexCandidate.entrySet()) {
            Integer K = entry.getKey();
            Long V = entry.getValue();
            index.insert(K, V);
        }
        index.traverse();
        IndexKeeper.INSTANCE.getBPlusTreeIndexes().put(file, index);
    }

//    todo add logging
    @Override
    public Object findAddrInIndex(String file, Object id) {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("Object id has to be an Integer type");

        BPlusTreeIndex index = IndexKeeper.INSTANCE.getBPlusTreeIndexes().get(file);

        return index.search((int)id);
    }

    @Override
    public void addValueToIndex(String file, Object id, Object value) throws IOException {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("Object id has to be an Integer type");
//        todo read index from file
        BPlusTreeIndex index = IndexKeeper.INSTANCE.getBPlusTreeIndexes().get(file);
        index.insert((int)id, (Long) value);
    }

    @Override
    public void deleteValueFromIndex(String file, Object id) throws IOException {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("Object id has to be an Integer type");
//        todo read index from file
        BPlusTreeIndex index = IndexKeeper.INSTANCE.getBPlusTreeIndexes().get(file);
        index.remove((Integer) id);
    }

    @Override
    public void deleteIndex(String file) throws IOException {
        FileHelper.removeFile(file);
        FileHelper.removeLineFromFile(PATH_TO_INDEX_REGISTRY, "bplustree");
    }

}
