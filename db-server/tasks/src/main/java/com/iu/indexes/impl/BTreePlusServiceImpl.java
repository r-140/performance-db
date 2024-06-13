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

public class BTreePlusServiceImpl implements TreesIndexService {
    private static final Logger LOGGER = Logger.getLogger(BTreePlusServiceImpl.class.getName());

    @Override
    public void createIndex(String file, String indexType) throws IOException {
        LOGGER.log(Level.INFO, String.format("createIndex: file %s, indexType %s", file, indexType));
        BPlusTreeIndex index = new BPlusTreeIndex(file);
//        read all data from datafile
        Map<Integer, Long> indexCandidate = FileHelper.readFile(PATH_TO_DATA_FILE, false);
        for (Map.Entry<Integer, Long> entry : indexCandidate.entrySet()) {
            Integer K = entry.getKey();
            Long V = entry.getValue();
            index.insert(K, V);
        }

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
    public void addValueToIndex(String file, Object id, Object value) {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("Object id has to be an Integer type");
        BPlusTreeIndex index = IndexKeeper.INSTANCE.getBPlusTreeIndexes().get(file);
        index.insert((int)id, value);
    }

}
