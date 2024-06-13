package com.iu.indexes.impl;


import com.files.FileHelper;
import com.iu.indexes.TreesIndexService;
import com.iu.indexes.IndexKeeper;
import com.iu.indexes.btreebased.btree.BTreeIndex;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.iu.worker.AbstractTask.PATH_TO_DATA_FILE;

public class BTreeServiceImpl implements TreesIndexService {
    private static final Logger LOGGER = Logger.getLogger(BTreeServiceImpl.class.getName());

    private static final int DEFAULT_MIN_DEGREE = 3;
    @Override
    public void createIndex(String file, String indexType) throws IOException {
        LOGGER.log(Level.FINE, String.format("createIndex: file %s, indexType %s", file, indexType));
       try {
           BTreeIndex index = new BTreeIndex(file, DEFAULT_MIN_DEGREE);
//        read all data from datafile
           Map<Integer, Long> indexCandidate = FileHelper.readFile(PATH_TO_DATA_FILE, false);
           for (Map.Entry<Integer, Long> entry : indexCandidate.entrySet()) {
//               try {
                   Integer K = entry.getKey();
                   Long V = entry.getValue();
                   if(K.equals(5)) {
                       System.out.println("fsdf");
                   }
                   index.insert(K, V);
//               } catch (Exception e) {
//                   LOGGER.log(Level.INFO, "exception has been thrown " + e.getMessage());
//                   e.printStackTrace();
//               }
           }
           index.traverse();
           IndexKeeper.INSTANCE.getBTreeIndexes().put(file, index);
       } catch (Exception e) {
           LOGGER.log(Level.INFO, "exception has been thrown " + e.getMessage());
           e.printStackTrace();
       }
    }

//    todo add logging
    @Override
    public Object findAddrInIndex(String file, Object id) throws IOException {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("Object id has to be an Integer type");

        BTreeIndex index = IndexKeeper.INSTANCE.getBTreeIndexes().get(file);

        Long result =  index.search((int)id);

        LOGGER.info("found offset in btree index " + result);

        return result;
    }

    @Override
    public void addValueToIndex(String file, Object id, Object value) throws IOException {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("Object id has to be an Integer type");
        BTreeIndex index = IndexKeeper.INSTANCE.getBTreeIndexes().get(file);
        index.insert((int)id, (Long) value);
    }

}
