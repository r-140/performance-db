package com.iu.indexes.impl;

import com.files.FileHelper;
import com.iu.indexes.HashIndexService;
import com.iu.indexes.IndexKeeper;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HashIndexServiceImpl implements HashIndexService {
    private static final Logger LOGGER = Logger.getLogger(HashIndexServiceImpl.class.getName());

    @Override
    public void createIndex(String file, String indexType) throws IOException {
        LOGGER.log(Level.INFO, String.format("createIndex: file %s, indexType %s", file, indexType));
        Map<Integer, Long> hashIndexCandidate = FileHelper.readFile(file, false);

        LOGGER.log(Level.FINE, String.format("createIndex(): data for hash index %s", hashIndexCandidate.size()));
        IndexKeeper.INSTANCE.getHashIndex().putAll(hashIndexCandidate);

        LOGGER.log(Level.INFO, String.format("createIndex(): hash index created %s", IndexKeeper.INSTANCE.getHashIndex().size()));
    }

    @Override
    public void recoverIndex(String file) throws IOException {
        LOGGER.log(Level.INFO, String.format("recoverIndex: file %s", file));
        Map<Integer, Long> hashIndexCandidate = FileHelper.readFile(file, true);

        LOGGER.log(Level.FINE, String.format("recoverIndex(): data for hash index %s", hashIndexCandidate.size()));
        IndexKeeper.INSTANCE.getHashIndex().putAll(hashIndexCandidate);

        LOGGER.log(Level.INFO, String.format("recoverIndex(): hash index created %s", IndexKeeper.INSTANCE.getHashIndex().size()));
    }

    @Override
    public Long findAddrInIndex(Object id) {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("Object id has to be an Integer type");

        final Long found = IndexKeeper.INSTANCE.getHashIndex().get(id);

        LOGGER.log(Level.FINE, String.format("findAddrInIndex(): found offset %s", found));

        return found;
    }

    @Override
    public void addValueToIndex(Object id, Long value) {
        if (!(id instanceof Integer))
            throw new IllegalArgumentException("Object id has to be an Integer type");
        IndexKeeper.INSTANCE.getHashIndex().put((Integer) id, value);
    }
}
