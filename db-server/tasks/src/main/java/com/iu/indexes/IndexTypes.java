package com.iu.indexes;


import com.iu.indexes.impl.BTreePlusServiceImpl;
import com.iu.indexes.impl.BTreeServiceImpl;
import com.iu.indexes.impl.HashIndexServiceImpl;
import com.iu.indexes.impl.LSMTreeServiceImpl;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public enum IndexTypes {
    HASH_INDEX("hashIndex", null) {
        private HashIndexService hashIndexService = new HashIndexServiceImpl();
        @Override
        public Long findAddrInIndex(Object id) {
            return hashIndexService.findAddrInIndex(id);
        }

        @Override
        public void addValueToIndex(Object id, Long value) {
            hashIndexService.addValueToIndex(id, value);
        }

        @Override
        public void createIndex(String file, String indexType) throws IOException {
            hashIndexService.createIndex(file, indexType);
        }

        public void recoverIndex(String file) throws IOException {
            hashIndexService.recoverIndex(file);
        }
    },
    LSMTREE("lsmtree", "lsmtree.dat") {
        private final TreesIndexService treesIndexService = new LSMTreeServiceImpl();
        @Override
        public Object findAddrInIndex(Object id) {
            return treesIndexService.findAddrInIndex(getIndexFileName(), id);
        }

        @Override
        public void addValueToIndex(Object id, Long value) {
            treesIndexService.addValueToIndex(getIndexFileName(), id, value);
        }

        @Override
        public void createIndex(String file, String indexType) throws IOException {
            treesIndexService.createIndex(file, indexType);
        }
    },
    BTREE("btree", "btree.dat") {
        private final TreesIndexService treesIndexService = new BTreeServiceImpl();
        @Override
        public Object findAddrInIndex(Object id) {
            return treesIndexService.findAddrInIndex(getIndexFileName(), id);
        }

        @Override
        public void addValueToIndex(Object id, Long value) {
            treesIndexService.addValueToIndex(getIndexFileName(), id, value);
        }

        @Override
        public void createIndex(String file, String indexType) throws IOException {
            treesIndexService.createIndex(file, indexType);
        }
    },
    BPLUSTREE("bplustree", "bplustree.dat") {
        private final TreesIndexService treesIndexService = new BTreePlusServiceImpl();
        @Override
        public Object findAddrInIndex(Object id) {
            return treesIndexService.findAddrInIndex(getIndexFileName(), id);
        }

        @Override
        public void addValueToIndex(Object id, Long value) {
            treesIndexService.addValueToIndex(getIndexFileName(), id, value);
        }

        @Override
        public void createIndex(String file, String indexType) throws IOException {
            treesIndexService.createIndex(file, indexType);
        }
    },
    NONE("none", "none") {
        @Override
        public Object findAddrInIndex(Object id) {
            throw new IllegalStateException("Invalid method call");
        }

        @Override
        public void addValueToIndex(Object id, Long value) {
            throw new IllegalStateException("Invalid method call");
        }

        @Override
        public void createIndex(String file, String indexType) throws IOException {
            throw new IllegalStateException("Invalid method call");
        }
    };

    private final String indexType;

    private final String indexFileName;

    IndexTypes(String indexType, String indexFileName) {
        this.indexType = indexType;
        this.indexFileName = indexFileName;
    }

    public static IndexTypes getIndexByType(String indexTypeStr) {
        for (IndexTypes indexType : IndexTypes.values()) {
            if (indexType.getIndexType().equals(indexTypeStr))
                return indexType;
        }

        return null;
    }

    public String getIndexType() {
        return indexType;
    }

    public String getIndexFileName() {
        return indexFileName;
    }

    public abstract Object findAddrInIndex(Object id);

    public abstract void addValueToIndex(Object id, Long value);

    public abstract void createIndex(String file, String indexType) throws IOException;

    public void recoverIndex(String snapshotFile) throws IOException {
        throw new IllegalStateException("Imvalid method call");
    }
}
