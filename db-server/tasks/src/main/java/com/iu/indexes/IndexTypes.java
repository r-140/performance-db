package com.iu.indexes;


import com.iu.indexes.impl.BTreePlusServiceImpl;
import com.iu.indexes.impl.BTreeServiceImpl;
import com.iu.indexes.impl.HashIndexServiceImpl;
import com.iu.indexes.impl.LSMTreeServiceImpl;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static com.iu.worker.AbstractTask.DISC_PATH;

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

        @Override
        public void deleteIndex(String indexType) throws IOException {
            hashIndexService.deleteIndex();
        }

        @Override
        public void deleteAddrFromIndex(Object id) {
            hashIndexService.deleteValueFromIndex(id);
        }

        public void recoverIndex(String file) throws IOException {
            hashIndexService.recoverIndex(file);
        }
    },
    LSMTREE("lsmtree", "lsmtree.dat") {
        private final String FILE_PATH = DISC_PATH + "/" + getIndexFileName();
        private final TreesIndexService treesIndexService = new LSMTreeServiceImpl();
        @Override
        public Object findAddrInIndex(Object id) throws IOException {
            return treesIndexService.findAddrInIndex(FILE_PATH, id);
        }

        @Override
        public void addValueToIndex(Object id, Long value) throws IOException {
            treesIndexService.addValueToIndex(FILE_PATH, id, value);
        }

        @Override
        public void createIndex(String file, String indexType) throws IOException {
            treesIndexService.createIndex(file, indexType);
        }

        @Override
        public void deleteIndex(String indexType) throws IOException {
            treesIndexService.deleteIndex(FILE_PATH);
        }

        @Override
        public void deleteAddrFromIndex(Object id) throws IOException {
            treesIndexService.deleteValueFromIndex(FILE_PATH, id);
        }
    },
    BTREE("btree", "btree.dat") {
        private final String FILE_PATH = DISC_PATH + "/" + getIndexFileName();
        private final TreesIndexService treesIndexService = new BTreeServiceImpl();
        @Override
        public Object findAddrInIndex(Object id) throws IOException {
            return treesIndexService.findAddrInIndex(FILE_PATH, id);
        }

        @Override
        public void addValueToIndex(Object id, Long value) throws IOException {
            treesIndexService.addValueToIndex(FILE_PATH, id, value);
        }

        @Override
        public void createIndex(String file, String indexType) throws IOException {
            treesIndexService.createIndex(file, indexType);
        }

        @Override
        public void deleteIndex(String indexType) throws IOException {
            treesIndexService.deleteIndex(FILE_PATH);
        }

        @Override
        public void deleteAddrFromIndex(Object id) throws IOException {
            treesIndexService.deleteValueFromIndex(FILE_PATH, id);
        }
    },
    BPLUSTREE("bplustree", "bplustree.dat") {
        private final String FILE_PATH = DISC_PATH + "/" + getIndexFileName();
        private final TreesIndexService treesIndexService = new BTreePlusServiceImpl();
        @Override
        public Object findAddrInIndex(Object id) throws IOException {
            return treesIndexService.findAddrInIndex(FILE_PATH, id);
        }

        @Override
        public void addValueToIndex(Object id, Long value) throws IOException {
            treesIndexService.addValueToIndex(FILE_PATH, id, value);
        }

        @Override
        public void createIndex(String file, String indexType) throws IOException {
            treesIndexService.createIndex(file, indexType);
        }

        @Override
        public void deleteIndex(String indexType) throws IOException {
            treesIndexService.deleteIndex(FILE_PATH);
        }

        @Override
        public void deleteAddrFromIndex(Object id) throws IOException {
            treesIndexService.deleteValueFromIndex(FILE_PATH, id);
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

        @Override
        public void deleteIndex(String indexType) throws IOException {
            throw new IllegalStateException("Invalid method call");
        }

        @Override
        public void deleteAddrFromIndex(Object id) {
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

    public abstract Object findAddrInIndex(Object id) throws IOException;

    public abstract void addValueToIndex(Object id, Long value) throws IOException;

    public abstract void createIndex(String file, String indexType) throws IOException;

    public abstract void deleteIndex(String indexType) throws IOException;

    public abstract void deleteAddrFromIndex(Object id) throws IOException;

    public void recoverIndex(String snapshotFile) throws IOException {
        throw new IllegalStateException("Imvalid method call");
    }
}
