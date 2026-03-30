package com.iu.indexes;

import com.iu.indexes.impl.BTreePlusServiceImpl;
import com.iu.indexes.impl.BTreeServiceImpl;
import com.iu.indexes.impl.BitmapIndexServiceImpl;
import com.iu.indexes.impl.GINIndexServiceImpl;
import com.iu.indexes.impl.HashIndexServiceImpl;
import com.iu.indexes.impl.LSMTreeServiceImpl;

import java.io.IOException;

import static com.iu.worker.AbstractTask.DISC_PATH;

public enum IndexTypes {

    HASH_INDEX("hashIndex", null) {
        private final HashIndexService svc = new HashIndexServiceImpl();
        @Override public Long findAddrInIndex(Object id)            { return svc.findAddrInIndex(id); }
        @Override public void addValueToIndex(Object id, Long v)    { svc.addValueToIndex(id, v); }
        @Override public void createIndex(String file) throws IOException { svc.createIndex(file); }
        @Override public void deleteIndex()            throws IOException { svc.deleteIndex(); }
        @Override public void deleteAddrFromIndex(Object id)        { svc.deleteValueFromIndex(id); }
        @Override public void recoverIndex(String file) throws IOException { svc.recoverIndex(file); }
    },

    LSMTREE("lsmtree", "lsmtree.dat") {
        private final String FILE_PATH = DISC_PATH + "/" + getIndexFileName();
        private final TreesIndexService svc = new LSMTreeServiceImpl();
        @Override public Object findAddrInIndex(Object id) throws IOException { return svc.findAddrInIndex(FILE_PATH, id); }
        @Override public void addValueToIndex(Object id, Long v) throws IOException { svc.addValueToIndex(FILE_PATH, id, v); }
        @Override public void createIndex(String file) throws IOException { svc.createIndex(file); }
        @Override public void deleteIndex() throws IOException { svc.deleteIndex(DISC_PATH); }
        @Override public void deleteAddrFromIndex(Object id) throws IOException { svc.deleteValueFromIndex(FILE_PATH, id); }
    },

    BTREE("btree", "btree.dat") {
        private final String FILE_PATH = DISC_PATH + "/" + getIndexFileName();
        private final TreesIndexService svc = new BTreeServiceImpl();
        @Override public Object findAddrInIndex(Object id) throws IOException { return svc.findAddrInIndex(FILE_PATH, id); }
        @Override public void addValueToIndex(Object id, Long v) throws IOException { svc.addValueToIndex(FILE_PATH, id, v); }
        @Override public void createIndex(String file) throws IOException { svc.createIndex(file); }
        @Override public void deleteIndex() throws IOException { svc.deleteIndex(FILE_PATH); }
        @Override public void deleteAddrFromIndex(Object id) throws IOException { svc.deleteValueFromIndex(FILE_PATH, id); }
    },

    BPLUSTREE("bplustree", "bplustree.dat") {
        private final String FILE_PATH = DISC_PATH + "/" + getIndexFileName();
        private final TreesIndexService svc = new BTreePlusServiceImpl();
        @Override public Object findAddrInIndex(Object id) throws IOException { return svc.findAddrInIndex(FILE_PATH, id); }
        @Override public void addValueToIndex(Object id, Long v) throws IOException { svc.addValueToIndex(FILE_PATH, id, v); }
        @Override public void createIndex(String file) throws IOException { svc.createIndex(file); }
        @Override public void deleteIndex() throws IOException { svc.deleteIndex(FILE_PATH); }
        @Override public void deleteAddrFromIndex(Object id) throws IOException { svc.deleteValueFromIndex(FILE_PATH, id); }
    },

    GIN("gin", "gin.dat") {
        private final String FILE_PATH = DISC_PATH + "/" + getIndexFileName();
        private final TreesIndexService svc = new GINIndexServiceImpl();
        @Override public Object findAddrInIndex(Object id) throws IOException { return svc.findAddrInIndex(FILE_PATH, id); }
        @Override public void addValueToIndex(Object id, Long v) throws IOException { svc.addValueToIndex(FILE_PATH, id, v); }
        @Override public void createIndex(String file) throws IOException { svc.createIndex(file); }
        @Override public void deleteIndex() throws IOException { svc.deleteIndex(FILE_PATH); }
        @Override public void deleteAddrFromIndex(Object id) throws IOException { svc.deleteValueFromIndex(FILE_PATH, id); }
    },

    BITMAP("bitmap", "bitmap.dat") {
        private final String FILE_PATH = DISC_PATH + "/" + getIndexFileName();
        private final TreesIndexService svc = new BitmapIndexServiceImpl();
        @Override public Object findAddrInIndex(Object id) throws IOException { return svc.findAddrInIndex(FILE_PATH, id); }
        @Override public void addValueToIndex(Object id, Long v) throws IOException { svc.addValueToIndex(FILE_PATH, id, v); }
        @Override public void createIndex(String file) throws IOException { svc.createIndex(file); }
        @Override public void deleteIndex() throws IOException { svc.deleteIndex(FILE_PATH); }
        @Override public void deleteAddrFromIndex(Object id) throws IOException { svc.deleteValueFromIndex(FILE_PATH, id); }
    },

    NONE("none", "none") {
        @Override public Object findAddrInIndex(Object id)         { throw new IllegalStateException("NONE index"); }
        @Override public void addValueToIndex(Object id, Long v)   { throw new IllegalStateException("NONE index"); }
        @Override public void createIndex(String file)             { throw new IllegalStateException("NONE index"); }
        @Override public void deleteIndex()                        { throw new IllegalStateException("NONE index"); }
        @Override public void deleteAddrFromIndex(Object id)       { throw new IllegalStateException("NONE index"); }
    };

    private final String indexType;
    private final String indexFileName;

    IndexTypes(String indexType, String indexFileName) {
        this.indexType = indexType;
        this.indexFileName = indexFileName;
    }

    public static IndexTypes getIndexByType(String type) {
        for (IndexTypes it : values()) {
            if (it.indexType.equals(type)) return it;
        }
        return null;
    }

    public String getIndexType()     { return indexType; }
    public String getIndexFileName() { return indexFileName; }

    public abstract Object findAddrInIndex(Object id)              throws IOException;
    public abstract void   addValueToIndex(Object id, Long value)  throws IOException;
    public abstract void   createIndex(String file)                throws IOException;
    public abstract void   deleteIndex()                           throws IOException;
    public abstract void   deleteAddrFromIndex(Object id)          throws IOException;

    public void recoverIndex(String snapshotFile) throws IOException {
        throw new IllegalStateException("recoverIndex not supported for " + indexType);
    }
}
