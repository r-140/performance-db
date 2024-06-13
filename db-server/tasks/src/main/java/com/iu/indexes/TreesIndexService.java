package com.iu.indexes;

import java.io.IOException;

public interface TreesIndexService {

    void createIndex(String file, String indexType) throws IOException;

//    void addIndex2Registry(String file, String indexType) throws IOException;

//    boolean checkIndexExists(String file, String indexType) throws IOException;

    Object findAddrInIndex(String file, Object id);

    void addValueToIndex(String file, Object id, Object value);

}
