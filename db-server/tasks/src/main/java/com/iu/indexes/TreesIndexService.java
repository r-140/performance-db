package com.iu.indexes;

import java.io.IOException;

public interface TreesIndexService extends IndexService {

    Object findAddrInIndex(String file, Object id) throws IOException;

    void addValueToIndex(String file, Object id, Object value) throws IOException;

    void deleteValueFromIndex(String file, Object id) throws IOException;

    void deleteIndex(String file) throws IOException;
}
