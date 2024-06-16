package com.iu.indexes;

import java.io.IOException;

public interface HashIndexService extends IndexService {
    void recoverIndex(String file) throws IOException;
    Long findAddrInIndex(Object id);

    void addValueToIndex(Object id, Long value);

    void deleteValueFromIndex(Object id);

    void deleteIndex() throws IOException;
}
