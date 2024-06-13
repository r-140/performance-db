package com.files;

import java.util.HashMap;
import java.util.Map;


class AbstractTest {
    // TODO: 5/2/2019 clarify how to put relative path via classloader getResource
    static final String PATH_TO_FILE = "C:\\workspace\\nosql_concept\\nosql_concept\\common\\src\\test\\datafiles\\test_data.dat";

    static final String SNAPSHOT_FILE = "C:\\workspace\\nosql_concept\\nosql_concept\\common\\src\\test\\datafiles\\hashIndex_Snapshot.dat";

     static Map<Integer, Long> hashIndex;

    static {
        hashIndex = new HashMap<>();
        hashIndex.put(0, 0L);
        hashIndex.put(1, 31L);
        hashIndex.put(2, 62L);
        hashIndex.put(3, 93L);
        hashIndex.put(4, 124L);
    }
}
