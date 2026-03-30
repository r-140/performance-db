package com.collection;

import com.util.CollectionsUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class CollectionUtilTest {

    private Map<Integer, Long> hashIndex;

    @BeforeEach
    public void init(){
        hashIndex = new HashMap<>();
        hashIndex.put(0, 0L);
        hashIndex.put(1, 31L);
        hashIndex.put(2, 62L);
        hashIndex.put(3, 93L);
        hashIndex.put(4, 124L);
    }

    @Test
    public void findMaxInMapTest(){
        int max = CollectionsUtil.findMaxKeyInMap(hashIndex);

        assertEquals(max, 4);
    }
}
