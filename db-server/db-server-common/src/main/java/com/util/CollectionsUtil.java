package com.util;

import java.util.Map;
import java.util.Set;

public class CollectionsUtil {

    public static int findMaxKeyInMap(Map<Integer, Long> map) {
        Set<Integer> keySet = map.keySet();

        return keySet.parallelStream().max(Integer::compareTo).orElse(-1);

    }
}
