package com.iu.indexes.lsmtree;

import java.util.TreeMap;

class MemoryTable {
    private TreeMap<Integer, Long> table;

    public MemoryTable() {
        table = new TreeMap<>();
    }

    public void put(int key, Long value) {
        table.put(key, value);
    }

    public void remove(int key) {
        table.put(key, null); // Используем null для логического удаления
    }

    public Long get(int key) {
        return table.get(key);
    }

    public TreeMap<Integer, Long> getTable() {
        return table;
    }

    public void clear() {
        table.clear();
    }
}

