package com.iu.indexes.lsmtree;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class LSMTreeIndex {
    private static final int MEMTABLE_LIMIT = 5; // Ограничение на размер MemTable
    private List<SSTable> sstables;
    private MemoryTable memTable;
    private int sstableCounter;
    private String fileName;

    public LSMTreeIndex(String fileName) {
        this.sstables = new ArrayList<>();
        this.memTable = new MemoryTable();
        this.sstableCounter = 0;
        this.fileName = fileName;
    }

    public void put(int key, Long value) throws IOException {
        memTable.put(key, value);
        if (memTable.getTable().size() >= MEMTABLE_LIMIT) {
            flush();
        }
    }

    public void remove(int key) throws IOException {
        memTable.remove(key);
        if (memTable.getTable().size() >= MEMTABLE_LIMIT) {
            flush();
        }
    }

    public Long get(int key) throws IOException {
        Long value = memTable.get(key);
        if (value != null) {
            return value;
        }
        for (SSTable sstable : sstables) {
            TreeMap<Integer, Long> table = sstable.read();
            if (table.containsKey(key)) {
                return table.get(key);
            }
        }
        return null;
    }

    private void flush() throws IOException {
        File file = new File(fileName + sstableCounter++ + ".dat");
        SSTable sstable = new SSTable(file);
        sstable.write(memTable.getTable());
        sstables.add(sstable);
        memTable.clear();
    }

    public void merge() throws IOException {
        if (sstables.size() < 2) {
            return;
        }

        SSTable sstable1 = sstables.remove(0);
        SSTable sstable2 = sstables.remove(0);

        TreeMap<Integer, Long> map1 = sstable1.read();
        TreeMap<Integer, Long> map2 = sstable2.read();
        TreeMap<Integer, Long> mergedMap = new TreeMap<>(map1);
        mergedMap.putAll(map2);

        File file = new File("sstable" + sstableCounter++ + ".dat");
        SSTable mergedSSTable = new SSTable(file);
        mergedSSTable.write(mergedMap);
        sstables.add(mergedSSTable);
    }

//    public static void main(String[] args) {
//        try {
//            LSMTree lsmTree = new LSMTree();
//
//            lsmTree.put(1, 1l);
//            lsmTree.put(2, 2l);
//            lsmTree.put(3, 3l);
//            lsmTree.put(4, 4l);
//            lsmTree.put(5, 5l);
//
//            System.out.println("MemTable flushed to SSTable");
//
//            System.out.println("Value for key 3: " + lsmTree.get(3));
//
//            lsmTree.put(6, 6l);
//            lsmTree.put(7, 7l);
//
//            System.out.println("MemTable flushed to SSTable");
//
//            System.out.println("Value for key 6: " + lsmTree.get(6));
//
//            lsmTree.remove(3);
//            lsmTree.put(8, 8l);
//
//            System.out.println("Value for key 3 after removal: " + lsmTree.get(3));
//
//            lsmTree.merge();
//            System.out.println("Merged SSTables");
//
//            System.out.println("Value for key 8: " + lsmTree.get(8));
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
}
