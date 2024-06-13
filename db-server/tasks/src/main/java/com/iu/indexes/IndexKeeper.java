package com.iu.indexes;

import com.iu.indexes.btreebased.bplustree.BPlusTreeIndex;
import com.iu.indexes.btreebased.btree.BTreeIndex;
import com.iu.indexes.lsmtree.LSMTreeIndex;

import java.util.concurrent.ConcurrentHashMap;

public enum IndexKeeper {
    INSTANCE;
    private ConcurrentHashMap<Integer, Long> hashIndex;

//    todo add recovering indexes when db is restarted
    private ConcurrentHashMap<String, BTreeIndex> bTreeIndexStorage;

    private ConcurrentHashMap<String, BPlusTreeIndex> bPlusTreeIndexStorage;

    private ConcurrentHashMap<String, LSMTreeIndex> lsmTreeIndexStorage;

    public synchronized ConcurrentHashMap<Integer, Long> getHashIndex() {
        if (hashIndex == null)
            hashIndex = new ConcurrentHashMap<>();
        return hashIndex;
    }

    public synchronized ConcurrentHashMap<String, BTreeIndex> getBTreeIndexes(){
        if (bTreeIndexStorage == null){
            bTreeIndexStorage = new ConcurrentHashMap<>();
        }

        return bTreeIndexStorage;
    }

    public synchronized ConcurrentHashMap<String, BPlusTreeIndex> getBPlusTreeIndexes(){
        if (bPlusTreeIndexStorage == null){
            bPlusTreeIndexStorage = new ConcurrentHashMap<>();
        }

        return bPlusTreeIndexStorage;
    }

    public synchronized ConcurrentHashMap<String, LSMTreeIndex> getLsmTreeIndexes(){
        if (lsmTreeIndexStorage == null){
            lsmTreeIndexStorage = new ConcurrentHashMap<>();
        }

        return lsmTreeIndexStorage;
    }
}
