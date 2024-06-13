package com.iu.indexes.btreebased.bplustree;

import java.io.RandomAccessFile;
import java.io.IOException;

class BPlusTreeNode {
    int[] keys;
    Long[] values;
    int t;
    BPlusTreeNode[] children;
    int n;
    boolean leaf;
    long position;
    long next;

    public BPlusTreeNode(int t, boolean leaf, long position) {
        this.t = t;
        this.leaf = leaf;
        this.position = position;
        this.keys = new int[2 * t - 1];
        this.values = new Long[2 * t - 1];
        this.children = new BPlusTreeNode[2 * t];
        this.n = 0;
        this.next = -1;
    }

    public void traverse() {
        int i;
        for (i = 0; i < n; i++) {
            if (!leaf) {
                children[i].traverse();
            }
            System.out.print(" (" + keys[i] + ", " + values[i] + ")");
        }
        if (!leaf) {
            children[i].traverse();
        }
    }

    public Long search(int key) {
        int i = 0;
        while (i < n && key > keys[i]) {
            i++;
        }
        if (i < n && keys[i] == key) {
            return values[i];
        }
        if (leaf) {
            return null;
        }
        return children[i].search(key);
    }
}

