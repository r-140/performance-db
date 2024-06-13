package com.iu.indexes.btreebased.btree;
import java.io.RandomAccessFile;
import java.io.IOException;

import java.io.RandomAccessFile;
import java.io.IOException;

class BTreeNode {
    int[] keys;
    Long[] values;
    int t;
    BTreeNode[] children;
    int n;
    boolean leaf;
    long position;

    public BTreeNode(int t, boolean leaf, long position) {
        this.t = t;
        this.leaf = leaf;
        this.position = position;
        this.keys = new int[2 * t - 1];
        this.values = new Long[2 * t - 1];
        this.children = new BTreeNode[2 * t];
        this.n = 0;
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
