package com.iu.indexes.btreebased.btree;

/**
 * B-Tree node.
 *
 * In a B-Tree EVERY node (leaf and internal) stores both keys and values.
 * This differs from B+ Tree where only leaf nodes carry values.
 * The consequence: internal nodes have less room for keys (lower fanout),
 * so the tree is taller and range scans require repeated tree traversal.
 */
class BTreeNode {
    int[]      keys;
    Long[]     values;
    BTreeNode[] children;
    int        n;       // active key count
    int        t;
    boolean    leaf;
    long       position;

    BTreeNode(int t, boolean leaf, long position) {
        this.t        = t;
        this.leaf     = leaf;
        this.position = position;
        this.n        = 0;
        this.keys     = new int[2 * t - 1];
        this.values   = new Long[2 * t - 1];
        this.children = new BTreeNode[2 * t];
    }

    Long search(int key) {
        int i = 0;
        while (i < n && key > keys[i]) i++;
        if (i < n && keys[i] == key) return values[i];
        if (leaf) return null;
        return children[i].search(key);
    }

    void traverse() {
        int i;
        for (i = 0; i < n; i++) {
            if (!leaf) children[i].traverse();
            System.out.print(" (" + keys[i] + "," + values[i] + ")");
        }
        if (!leaf) children[i].traverse();
    }
}
