package com.iu.indexes.btreebased.bplustree;

/**
 * B+ Tree node.
 *
 * KEY INVARIANT — what makes this a B+ Tree, not a B-Tree:
 *   Internal nodes hold ONLY keys (routing separators); values[] is null.
 *   Leaf nodes hold BOTH keys and values (file offsets).
 *   All leaf nodes are linked via `next` forming a sorted linked list
 *   for O(k) range scans.
 */
class BPlusTreeNode {
    int[]           keys;
    Long[]          values;    // non-null only in leaf nodes
    BPlusTreeNode[] children;  // non-null only in internal nodes
    int             n;         // active key count
    int             t;
    boolean         leaf;
    long            position;  // byte offset in the backing file
    long            next;      // leaf-chain pointer; -1 = end of list

    BPlusTreeNode(int t, boolean leaf, long position) {
        this.t        = t;
        this.leaf     = leaf;
        this.position = position;
        this.n        = 0;
        this.next     = -1;
        this.keys     = new int[2 * t - 1];
        this.values   = leaf ? new Long[2 * t - 1] : null;
        this.children = leaf ? null : new BPlusTreeNode[2 * t];
    }

    /**
     * Search for a key.
     * Internal nodes: route to the correct child using keys only.
     * Leaf nodes: linear scan for the exact key.
     */
    Long search(int key) {
        if (leaf) {
            for (int i = 0; i < n; i++) {
                if (keys[i] == key) return values[i];
            }
            return null;
        }
        int i = 0;
        while (i < n && key >= keys[i]) i++;
        return children[i].search(key);
    }

    /** Debug traversal printing leaf entries only. */
    void traverse() {
        if (leaf) {
            for (int i = 0; i < n; i++)
                System.out.print(" (" + keys[i] + "->" + values[i] + ")");
        } else {
            for (int i = 0; i < n; i++) {
                children[i].traverse();
                System.out.print(" [" + keys[i] + "]");
            }
            children[n].traverse();
        }
    }
}
