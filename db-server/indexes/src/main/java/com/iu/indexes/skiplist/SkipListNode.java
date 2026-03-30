package com.iu.indexes.skiplist;

/**
 * Skip list node — holds a key, value, and array of forward pointers,
 * one per level this node participates in.
 */
class SkipListNode {
    final int            key;
    volatile Long        value;
    final SkipListNode[] forward;

    SkipListNode(int key, Long value, int level) {
        this.key     = key;
        this.value   = value;
        this.forward = new SkipListNode[level + 1];
    }
}
