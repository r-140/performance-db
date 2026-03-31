package com.iu.indexes.btreebased.btree;

import java.io.*;
import java.util.logging.Logger;

/**
 * Disk-based B-Tree index.
 *
 * Fixes vs. original:
 *  - Values written as raw longs (8 bytes), not 256-byte ASCII strings.
 *  - Null/empty slots stored as sentinel -1L, never parsed as Long.valueOf("null").
 *  - nodeCounter recovered from file size on reload — no position-0 overwrite.
 *  - insert() propagates IOException instead of silently swallowing it.
 *
 * This remains a true B-Tree: every node (leaf and internal) stores values.
 * Contrast with BPlusTreeIndex where only leaf nodes store values.
 */
public class BTreeIndex {
    private static final Logger LOGGER = Logger.getLogger(BTreeIndex.class.getName());

    private BTreeNode root;
    private final int t;
    private final RandomAccessFile file;
    private long nodeCounter;

    // Slot: n(4) + leaf(1) + keys*(2t-1)*4 + values*(2t-1)*8 + children*(2t)*8
    private final long SLOT_SIZE;

    public BTreeIndex(String fileName, int t) throws IOException {
        this.t         = t;
        this.SLOT_SIZE = 4L + 1 + (long)(2*t-1)*4 + (long)(2*t-1)*8 + (long)(2*t)*8;
        this.file      = new RandomAccessFile(fileName, "rw");

        if (file.length() == 0) {
            nodeCounter = 0;
            root = new BTreeNode(t, true, allocateSlot());
            writeNode(root);
        } else {
            nodeCounter = (file.length() + SLOT_SIZE - 1) / SLOT_SIZE;
            root = readNode(0);
        }
    }

    public Long search(int key) {
        return root == null ? null : root.search(key);
    }

    public void insert(int key, Long value) throws IOException {
        if (root.n == 2 * t - 1) {
            BTreeNode s = new BTreeNode(t, false, allocateSlot());
            s.children[0] = root;
            splitChild(s, 0, root);
            root = s;
        }
        insertNonFull(root, key, value);
    }

    private void insertNonFull(BTreeNode node, int key, Long value) throws IOException {
        int i = node.n - 1;
        if (node.leaf) {
            while (i >= 0 && node.keys[i] > key) {
                node.keys[i+1]   = node.keys[i];
                node.values[i+1] = node.values[i];
                i--;
            }
            node.keys[i+1]   = key;
            node.values[i+1] = value;
            node.n++;
            writeNode(node);
        } else {
            while (i >= 0 && node.keys[i] > key) i--;
            i++;
            if (node.children[i].n == 2*t-1) {
                splitChild(node, i, node.children[i]);
                if (node.keys[i] < key) i++;
            }
            insertNonFull(node.children[i], key, value);
        }
    }

    /** Standard B-Tree split: median key+value moved up to parent. */
    private void splitChild(BTreeNode parent, int i, BTreeNode child) throws IOException {
        BTreeNode right = new BTreeNode(t, child.leaf, allocateSlot());
        right.n = t - 1;
        for (int j = 0; j < t-1; j++) {
            right.keys[j]   = child.keys[j+t];
            right.values[j] = child.values[j+t];
        }
        if (!child.leaf) {
            for (int j = 0; j < t; j++) right.children[j] = child.children[j+t];
        }
        child.n = t - 1;

        for (int j = parent.n; j >= i+1; j--) parent.children[j+1] = parent.children[j];
        parent.children[i+1] = right;
        for (int j = parent.n-1; j >= i; j--) {
            parent.keys[j+1]   = parent.keys[j];
            parent.values[j+1] = parent.values[j];
        }
        parent.keys[i]   = child.keys[t-1];
        parent.values[i] = child.values[t-1];
        parent.n++;

        writeNode(child); writeNode(right); writeNode(parent);
    }

    public void remove(int key) throws IOException {
        if (root == null) return;
        remove(root, key);
        if (root.n == 0 && !root.leaf) root = root.children[0];
    }

    private void remove(BTreeNode node, int key) throws IOException {
        int idx = findKey(node, key);
        if (idx < node.n && node.keys[idx] == key) {
            if (node.leaf) removeFromLeaf(node, idx);
            else           removeFromInternal(node, idx);
        } else {
            if (node.leaf) return; // not present
            boolean last = (idx == node.n);
            if (node.children[idx].n < t) fill(node, idx);
            if (last && idx > node.n) remove(node.children[idx-1], key);
            else                      remove(node.children[idx],   key);
        }
    }

    private void removeFromLeaf(BTreeNode node, int idx) throws IOException {
        for (int i = idx+1; i < node.n; i++) {
            node.keys[i-1]   = node.keys[i];
            node.values[i-1] = node.values[i];
        }
        node.n--;
        writeNode(node);
    }

    private void removeFromInternal(BTreeNode node, int idx) throws IOException {
        int key = node.keys[idx];
        if (node.children[idx].n >= t) {
            // Replace with in-order predecessor
            BTreeNode pred = node.children[idx];
            while (!pred.leaf) pred = pred.children[pred.n];
            node.keys[idx]   = pred.keys[pred.n-1];
            node.values[idx] = pred.values[pred.n-1];
            writeNode(node);
            remove(node.children[idx], node.keys[idx]);
        } else if (node.children[idx+1].n >= t) {
            // Replace with in-order successor
            BTreeNode succ = node.children[idx+1];
            while (!succ.leaf) succ = succ.children[0];
            node.keys[idx]   = succ.keys[0];
            node.values[idx] = succ.values[0];
            writeNode(node);
            remove(node.children[idx+1], node.keys[idx]);
        } else {
            mergeChildren(node, idx);
            remove(node.children[idx], key);
        }
    }

    private void fill(BTreeNode node, int idx) throws IOException {
        if (idx != 0 && node.children[idx-1].n >= t)      borrowFromPrev(node, idx);
        else if (idx != node.n && node.children[idx+1].n >= t) borrowFromNext(node, idx);
        else if (idx != node.n)                            mergeChildren(node, idx);
        else                                               mergeChildren(node, idx-1);
    }

    private void borrowFromPrev(BTreeNode node, int idx) throws IOException {
        BTreeNode child   = node.children[idx];
        BTreeNode sibling = node.children[idx-1];
        for (int i = child.n-1; i >= 0; i--) {
            child.keys[i+1]   = child.keys[i];
            child.values[i+1] = child.values[i];
        }
        if (!child.leaf) for (int i = child.n; i >= 0; i--) child.children[i+1] = child.children[i];
        child.keys[0]   = node.keys[idx-1];
        child.values[0] = node.values[idx-1];
        if (!child.leaf) child.children[0] = sibling.children[sibling.n];
        node.keys[idx-1]   = sibling.keys[sibling.n-1];
        node.values[idx-1] = sibling.values[sibling.n-1];
        child.n++; sibling.n--;
        writeNode(child); writeNode(sibling); writeNode(node);
    }

    private void borrowFromNext(BTreeNode node, int idx) throws IOException {
        BTreeNode child   = node.children[idx];
        BTreeNode sibling = node.children[idx+1];
        child.keys[child.n]   = node.keys[idx];
        child.values[child.n] = node.values[idx];
        if (!child.leaf) child.children[child.n+1] = sibling.children[0];
        node.keys[idx]   = sibling.keys[0];
        node.values[idx] = sibling.values[0];
        for (int i = 1; i < sibling.n; i++) {
            sibling.keys[i-1]   = sibling.keys[i];
            sibling.values[i-1] = sibling.values[i];
        }
        if (!sibling.leaf) for (int i = 1; i <= sibling.n; i++) sibling.children[i-1] = sibling.children[i];
        child.n++; sibling.n--;
        writeNode(child); writeNode(sibling); writeNode(node);
    }

    private void mergeChildren(BTreeNode node, int idx) throws IOException {
        BTreeNode left  = node.children[idx];
        BTreeNode right = node.children[idx+1];
        left.keys[t-1]   = node.keys[idx];
        left.values[t-1] = node.values[idx];
        for (int i = 0; i < right.n; i++) {
            left.keys[i+t]   = right.keys[i];
            left.values[i+t] = right.values[i];
        }
        if (!left.leaf) for (int i = 0; i <= right.n; i++) left.children[i+t] = right.children[i];
        for (int i = idx+1; i < node.n; i++) {
            node.keys[i-1]       = node.keys[i];
            node.values[i-1]     = node.values[i];
            node.children[i]     = node.children[i+1];
        }
        left.n += right.n + 1;
        node.n--;
        writeNode(left); writeNode(node);
    }

    // -----------------------------------------------------------------------
    // Disk I/O — values stored as raw longs, not ASCII strings
    // -----------------------------------------------------------------------

    private void writeNode(BTreeNode node) throws IOException {
        file.seek(node.position);
        file.writeInt(node.n);
        file.writeBoolean(node.leaf);
        for (int i = 0; i < 2*t-1; i++) file.writeInt(i < node.n ? node.keys[i] : 0);
        for (int i = 0; i < 2*t-1; i++) {
            file.writeLong((i < node.n && node.values[i] != null) ? node.values[i] : -1L);
        }
        for (int i = 0; i < 2*t; i++) {
            file.writeLong(node.children[i] != null ? node.children[i].position : -1L);
        }
    }

    private BTreeNode readNode(long position) throws IOException {
        file.seek(position);
        int     n    = file.readInt();
        boolean leaf = file.readBoolean();
        BTreeNode node = new BTreeNode(t, leaf, position);
        node.n = n;
        for (int i = 0; i < 2*t-1; i++) node.keys[i] = file.readInt();
        for (int i = 0; i < 2*t-1; i++) {
            long v = file.readLong();
            node.values[i] = (v == -1L) ? null : v;
        }
        // Read all child positions first, then recurse — avoids file-pointer corruption
        long[] childPositions = new long[2*t];
        for (int i = 0; i < 2*t; i++) childPositions[i] = file.readLong();
        for (int i = 0; i < 2*t; i++) {
            if (childPositions[i] != -1L) node.children[i] = readNode(childPositions[i]);
        }
        return node;
    }

    private long allocateSlot() { return nodeCounter++ * SLOT_SIZE; }

    private int findKey(BTreeNode node, int key) {
        int i = 0;
        while (i < node.n && node.keys[i] < key) i++;
        return i;
    }

    public void traverse() { if (root != null) { root.traverse(); System.out.println(); } }
}
