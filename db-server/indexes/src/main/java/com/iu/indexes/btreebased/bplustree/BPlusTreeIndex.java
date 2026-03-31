package com.iu.indexes.btreebased.bplustree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Disk-based B+ Tree index.
 *
 * Invariants:
 *  1. Internal nodes store KEYS ONLY — no values array.
 *  2. Leaf nodes store KEY+VALUE pairs; values are file offsets.
 *  3. All leaf nodes are linked via `next` for O(k) range scans.
 *  4. Every key in the tree appears exactly once in a leaf.
 *     Internal separators are copies of leaf keys used for routing only.
 *
 * Deletion algorithm (standard textbook "delete and underflow fix"):
 *   - Always delete from the leaf containing the target key.
 *   - If the leaf now has fewer than t-1 keys (underflow):
 *       a. Borrow from a sibling leaf if possible (borrow updates the separator).
 *       b. Otherwise merge with a sibling (removes one separator from parent).
 *   - If an internal separator equals the deleted key, update it to the new
 *     leftmost key of the right subtree after the leaf deletion.
 *
 * Disk format (fixed slot size for simple position arithmetic):
 *   [n:4][leaf:1][next:8][keys:(2t-1)*4][values:(2t-1)*8][children:(2t)*8]
 */
public class BPlusTreeIndex {

    private BPlusTreeNode root;
    private final int  t;
    private final RandomAccessFile file;
    private long nodeCounter;
    private final long SLOT_SIZE;

    public BPlusTreeIndex(String fileName, int t) throws IOException {
        this.t         = t;
        this.SLOT_SIZE = 4L + 1 + 8 + (long)(2*t-1)*4 + (long)(2*t-1)*8 + (long)(2*t)*8;
        this.file      = new RandomAccessFile(fileName, "rw");

        if (file.length() == 0) {
            nodeCounter = 0;
            root = new BPlusTreeNode(t, true, allocateSlot());
            writeNode(root);
        } else {
            nodeCounter = (file.length() + SLOT_SIZE - 1) / SLOT_SIZE;
            root = readNode(0);
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    public Long search(int key) {
        return root == null ? null : root.search(key);
    }

    public List<Long> rangeScan(int lo, int hi) {
        List<Long> result = new ArrayList<>();
        BPlusTreeNode leaf = findLeaf(root, lo);
        while (leaf != null) {
            for (int i = 0; i < leaf.n; i++) {
                if (leaf.keys[i] > hi) return result;
                if (leaf.keys[i] >= lo) result.add(leaf.values[i]);
            }
            if (leaf.next == -1) break;
            try { leaf = readNode(leaf.next); } catch (IOException e) { break; }
        }
        return result;
    }

    public void insert(int key, Long value) throws IOException {
        if (root.n == 2 * t - 1) {
            BPlusTreeNode newRoot = new BPlusTreeNode(t, false, allocateSlot());
            newRoot.children[0] = root;
            splitChild(newRoot, 0, root);
            root = newRoot;
        }
        insertNonFull(root, key, value);
    }

    public void remove(int key) throws IOException {
        if (root == null) return;
        delete(root, key);
        if (!root.leaf && root.n == 0) {
            root = root.children[0];
        }
    }

    public void traverse() {
        if (root != null) root.traverse();
        System.out.println();
    }

    // -----------------------------------------------------------------------
    // Insert
    // -----------------------------------------------------------------------

    private void insertNonFull(BPlusTreeNode node, int key, Long value) throws IOException {
        if (node.leaf) {
            int i = node.n - 1;
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
            int i = node.n - 1;
            while (i >= 0 && node.keys[i] > key) i--;
            i++;
            if (node.children[i].n == 2 * t - 1) {
                splitChild(node, i, node.children[i]);
                if (node.keys[i] <= key) i++;
            }
            insertNonFull(node.children[i], key, value);
        }
    }

    private void splitChild(BPlusTreeNode parent, int idx, BPlusTreeNode child) throws IOException {
        BPlusTreeNode right = new BPlusTreeNode(t, child.leaf, allocateSlot());
        if (child.leaf) {
            right.n = t;
            for (int j = 0; j < t; j++) {
                right.keys[j]   = child.keys[j + t - 1];
                right.values[j] = child.values[j + t - 1];
            }
            right.next  = child.next;
            child.next  = right.position;
            child.n     = t - 1;
            insertSeparator(parent, idx, right.keys[0], right);
        } else {
            right.n = t - 1;
            for (int j = 0; j < t - 1; j++) right.keys[j]      = child.keys[j + t];
            for (int j = 0; j < t;     j++) right.children[j]   = child.children[j + t];
            int sep  = child.keys[t - 1];
            child.n  = t - 1;
            insertSeparator(parent, idx, sep, right);
        }
        writeNode(child); writeNode(right); writeNode(parent);
    }

    private void insertSeparator(BPlusTreeNode parent, int idx, int sepKey, BPlusTreeNode right) {
        for (int j = parent.n; j > idx; j--) {
            parent.keys[j]         = parent.keys[j-1];
            parent.children[j+1]   = parent.children[j];
        }
        parent.keys[idx]       = sepKey;
        parent.children[idx+1] = right;
        parent.n++;
    }

    // -----------------------------------------------------------------------
    // Delete — unified recursive descent
    //
    // This implementation follows the standard algorithm:
    //   1. Descend to the leaf containing the key.
    //   2. Remove from the leaf.
    //   3. On the way back up, fix any underflow by borrowing or merging.
    //   4. If an internal separator equals the deleted key, update it to
    //      the new leftmost key of the right subtree.
    // -----------------------------------------------------------------------

    /**
     * Recursively delete `key` from the subtree rooted at `node`.
     * Returns true if `key` was actually found and removed.
     */
    private boolean delete(BPlusTreeNode node, int key) throws IOException {
        if (node.leaf) {
            return deleteFromLeaf(node, key);
        }

        // Find the child index to descend into
        int idx = findKeyIndex(node, key);

        // If the key equals an internal separator, the actual key is in the
        // right subtree (or possibly both subtrees due to B+ Tree duplication).
        // We always delete from the leaf, then update the separator if needed.
        boolean found = deleteWithUnderflowFix(node, idx, key);

        // After deletion, check if separator at idx needs updating.
        // If node.keys[idx] == key that separator still exists (we may have
        // deleted the leftmost key of the right subtree), update it.
        if (found && idx < node.n && node.keys[idx] == key) {
            // Find the new leftmost key of the right subtree
            BPlusTreeNode leftmost = node.children[idx + 1];
            while (!leftmost.leaf) leftmost = leftmost.children[0];
            if (leftmost.n > 0) {
                node.keys[idx] = leftmost.keys[0];
                writeNode(node);
            }
        }

        return found;
    }

    /**
     * Descend into the correct child at `idx`, ensuring it has enough keys,
     * then recurse. Returns whether the key was found.
     */
    private boolean deleteWithUnderflowFix(BPlusTreeNode parent, int idx, int key)
            throws IOException {
        // Ensure the child we descend into has at least t keys (so we can
        // remove one without causing underflow)
        if (parent.children[idx].n < t) {
            fixUnderflow(parent, idx);
            // After fixUnderflow the tree structure may have changed.
            // Recompute idx since a merge may have pulled the separator down.
            idx = findKeyIndex(parent, key);
            // If key now equals a separator, idx is correct; if key < all,
            // idx == 0; descend into children[idx].
        }

        // Re-check: if idx points to a separator == key, the key could be
        // in children[idx] OR children[idx+1].  In B+ Tree, the actual
        // data is always in a leaf. Since separators are copies of leaf keys,
        // descend into children[idx] first; if not found, try children[idx+1].
        boolean found = false;
        if (idx < parent.n) {
            found = delete(parent.children[idx], key);
        }
        if (!found && idx < parent.n) {
            found = delete(parent.children[idx + 1], key);
        } else if (!found && idx == parent.n) {
            found = delete(parent.children[idx], key);
        }

        return found;
    }

    /** Remove `key` from a leaf node. Returns true if found. */
    private boolean deleteFromLeaf(BPlusTreeNode leaf, int key) throws IOException {
        int idx = findKeyIndex(leaf, key);
        if (idx >= leaf.n || leaf.keys[idx] != key) return false; // not present

        for (int i = idx + 1; i < leaf.n; i++) {
            leaf.keys[i-1]   = leaf.keys[i];
            leaf.values[i-1] = leaf.values[i];
        }
        leaf.n--;
        writeNode(leaf);
        return true;
    }

    /**
     * Ensure parent.children[idx] has at least t keys before we descend into it.
     * Fix by borrowing from a sibling or merging with one.
     */
    private void fixUnderflow(BPlusTreeNode parent, int idx) throws IOException {
        // Try borrowing from left sibling
        if (idx > 0 && parent.children[idx-1].n >= t) {
            borrowFromLeft(parent, idx);
        }
        // Try borrowing from right sibling
        else if (idx < parent.n && parent.children[idx+1].n >= t) {
            borrowFromRight(parent, idx);
        }
        // Merge
        else {
            if (idx < parent.n) {
                mergeChildren(parent, idx);
            } else {
                mergeChildren(parent, idx - 1);
            }
        }
    }

    private void borrowFromLeft(BPlusTreeNode parent, int idx) throws IOException {
        BPlusTreeNode child   = parent.children[idx];
        BPlusTreeNode sibling = parent.children[idx-1];

        // Shift child entries right
        for (int i = child.n-1; i >= 0; i--) {
            child.keys[i+1] = child.keys[i];
            if (child.leaf) child.values[i+1] = child.values[i];
            else            child.children[i+2] = child.children[i+1];
        }
        if (!child.leaf) child.children[1] = child.children[0];

        if (child.leaf) {
            child.keys[0]   = sibling.keys[sibling.n-1];
            child.values[0] = sibling.values[sibling.n-1];
            parent.keys[idx-1] = child.keys[0];
        } else {
            child.keys[0]      = parent.keys[idx-1];
            child.children[0]  = sibling.children[sibling.n];
            parent.keys[idx-1] = sibling.keys[sibling.n-1];
        }
        child.n++; sibling.n--;
        writeNode(child); writeNode(sibling); writeNode(parent);
    }

    private void borrowFromRight(BPlusTreeNode parent, int idx) throws IOException {
        BPlusTreeNode child   = parent.children[idx];
        BPlusTreeNode sibling = parent.children[idx+1];

        if (child.leaf) {
            child.keys[child.n]   = sibling.keys[0];
            child.values[child.n] = sibling.values[0];
            for (int i = 1; i < sibling.n; i++) {
                sibling.keys[i-1]   = sibling.keys[i];
                sibling.values[i-1] = sibling.values[i];
            }
            parent.keys[idx] = sibling.keys[0];
        } else {
            child.keys[child.n]       = parent.keys[idx];
            child.children[child.n+1] = sibling.children[0];
            parent.keys[idx]          = sibling.keys[0];
            for (int i = 1; i < sibling.n; i++) {
                sibling.keys[i-1]     = sibling.keys[i];
                sibling.children[i-1] = sibling.children[i];
            }
            sibling.children[sibling.n-1] = sibling.children[sibling.n];
        }
        child.n++; sibling.n--;
        writeNode(child); writeNode(sibling); writeNode(parent);
    }

    private void mergeChildren(BPlusTreeNode parent, int idx) throws IOException {
        BPlusTreeNode left  = parent.children[idx];
        BPlusTreeNode right = parent.children[idx+1];

        if (left.leaf) {
            for (int i = 0; i < right.n; i++) {
                left.keys[left.n+i]   = right.keys[i];
                left.values[left.n+i] = right.values[i];
            }
            left.n   += right.n;
            left.next = right.next;
        } else {
            left.keys[left.n] = parent.keys[idx];
            for (int i = 0; i < right.n; i++) {
                left.keys[left.n+1+i]     = right.keys[i];
                left.children[left.n+1+i] = right.children[i];
            }
            left.children[left.n+right.n+1] = right.children[right.n];
            left.n += right.n + 1;
        }

        for (int i = idx+1; i < parent.n; i++) {
            parent.keys[i-1]     = parent.keys[i];
            parent.children[i]   = parent.children[i+1];
        }
        parent.n--;
        writeNode(left); writeNode(parent);
    }

    // -----------------------------------------------------------------------
    // Disk I/O
    //
    // CRITICAL: readNode() must save and restore the RandomAccessFile position
    // around every recursive child read. The file has a single shared pointer;
    // a recursive call moves it, breaking subsequent reads in the parent loop.
    // -----------------------------------------------------------------------

    private void writeNode(BPlusTreeNode node) throws IOException {
        file.seek(node.position);
        file.writeInt(node.n);
        file.writeBoolean(node.leaf);
        file.writeLong(node.leaf ? node.next : -1L);
        for (int i = 0; i < 2*t-1; i++) file.writeInt(i < node.n ? node.keys[i] : 0);
        for (int i = 0; i < 2*t-1; i++) {
            if (node.leaf) file.writeLong(i < node.n && node.values[i] != null ? node.values[i] : -1L);
            else           file.writeLong(-1L);
        }
        for (int i = 0; i < 2*t; i++) {
            if (!node.leaf) file.writeLong(node.children[i] != null ? node.children[i].position : -1L);
            else            file.writeLong(-1L);
        }
    }

    private BPlusTreeNode readNode(long position) throws IOException {
        file.seek(position);
        int     n    = file.readInt();
        boolean leaf = file.readBoolean();
        long    next = file.readLong();

        BPlusTreeNode node = new BPlusTreeNode(t, leaf, position);
        node.n    = n;
        node.next = next;

        for (int i = 0; i < 2*t-1; i++) node.keys[i] = file.readInt();
        for (int i = 0; i < 2*t-1; i++) {
            long v = file.readLong();
            if (leaf) node.values[i] = (v == -1L) ? null : v;
        }

        // Read child positions, then read each child node with position save/restore.
        // This is the critical fix: without it, recursive readNode() calls move
        // the file pointer and the parent's child-position reads are corrupted.
        long[] childPositions = new long[2*t];
        for (int i = 0; i < 2*t; i++) childPositions[i] = file.readLong();

        if (!leaf) {
            for (int i = 0; i < 2*t; i++) {
                if (childPositions[i] != -1L) {
                    node.children[i] = readNode(childPositions[i]);
                    // After returning from readNode(), the file pointer is at
                    // the END of that child's subtree — but we already read all
                    // childPositions above, so no further sequential reads needed.
                }
            }
        }

        return node;
    }

    private long allocateSlot() { return nodeCounter++ * SLOT_SIZE; }

    private int findKeyIndex(BPlusTreeNode node, int key) {
        int i = 0;
        while (i < node.n && node.keys[i] < key) i++;
        return i;
    }

    private BPlusTreeNode findLeaf(BPlusTreeNode node, int key) {
        if (node.leaf) return node;
        int i = 0;
        while (i < node.n && key >= node.keys[i]) i++;
        return findLeaf(node.children[i], key);
    }
}
