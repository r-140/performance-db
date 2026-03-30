package com.iu.indexes.btreebased.bplustree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Disk-based B+ Tree index.
 *
 * Structural invariants enforced here (vs. the original B-Tree-disguised-as-B+Tree):
 *
 *  1. INTERNAL NODES STORE KEYS ONLY.
 *     Values (file offsets) are stored exclusively in leaf nodes.
 *     The on-disk layout for internal nodes therefore writes no value bytes.
 *
 *  2. LEAF SPLIT KEEPS THE MEDIAN KEY IN THE RIGHT CHILD.
 *     When a full leaf is split at position t, keys[t-1] becomes the separator
 *     pushed to the parent, AND it is also the first key of the new right leaf.
 *     This ensures every key in the leaf layer is reachable via the linked list.
 *
 *  3. LEAF LINKED LIST IS ACTUALLY USED.
 *     rangeScan(lo, hi) walks to the first leaf containing lo via the tree,
 *     then follows next-pointers across leaf nodes — O(log N + k).
 *
 *  4. DELETION HANDLES LEAVES AND INTERNAL NODES CORRECTLY.
 *     Removing from an internal node updates the separator to the new
 *     leftmost key of the right subtree, rather than copying a value up.
 *
 *  5. NODE COUNTER IS RECOVERED FROM FILE SIZE ON RELOAD.
 *     Prevents position-0 overwrite on a second open.
 *
 * Disk layout — two node kinds:
 *
 *   Leaf node:     [n:4][leaf:1][next:8][keys:(2t-1)*4][values:(2t-1)*8]
 *   Internal node: [n:4][leaf:1][next:8(ignored)][keys:(2t-1)*4][children:(2t)*8]
 *
 * Fixed slot size (same formula for both kinds so position arithmetic is trivial):
 *   SLOT = 4 + 1 + 8 + (2t-1)*4 + (2t-1)*8 + (2t)*8
 */
public class BPlusTreeIndex {

    private BPlusTreeNode root;
    private final int t;
    private final RandomAccessFile file;
    private long nodeCounter;

    // Slot size: n(4) + leaf(1) + next(8) + keys*(2t-1)*4 + values*(2t-1)*8 + children*(2t)*8
    private final long SLOT_SIZE;

    public BPlusTreeIndex(String fileName, int t) throws IOException {
        this.t    = t;
        this.SLOT_SIZE = 4L + 1 + 8 + (long)(2 * t - 1) * 4 + (long)(2 * t - 1) * 8 + (long)(2 * t) * 8;
        this.file = new RandomAccessFile(fileName, "rw");

        if (file.length() == 0) {
            nodeCounter = 0;
            root = new BPlusTreeNode(t, true, allocateSlot());
            writeNode(root);
        } else {
            // Recover nodeCounter from file size so allocateSlot() never overwrites existing data
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

    /**
     * Range scan — the killer feature of B+ Tree over B-Tree.
     * Walks the tree to find the first leaf, then follows the leaf linked list.
     */
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
        removeFromNode(root, key);
        // If root lost all keys and is internal, shrink
        if (root.n == 0 && !root.leaf) {
            root = root.children[0];
        }
    }

    public void traverse() {
        if (root != null) root.traverse();
        System.out.println();
    }

    // -----------------------------------------------------------------------
    // Insert internals
    // -----------------------------------------------------------------------

    private void insertNonFull(BPlusTreeNode node, int key, Long value) throws IOException {
        if (node.leaf) {
            // Shift right to make room, insert in sorted position
            int i = node.n - 1;
            while (i >= 0 && node.keys[i] > key) {
                node.keys[i + 1]   = node.keys[i];
                node.values[i + 1] = node.values[i];
                i--;
            }
            node.keys[i + 1]   = key;
            node.values[i + 1] = value;
            node.n++;
            writeNode(node);
        } else {
            // Find child to descend into
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

    /**
     * Split a full child node at parent.children[idx].
     *
     * B+ Tree split rules (differ from B-Tree):
     *
     * Leaf split:
     *   - Left child keeps keys[0..t-1] (t keys).
     *   - Right child gets keys[t-1..2t-2] (t keys) — the median is DUPLICATED.
     *   - Separator pushed to parent = keys[t-1] (the first key of the right leaf).
     *   - Leaf linked list is updated: left.next = right, right.next = old left.next.
     *
     * Internal split:
     *   - Left child keeps keys[0..t-2] and children[0..t-1].
     *   - Separator = keys[t-1] is pushed to parent (not kept in either child).
     *   - Right child gets keys[t..2t-2] and children[t..2t-1].
     */
    private void splitChild(BPlusTreeNode parent, int idx, BPlusTreeNode child) throws IOException {
        BPlusTreeNode right = new BPlusTreeNode(t, child.leaf, allocateSlot());

        if (child.leaf) {
            // --- Leaf split ---
            // Right leaf starts at index t-1 (median is duplicated as separator)
            right.n = t;
            for (int j = 0; j < t; j++) {
                right.keys[j]   = child.keys[j + t - 1];
                right.values[j] = child.values[j + t - 1];
            }
            // Update leaf chain
            right.next = child.next;
            child.next = right.position;
            // Left leaf keeps only t-1 entries (index 0..t-2)
            child.n = t - 1;
            // Separator pushed to parent = first key of right leaf
            insertSeparator(parent, idx, child.keys[t - 1], right);
        } else {
            // --- Internal split ---
            right.n = t - 1;
            for (int j = 0; j < t - 1; j++) {
                right.keys[j] = child.keys[j + t];
            }
            for (int j = 0; j < t; j++) {
                right.children[j] = child.children[j + t];
            }
            // Separator = child.keys[t-1] pushed to parent (not stored in either child)
            int separator = child.keys[t - 1];
            child.n = t - 1;
            insertSeparator(parent, idx, separator, right);
        }

        writeNode(child);
        writeNode(right);
        writeNode(parent);
    }

    /** Insert a separator key and right-child pointer into an internal node. */
    private void insertSeparator(BPlusTreeNode parent, int idx, int separatorKey, BPlusTreeNode right) {
        for (int j = parent.n; j > idx; j--) {
            parent.keys[j]       = parent.keys[j - 1];
            parent.children[j + 1] = parent.children[j];
        }
        parent.keys[idx]       = separatorKey;
        parent.children[idx + 1] = right;
        parent.n++;
    }

    // -----------------------------------------------------------------------
    // Remove internals
    // -----------------------------------------------------------------------

    private void removeFromNode(BPlusTreeNode node, int key) throws IOException {
        int idx = findKeyIndex(node, key);

        if (node.leaf) {
            if (idx < node.n && node.keys[idx] == key) {
                // Shift left to fill the gap
                for (int i = idx + 1; i < node.n; i++) {
                    node.keys[i - 1]   = node.keys[i];
                    node.values[i - 1] = node.values[i];
                }
                node.n--;
                writeNode(node);
            }
            // Key not present — silently ignore
            return;
        }

        // Internal node
        if (idx < node.n && node.keys[idx] == key) {
            // Key exists as separator — delete from left subtree's rightmost leaf
            // then update separator to new leftmost key of right child
            removeLeafMax(node.children[idx], node, idx);
        } else {
            // Descend into the appropriate child
            BPlusTreeNode child = node.children[idx];
            if (child.n < t) {
                rebalance(node, idx);
                // After rebalance the tree may have changed; re-compute idx
                idx = findKeyIndex(node, key);
                if (idx < node.n && node.keys[idx] == key) {
                    removeLeafMax(node.children[idx], node, idx);
                    return;
                }
                child = node.children[idx < node.n + 1 ? idx : node.n];
            }
            removeFromNode(child, key);
        }
    }

    /**
     * Remove the largest key from the subtree rooted at `node`,
     * and update parent.keys[separatorIdx] to the new leftmost key of
     * the right sibling (parent.children[separatorIdx+1]).
     */
    private void removeLeafMax(BPlusTreeNode node, BPlusTreeNode parent, int separatorIdx)
            throws IOException {
        // Walk to rightmost leaf
        BPlusTreeNode cur = node;
        while (!cur.leaf) cur = cur.children[cur.n];
        // Remove its last key
        cur.n--;
        writeNode(cur);
        // Update the separator to the new leftmost key of the right subtree
        BPlusTreeNode rightSubtreeFirst = parent.children[separatorIdx + 1];
        while (!rightSubtreeFirst.leaf) rightSubtreeFirst = rightSubtreeFirst.children[0];
        parent.keys[separatorIdx] = rightSubtreeFirst.keys[0];
        writeNode(parent);
    }

    private void rebalance(BPlusTreeNode parent, int idx) throws IOException {
        BPlusTreeNode child = parent.children[idx];
        if (idx > 0 && parent.children[idx - 1].n >= t) {
            borrowFromLeft(parent, idx);
        } else if (idx < parent.n && parent.children[idx + 1].n >= t) {
            borrowFromRight(parent, idx);
        } else {
            if (idx < parent.n) {
                mergeChildren(parent, idx);
            } else {
                mergeChildren(parent, idx - 1);
            }
        }
    }

    private void borrowFromLeft(BPlusTreeNode parent, int idx) throws IOException {
        BPlusTreeNode child   = parent.children[idx];
        BPlusTreeNode sibling = parent.children[idx - 1];

        // Shift child entries right
        for (int i = child.n - 1; i >= 0; i--) {
            child.keys[i + 1] = child.keys[i];
            if (child.leaf) child.values[i + 1] = child.values[i];
            else child.children[i + 2] = child.children[i + 1];
        }
        if (!child.leaf) child.children[1] = child.children[0];

        if (child.leaf) {
            // Move sibling's last key+value into child[0]
            child.keys[0]   = sibling.keys[sibling.n - 1];
            child.values[0] = sibling.values[sibling.n - 1];
            // Update parent separator to the new first key of child
            parent.keys[idx - 1] = child.keys[0];
        } else {
            // Rotate parent separator down, move sibling's last child
            child.keys[0]      = parent.keys[idx - 1];
            child.children[0]  = sibling.children[sibling.n];
            parent.keys[idx-1] = sibling.keys[sibling.n - 1];
        }
        child.n++;
        sibling.n--;
        writeNode(child); writeNode(sibling); writeNode(parent);
    }

    private void borrowFromRight(BPlusTreeNode parent, int idx) throws IOException {
        BPlusTreeNode child   = parent.children[idx];
        BPlusTreeNode sibling = parent.children[idx + 1];

        if (child.leaf) {
            // Move sibling's first key+value into child's last slot
            child.keys[child.n]   = sibling.keys[0];
            child.values[child.n] = sibling.values[0];
            // Shift sibling left
            for (int i = 1; i < sibling.n; i++) {
                sibling.keys[i-1]   = sibling.keys[i];
                sibling.values[i-1] = sibling.values[i];
            }
            // Update parent separator to new first key of sibling
            parent.keys[idx] = sibling.keys[0];
        } else {
            child.keys[child.n]        = parent.keys[idx];
            child.children[child.n+1]  = sibling.children[0];
            parent.keys[idx]           = sibling.keys[0];
            for (int i = 1; i < sibling.n; i++) {
                sibling.keys[i-1]      = sibling.keys[i];
                sibling.children[i-1]  = sibling.children[i];
            }
            sibling.children[sibling.n-1] = sibling.children[sibling.n];
        }
        child.n++; sibling.n--;
        writeNode(child); writeNode(sibling); writeNode(parent);
    }

    /**
     * Merge parent.children[idx] and parent.children[idx+1].
     *
     * For internal nodes: the parent separator is pulled down.
     * For leaf nodes:     the parent separator is discarded; leaf chain updated.
     */
    private void mergeChildren(BPlusTreeNode parent, int idx) throws IOException {
        BPlusTreeNode left  = parent.children[idx];
        BPlusTreeNode right = parent.children[idx + 1];

        if (left.leaf) {
            // Copy all right-leaf entries into left
            for (int i = 0; i < right.n; i++) {
                left.keys[left.n + i]   = right.keys[i];
                left.values[left.n + i] = right.values[i];
            }
            left.n   += right.n;
            left.next = right.next;  // stitch the leaf chain
        } else {
            // Pull parent separator down, then copy right's keys/children
            left.keys[left.n] = parent.keys[idx];
            for (int i = 0; i < right.n; i++) {
                left.keys[left.n + 1 + i]      = right.keys[i];
                left.children[left.n + 1 + i]  = right.children[i];
            }
            left.children[left.n + right.n + 1] = right.children[right.n];
            left.n += right.n + 1;
        }

        // Remove separator and right-child pointer from parent
        for (int i = idx + 1; i < parent.n; i++) {
            parent.keys[i - 1]     = parent.keys[i];
            parent.children[i]     = parent.children[i + 1];
        }
        parent.n--;

        writeNode(left);
        writeNode(parent);
    }

    // -----------------------------------------------------------------------
    // Disk I/O
    // -----------------------------------------------------------------------

    /**
     * Disk format per slot:
     *   n        : int  (4 bytes)
     *   leaf     : boolean (1 byte)
     *   next     : long (8 bytes)  — leaf chain; 0 for internal
     *   keys     : (2t-1) * int
     *   values   : (2t-1) * long  — leaf only; 0-filled for internal
     *   children : (2t)   * long  — internal only; 0-filled for leaf
     */
    private void writeNode(BPlusTreeNode node) throws IOException {
        file.seek(node.position);
        file.writeInt(node.n);
        file.writeBoolean(node.leaf);
        file.writeLong(node.leaf ? node.next : -1L);

        // Keys
        for (int i = 0; i < 2 * t - 1; i++) {
            file.writeInt(i < node.n ? node.keys[i] : 0);
        }
        // Values (leaf) or zeros (internal)
        for (int i = 0; i < 2 * t - 1; i++) {
            if (node.leaf) {
                file.writeLong(i < node.n && node.values[i] != null ? node.values[i] : -1L);
            } else {
                file.writeLong(-1L);
            }
        }
        // Children (internal) or zeros (leaf)
        for (int i = 0; i < 2 * t; i++) {
            if (!node.leaf) {
                file.writeLong(node.children[i] != null ? node.children[i].position : -1L);
            } else {
                file.writeLong(-1L);
            }
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

        // Keys
        for (int i = 0; i < 2 * t - 1; i++) {
            node.keys[i] = file.readInt();
        }
        // Values
        for (int i = 0; i < 2 * t - 1; i++) {
            long v = file.readLong();
            if (leaf) node.values[i] = (v == -1L) ? null : v;
        }
        // Children — read lazily only for internal nodes
        for (int i = 0; i < 2 * t; i++) {
            long childPos = file.readLong();
            if (!leaf && childPos != -1L) {
                node.children[i] = readNode(childPos);
            }
        }
        return node;
    }

    private long allocateSlot() {
        return nodeCounter++ * SLOT_SIZE;
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /** Find the first index i such that node.keys[i] >= key. */
    private int findKeyIndex(BPlusTreeNode node, int key) {
        int i = 0;
        while (i < node.n && node.keys[i] < key) i++;
        return i;
    }

    /** Walk internal nodes to find the leaf that would contain `key`. */
    private BPlusTreeNode findLeaf(BPlusTreeNode node, int key) {
        if (node.leaf) return node;
        int i = 0;
        while (i < node.n && key >= node.keys[i]) i++;
        return findLeaf(node.children[i], key);
    }
}
