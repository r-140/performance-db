package com.iu.indexes.btreebased.btree;
import java.io.*;
import java.util.logging.Logger;

public class BTreeIndex {
    private static final Logger LOGGER = Logger.getLogger(BTreeIndex.class.getName());
    private BTreeNode root;
    private int t;
    private RandomAccessFile file;
    private long nodeCounter = 0;

    public BTreeIndex(String fileName, int t) throws IOException {
        this.t = t;
        this.file = new RandomAccessFile(fileName, "rw");
        if (file.length() == 0) {
            root = new BTreeNode(t, true, allocateNodePosition());
            writeNode(root);
        } else {
            root = readNode(0);
        }
    }

    private long allocateNodePosition() throws IOException {
        return nodeCounter++ * (4 + 4 * (2 * t - 1) + 256 * (2 * t - 1) + 8 * (2 * t));
    }

    private void writeNode(BTreeNode node) throws IOException {
        file.seek(node.position);
        file.writeInt(node.n);
        file.writeBoolean(node.leaf);
        for (int i = 0; i < 2 * t - 1; i++) {
            file.writeInt(i < node.n ? node.keys[i] : 0);
            writeLong(file, String.valueOf(i < node.n ? node.values[i] : null));
        }
        for (int i = 0; i < 2 * t; i++) {
            file.writeLong(i < node.n + 1 ? (node.children[i] != null ? node.children[i].position : -1) : -1);
        }
    }

    private void writeLong(RandomAccessFile file, String value) throws IOException {
        byte[] buffer = new byte[256];
        if (value != null) {
            byte[] strBytes = value.getBytes();
            System.arraycopy(strBytes, 0, buffer, 0, Math.min(strBytes.length, buffer.length));
        }
        file.write(buffer);
    }

    private BTreeNode readNode(long position) throws IOException {
        file.seek(position);
        int n = file.readInt();
        boolean leaf = file.readBoolean();
        BTreeNode node = new BTreeNode(t, leaf, position);
        node.n = n;
        for (int i = 0; i < 2 * t - 1; i++) {
            node.keys[i] = file.readInt();
            node.values[i] = Long.valueOf(readString(file));
        }
        for (int i = 0; i < 2 * t; i++) {
            long childPos = file.readLong();
            if (childPos != -1) {
                node.children[i] = readNode(childPos);
            }
        }
        return node;
    }

    private String readString(RandomAccessFile file) throws IOException {
        byte[] buffer = new byte[256];
        file.read(buffer);
        return new String(buffer).trim();
    }

    public void traverse() {
        if (root != null) {
            root.traverse();
        }
    }

    public Long search(int key) {
        return root == null ? null : root.search(key);
    }

    public void insert(int key, Long value) throws IOException {
        try {
            if (root.n == 2 * t - 1) {
                BTreeNode s = new BTreeNode(t, false, allocateNodePosition());
                s.children[0] = root;
                splitChild(s, 0, root);
                root = s;
            }
            insertNonFull(root, key, value);
        } catch (Exception e) {
            LOGGER.info(String.format("Exception has been thrown during insertion " +
                    "key %s and value %s with messege %s", key, value, e.getMessage()));
        }

    }

    private void insertNonFull(BTreeNode node, int key, Long value) throws IOException {
        int i = node.n - 1;
        if (node.leaf) {
            while (i >= 0 && node.keys[i] > key) {
                node.keys[i + 1] = node.keys[i];
                node.values[i + 1] = node.values[i];
                i--;
            }
            node.keys[i + 1] = key;
            node.values[i + 1] = value;
            node.n++;
            writeNode(node);
        } else {
            while (i >= 0 && node.keys[i] > key) {
                i--;
            }
            i++;
            if (node.children[i].n == 2 * t - 1) {
                splitChild(node, i, node.children[i]);
                if (node.keys[i] < key) {
                    i++;
                }
            }
            insertNonFull(node.children[i], key, value);
        }
    }

    private void splitChild(BTreeNode parent, int i, BTreeNode child) throws IOException {
        BTreeNode newNode = new BTreeNode(child.t, child.leaf, allocateNodePosition());
        newNode.n = t - 1;
        for (int j = 0; j < t - 1; j++) {
            newNode.keys[j] = child.keys[j + t];
            newNode.values[j] = child.values[j + t];
        }
        if (!child.leaf) {
            for (int j = 0; j < t; j++) {
                newNode.children[j] = child.children[j + t];
            }
        }
        child.n = t - 1;
        for (int j = parent.n; j >= i + 1; j--) {
            parent.children[j + 1] = parent.children[j];
        }
        parent.children[i + 1] = newNode;
        for (int j = parent.n - 1; j >= i; j--) {
            parent.keys[j + 1] = parent.keys[j];
            parent.values[j + 1] = parent.values[j];
        }
        parent.keys[i] = child.keys[t - 1];
        parent.values[i] = child.values[t - 1];
        parent.n++;
        writeNode(child);
        writeNode(newNode);
        writeNode(parent);
    }

    public void remove(int key) throws IOException {
        if (root == null) {
            System.out.println("The tree is empty");
            return;
        }
        remove(root, key);
        if (root.n == 0) {
            if (root.leaf) {
                root = null;
            } else {
                root = root.children[0];
            }
        }
    }

    private void remove(BTreeNode node, int key) throws IOException {
        int idx = findKey(node, key);
        if (idx < node.n && node.keys[idx] == key) {
            if (node.leaf) {
                removeFromLeaf(node, idx);
            } else {
                removeFromNonLeaf(node, idx);
            }
        } else {
            if (node.leaf) {
                System.out.println("The key " + key + " is not present in the tree");
                return;
            }
            boolean flag = (idx == node.n);
            if (node.children[idx].n < t) {
                fill(node, idx);
            }
            if (flag && idx > node.n) {
                remove(node.children[idx - 1], key);
            } else {
                remove(node.children[idx], key);
            }
        }
    }

    private void removeFromLeaf(BTreeNode node, int idx) throws IOException {
        for (int i = idx + 1; i < node.n; ++i) {
            node.keys[i - 1] = node.keys[i];
            node.values[i - 1] = node.values[i];
        }
        node.n--;
        writeNode(node);
    }

    private void removeFromNonLeaf(BTreeNode node, int idx) throws IOException {
        int key = node.keys[idx];
        if (node.children[idx].n >= t) {
            int pred = getPredecessor(node, idx);
            node.keys[idx] = pred;
            node.values[idx] = search(pred);
            remove(node.children[idx], pred);
        } else if (node.children[idx + 1].n >= t) {
            int succ = getSuccessor(node, idx);
            node.keys[idx] = succ;
            node.values[idx] = search(succ);
            remove(node.children[idx + 1], succ);
        } else {
            merge(node, idx);
            remove(node.children[idx], key);
        }
    }

    private int getPredecessor(BTreeNode node, int idx) {
        BTreeNode cur = node.children[idx];
        while (!cur.leaf) {
            cur = cur.children[cur.n];
        }
        return cur.keys[cur.n - 1];
    }

    private int getSuccessor(BTreeNode node, int idx) {
        BTreeNode cur = node.children[idx + 1];
        while (!cur.leaf) {
            cur = cur.children[0];
        }
        return cur.keys[0];
    }

    private void fill(BTreeNode node, int idx) throws IOException {
        if (idx != 0 && node.children[idx - 1].n >= t) {
            borrowFromPrev(node, idx);
        } else if (idx != node.n && node.children[idx + 1].n >= t) {
            borrowFromNext(node, idx);
        } else {
            if (idx != node.n) {
                merge(node, idx);
            } else {
                merge(node, idx - 1);
            }
        }
    }

    private void borrowFromPrev(BTreeNode node, int idx) throws IOException {
        BTreeNode child = node.children[idx];
        BTreeNode sibling = node.children[idx - 1];
        for (int i = child.n - 1; i >= 0; --i) {
            child.keys[i + 1] = child.keys[i];
            child.values[i + 1] = child.values[i];
        }
        if (!child.leaf) {
            for (int i = child.n; i >= 0; --i) {
                child.children[i + 1] = child.children[i];
            }
        }
        child.keys[0] = node.keys[idx - 1];
        child.values[0] = node.values[idx - 1];
        if (!child.leaf) {
            child.children[0] = sibling.children[sibling.n];
        }
        node.keys[idx - 1] = sibling.keys[sibling.n - 1];
        node.values[idx - 1] = sibling.values[sibling.n - 1];
        child.n++;
        sibling.n--;
        writeNode(child);
        writeNode(sibling);
        writeNode(node);
    }

    private void borrowFromNext(BTreeNode node, int idx) throws IOException {
        BTreeNode child = node.children[idx];
        BTreeNode sibling = node.children[idx + 1];
        child.keys[child.n] = node.keys[idx];
        child.values[child.n] = node.values[idx];
        if (!child.leaf) {
            child.children[child.n + 1] = sibling.children[0];
        }
        node.keys[idx] = sibling.keys[0];
        node.values[idx] = sibling.values[0];
        for (int i = 1; i < sibling.n; ++i) {
            sibling.keys[i - 1] = sibling.keys[i];
            sibling.values[i - 1] = sibling.values[i];
        }
        if (!sibling.leaf) {
            for (int i = 1; i <= sibling.n; ++i) {
                sibling.children[i - 1] = sibling.children[i];
            }
        }
        child.n++;
        sibling.n--;
        writeNode(child);
        writeNode(sibling);
        writeNode(node);
    }

    private void merge(BTreeNode node, int idx) throws IOException {
        BTreeNode child = node.children[idx];
        BTreeNode sibling = node.children[idx + 1];
        child.keys[t - 1] = node.keys[idx];
        child.values[t - 1] = node.values[idx];
        for (int i = 0; i < sibling.n; ++i) {
            child.keys[i + t] = sibling.keys[i];
            child.values[i + t] = sibling.values[i];
        }
        if (!child.leaf) {
            for (int i = 0; i <= sibling.n; ++i) {
                child.children[i + t] = sibling.children[i];
            }
        }
        for (int i = idx + 1; i < node.n; ++i) {
            node.keys[i - 1] = node.keys[i];
            node.values[i - 1] = node.values[i];
        }
        for (int i = idx + 2; i <= node.n; ++i) {
            node.children[i - 1] = node.children[i];
        }
        child.n += sibling.n + 1;
        node.n--;
        writeNode(child);
        writeNode(node);
    }

    private int findKey(BTreeNode node, int key) {
        int idx = 0;
        while (idx < node.n && node.keys[idx] < key) {
            ++idx;
        }
        return idx;
    }
}
