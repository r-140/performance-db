package com.iu.indexes.btreebased.bplustree;

import java.io.*;

public class BPlusTreeIndex {
    private BPlusTreeNode root;
    private int t;
    private RandomAccessFile file;
    private long nodeCounter = 0;

    public BPlusTreeIndex(String fileName, int t) throws IOException {
        this.t = t;
        this.file = new RandomAccessFile(fileName, "rw");
        if (file.length() == 0) {
            root = new BPlusTreeNode(t, true, allocateNodePosition());
            writeNode(root);
        } else {
            root = readNode(0);
        }
    }

    private long allocateNodePosition() throws IOException {
        return nodeCounter++ * (4 + 4 * (2 * t - 1) + 256 * (2 * t - 1) + 8 * (2 * t) + 8);
    }

    private void writeNode(BPlusTreeNode node) throws IOException {
        file.seek(node.position);
        file.writeInt(node.n);
        file.writeBoolean(node.leaf);
        file.writeLong(node.next);
        for (int i = 0; i < 2 * t - 1; i++) {
            file.writeInt(i < node.n ? node.keys[i] : 0);
            writeString(file, String.valueOf(i < node.n ? node.values[i] : null));
        }
        for (int i = 0; i < 2 * t; i++) {
            file.writeLong(i < node.n + 1 ? (node.children[i] != null ? node.children[i].position : -1) : -1);
        }
    }

    private void writeString(RandomAccessFile file, String value) throws IOException {
        byte[] buffer = new byte[256];
        if (value != null) {
            byte[] strBytes = value.getBytes();
            System.arraycopy(strBytes, 0, buffer, 0, Math.min(strBytes.length, buffer.length));
        }
        file.write(buffer);
    }

    private BPlusTreeNode readNode(long position) throws IOException {
        file.seek(position);
        int n = file.readInt();
        boolean leaf = file.readBoolean();
        long next = file.readLong();
        BPlusTreeNode node = new BPlusTreeNode(t, leaf, position);
        node.n = n;
        node.next = next;
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
        if (root.n == 2 * t - 1) {
            BPlusTreeNode s = new BPlusTreeNode(t, false, allocateNodePosition());
            s.children[0] = root;
            splitChild(s, 0, root);
            root = s;
        }
        insertNonFull(root, key, value);
    }

    private void insertNonFull(BPlusTreeNode node, int key, Long value) throws IOException {
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

    private void splitChild(BPlusTreeNode parent, int i, BPlusTreeNode child) throws IOException {
        BPlusTreeNode newNode = new BPlusTreeNode(child.t, child.leaf, allocateNodePosition());
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
        if (child.leaf) {
            newNode.next = child.next;
            child.next = newNode.position;
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

    private void remove(BPlusTreeNode node, int key) throws IOException {
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

    private void removeFromLeaf(BPlusTreeNode node, int idx) throws IOException {
        for (int i = idx + 1; i < node.n; ++i) {
            node.keys[i - 1] = node.keys[i];
            node.values[i - 1] = node.values[i];
        }
        node.n--;
        writeNode(node);
    }

    private void removeFromNonLeaf(BPlusTreeNode node, int idx) throws IOException {
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

    private int getPredecessor(BPlusTreeNode node, int idx) {
        BPlusTreeNode cur = node.children[idx];
        while (!cur.leaf) {
            cur = cur.children[cur.n];
        }
        return cur.keys[cur.n - 1];
    }

    private int getSuccessor(BPlusTreeNode node, int idx) {
        BPlusTreeNode cur = node.children[idx + 1];
        while (!cur.leaf) {
            cur = cur.children[0];
        }
        return cur.keys[0];
    }

    private void fill(BPlusTreeNode node, int idx) throws IOException {
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

    private void borrowFromPrev(BPlusTreeNode node, int idx) throws IOException {
        BPlusTreeNode child = node.children[idx];
        BPlusTreeNode sibling = node.children[idx - 1];
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

    private void borrowFromNext(BPlusTreeNode node, int idx) throws IOException {
        BPlusTreeNode child = node.children[idx];
        BPlusTreeNode sibling = node.children[idx + 1];
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

    private void merge(BPlusTreeNode node, int idx) throws IOException {
        BPlusTreeNode child = node.children[idx];
        BPlusTreeNode sibling = node.children[idx + 1];
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

    private int findKey(BPlusTreeNode node, int key) {
        int idx = 0;
        while (idx < node.n && node.keys[idx] < key) {
            ++idx;
        }
        return idx;
    }

//    public static void main(String[] args) {
//        try {
//            BPlusTree bPlusTree = new BPlusTree("bplustree.dat", 3);
//
//            bPlusTree.insert(10, "Value10");
//            bPlusTree.insert(20, "Value20");
//            bPlusTree.insert(5, "Value5");
//            bPlusTree.insert(6, "Value6");
//            bPlusTree.insert(12, "Value12");
//            bPlusTree.insert(30, "Value30");
//            bPlusTree.insert(7, "Value7");
//            bPlusTree.insert(17, "Value17");
//
//            System.out.println("Traversal of the constructed B+ tree is:");
//            bPlusTree.traverse();
//
//            System.out.println("\nRemoving key 6:");
//            bPlusTree.remove(6);
//            bPlusTree.traverse();
//
//            int key = 12;
//            System.out.println("\nSearching for key " + key);
//            String result = bPlusTree.search(key);
//            if (result != null) {
//                System.out.println("Key " + key + " found with value: " + result);
//            } else {
//                System.out.println("Key " + key + " not found in the B+ tree.");
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
}

