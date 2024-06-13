package com.iu.indexes.btreebased.btree;

import java.io.*;
import java.util.logging.Logger;

public class BTreeIndex {
    private static final Logger LOGGER = Logger.getLogger(BTreeIndex.class.getName());
    private BTreeNode root;
    private int t;
    private RandomAccessFile file;
    private long nodeSize;

    public BTreeIndex(String fileName, int t) throws IOException {
        this.root = null;
        this.t = t;
        this.nodeSize = calculateNodeSize();
        this.file = new RandomAccessFile(fileName, "rw");
        if (file.length() == 0) {
            root = new BTreeNode(t, true);
            root.position = allocateNodePosition();
            writeNode(root);
        } else {
            root = readNode(0);
        }
    }

    private long calculateNodeSize() {
//        return 4 + (2 * t - 1) * 4 + (2 * t - 1) * 8 + 2 * t * 8 + 1 + 8;
        return (2 * t - 1);
    }

    public long allocateNodePosition() throws IOException {
        return file.length();
    }

    public void writeNode(BTreeNode node) throws IOException {
        file.seek(node.position);
        file.writeInt(node.t);
        file.writeBoolean(node.isLeaf);
        file.writeInt(node.keys.size());
        for (int key : node.keys) {
            file.writeInt(key);
        }
        for (long value : node.values) {
            file.writeLong(value);
        }
        file.writeInt(node.children.size());
        for (long childPos : node.children) {
            file.writeLong(childPos);
        }
    }

    public BTreeNode readNode(long position) throws IOException {
        file.seek(position);
        int t = file.readInt();
        boolean isLeaf = file.readBoolean();
        BTreeNode node = new BTreeNode(t, isLeaf);
        node.position = position;
        int keyCount = file.readInt();
        for (int i = 0; i < keyCount; i++) {
            node.keys.add(file.readInt());
        }
        for (int i = 0; i < keyCount; i++) {
            node.values.add(file.readLong());
        }
        int childCount = file.readInt();
        for (int i = 0; i < childCount; i++) {
            node.children.add(file.readLong());
        }
        return node;
    }

    public void traverse() throws IOException {
        if (root != null) {
            root.traverse(this);
        }
    }

    public Long search(int key) throws IOException {
        if (root == null) {
            return null;
        } else {
            return root.search(key, this);
        }
    }

    public void insert(int key, long value) throws IOException {
        try {
            if (root == null) {
                root = new BTreeNode(t, true);
                root.keys.add(key);
                root.values.add(value);
                root.position = allocateNodePosition();
                writeNode(root);
            } else {
                if (root.keys.size() == 2 * t - 1) {
                    BTreeNode s = new BTreeNode(t, false);
                    s.children.add(root.position);
                    s.position = allocateNodePosition();
                    writeNode(s);

                    s.splitChild(0, root, this);
                    int i = 0;
                    if (s.keys.getFirst() < key) {
                        i++;
                    }
                    BTreeNode child = readNode(s.children.get(i));
                    child.insertNonFull(key, value, this);

                    root = s;
                } else {
                    root.insertNonFull(key, value, this);
                }
                writeNode(root);
            }
        } catch (Exception e) {
            LOGGER.info("Exception has been thrown during adding index " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        BTreeIndex t = new BTreeIndex("btree_index.dat", 3);

        t.insert(10, 100L);
        t.insert(20, 200L);
        t.insert(5, 50L);
        t.insert(6, 60L);
        t.insert(12, 120L);
        t.insert(30, 300L);
        t.insert(7, 70L);
        t.insert(17, 170L);

        System.out.println("Traversal of the constructed tree is:");
        t.traverse();

        int k = 6;
        Long value = t.search(k);
        if (value != null) {
            System.out.println("\nKey " + k + " found with value: " + value);
        } else {
            System.out.println("\nKey " + k + " not present");
        }

        k = 15;
        value = t.search(k);
        if (value != null) {
            System.out.println("Key " + k + " found with value: " + value);
        } else {
            System.out.println("Key " + k + " not present");
        }
    }
}

