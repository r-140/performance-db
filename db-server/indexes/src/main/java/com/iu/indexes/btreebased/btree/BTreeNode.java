package com.iu.indexes.btreebased.btree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

class BTreeNode {
    int t;  // Минимальная степень B-дерева
    List<Integer> keys;  // Список ключей в узле
    List<Long> values;  // Список значений (смещения данных на диске)
    List<Long> children;  // Список дочерних узлов (позиции в файле)
    boolean isLeaf;  // Листовой узел или нет
    long position;  // Позиция узла в файле

    public BTreeNode(int t, boolean isLeaf) {
        this.t = t;
        this.isLeaf = isLeaf;
        this.keys = new ArrayList<>();
        this.values = new ArrayList<>();
        this.children = new ArrayList<>();
    }

    public void insertNonFull(int key, long value, BTreeIndex tree) throws IOException {
        int i = keys.size() - 1;

        if (isLeaf) {
            keys.add(0);  // Добавляем временный ключ
            values.add(0L);  // Добавляем временное значение
            while (i >= 0 && keys.get(i) > key) {
                keys.set(i + 1, keys.get(i));
                values.set(i + 1, values.get(i));
                i--;
            }
            keys.set(i + 1, key);
            values.set(i + 1, value);
            tree.writeNode(this);
        } else {
            while (i >= 0 && keys.get(i) > key) {
                i--;
            }
            i++;
            BTreeNode child = tree.readNode(children.get(i));
            if (child.keys.size() == 2 * t - 1) {
                splitChild(i, child, tree);
                if (keys.get(i) < key) {
                    i++;
                }
            }
            tree.readNode(children.get(i)).insertNonFull(key, value, tree);
        }
    }

    public void splitChild(int i, BTreeNode y, BTreeIndex tree) throws IOException {
        BTreeNode z = new BTreeNode(y.t, y.isLeaf);
        z.position = tree.allocateNodePosition();
        for (int j = 0; j < t - 1; j++) {
            z.keys.add(y.keys.remove(t));
            z.values.add(y.values.remove(t));
        }

        if (!y.isLeaf) {
            for (int j = 0; j < t; j++) {
                z.children.add(y.children.remove(t));
            }
        }

        children.add(i + 1, z.position);
        keys.add(i, y.keys.remove(t - 1));
        values.add(i, y.values.remove(t - 1));

        tree.writeNode(y);
        tree.writeNode(z);
        tree.writeNode(this);
    }

    public void traverse(BTreeIndex tree) throws IOException {
        int i;
        for (i = 0; i < keys.size(); i++) {
            if (!isLeaf) {
                tree.readNode(children.get(i)).traverse(tree);
            }
            System.out.print(" " + keys.get(i) + ":" + values.get(i));
        }

        if (!isLeaf) {
            tree.readNode(children.get(i)).traverse(tree);
        }
    }

    public Long search(int key, BTreeIndex tree) throws IOException {
        int i = 0;
        while (i < keys.size() && key > keys.get(i)) {
            i++;
        }

        if (i < keys.size() && keys.get(i) == key) {
            return values.get(i);
        }

        if (isLeaf) {
            return null;
        }

        return tree.readNode(children.get(i)).search(key, tree);
    }
}

