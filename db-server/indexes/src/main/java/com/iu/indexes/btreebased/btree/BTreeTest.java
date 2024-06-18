package com.iu.indexes.btreebased.btree;

import java.io.IOException;

public class BTreeTest {
    public static void main(String[] args) {
        try {
            BTreeIndex bTree = new BTreeIndex("btree.dat", 3);

            bTree.insert(10, 1l);
            bTree.insert(20, 2l);
            bTree.insert(30, 5l);
            bTree.insert(40, 6l);
            bTree.insert(50, 7l);
            bTree.insert(60, 8l);
            bTree.insert(70, 9l);
            bTree.insert(80, 17l);
            bTree.insert(90, 17l);

            System.out.println("Traversal of the constructed B-tree is:");
            bTree.traverse();

            System.out.println("\nRemoving key 6:");
            bTree.remove(6);
            bTree.traverse();

            int key = 12;
            System.out.println("\nSearching for key " + key);
            Long result = bTree.search(key);
            if (result != null) {
                System.out.println("Key " + key + " found with value: " + result);
            } else {
                System.out.println("Key " + key + " not found in the B-tree.");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
