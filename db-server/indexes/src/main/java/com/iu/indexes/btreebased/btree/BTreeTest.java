package com.iu.indexes.btreebased.btree;

import java.io.IOException;

public class BTreeTest {
    public static void main(String[] args) {
        try {
            BTreeIndex bTree = new BTreeIndex("btree.dat", 3);

            bTree.insert(10, 1l);
            bTree.insert(20, 2l);
            bTree.insert(5, 5l);
            bTree.insert(6, 6l);
            bTree.insert(12, 7l);
            bTree.insert(30, 8l);
            bTree.insert(7, 9l);
            bTree.insert(17, 17l);

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
