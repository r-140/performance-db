package com.iu.indexes.skiplist;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

/**
 * Skip List probabilistic index.
 *
 * HOW A SKIP LIST WORKS
 * =====================
 * A skip list is a layered set of sorted linked lists. The bottom layer
 * (level 0) contains every element. Each higher level contains a random
 * subset of the elements from the level below — roughly half, chosen by
 * a coin flip during insertion.
 *
 * Searching starts at the topmost level (fewest elements) and works down:
 *
 *   Level 2:  1 ---------> 50 ----------> null
 *   Level 1:  1 ---> 20 -> 50 -> 70 ----> null
 *   Level 0:  1 -> 10 -> 20 -> 30 -> 50 -> 70 -> null
 *
 *   search(30): start level 2: 1→50 (too big, drop down)
 *               level 1: 1→20→50 (too big, drop down)
 *               level 0: 20→30 FOUND
 *   = O(log N) expected hops, same as B-Tree
 *
 * WHY SKIP LISTS ARE INTERESTING
 * - No rotations (unlike AVL/Red-Black trees). Insertion and deletion
 *   are simpler to implement correctly.
 * - Lock-free concurrent variants exist (ConcurrentSkipListMap in JDK).
 * - Used by Redis Sorted Sets, LevelDB memtable, Cassandra memtable.
 * - Expected O(log N) search/insert/delete with high probability.
 *
 * COMPARISON TO B+ TREE
 * - Similar O(log N) search performance.
 * - B+ Tree uses less memory (skip list stores many pointers per node).
 * - B+ Tree is better for disk-based indexes (pages align to disk sectors).
 * - Skip list is better for in-memory indexes where simplicity matters.
 *
 * This implementation is in-memory (educational focus). For disk persistence
 * see the B+ Tree implementation.
 */
public class SkipListIndex {
    private static final Logger LOGGER = Logger.getLogger(SkipListIndex.class.getName());

    static final int MAX_LEVEL = 16;   // max levels (supports ~65536 elements efficiently)
    static final double PROBABILITY = 0.5; // coin-flip probability for level promotion

    private final SkipListNode head;    // sentinel head node (key = MIN_VALUE)
    private int currentLevel = 0;       // highest currently used level (0-indexed)
    private int size = 0;

    public SkipListIndex() {
        this.head = new SkipListNode(Integer.MIN_VALUE, null, MAX_LEVEL);
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Insert key → offset into the skip list.
     * Expected time: O(log N)
     */
    public void insert(int key, Long offset) {
        // Find update[] — predecessor nodes at each level that need pointer updates
        @SuppressWarnings("unchecked")
        SkipListNode[] update = new SkipListNode[MAX_LEVEL + 1];
        SkipListNode current = head;

        for (int i = currentLevel; i >= 0; i--) {
            while (current.forward[i] != null && current.forward[i].key < key) {
                current = current.forward[i];
            }
            update[i] = current;
        }

        current = current.forward[0];

        // If key already exists, update its value
        if (current != null && current.key == key) {
            current.value = offset;
            return;
        }

        // New key — pick a random level
        int newLevel = randomLevel();
        if (newLevel > currentLevel) {
            for (int i = currentLevel + 1; i <= newLevel; i++) update[i] = head;
            currentLevel = newLevel;
        }

        // Insert new node and update forward pointers
        SkipListNode newNode = new SkipListNode(key, offset, newLevel);
        for (int i = 0; i <= newLevel; i++) {
            newNode.forward[i]  = update[i].forward[i];
            update[i].forward[i] = newNode;
        }
        size++;
    }

    /**
     * Search for an exact key.
     * Expected time: O(log N)
     */
    public Long search(int key) {
        SkipListNode current = head;
        for (int i = currentLevel; i >= 0; i--) {
            while (current.forward[i] != null && current.forward[i].key < key) {
                current = current.forward[i];
            }
        }
        current = current.forward[0];
        return (current != null && current.key == key) ? current.value : null;
    }

    /**
     * Range scan [lo, hi] — walk the level-0 linked list.
     * Time: O(log N + k) where k = result count.
     * This is the same complexity as B+ Tree range scan via leaf list.
     */
    public List<Long> rangeScan(int lo, int hi) {
        // Find the start node in O(log N)
        SkipListNode current = head;
        for (int i = currentLevel; i >= 0; i--) {
            while (current.forward[i] != null && current.forward[i].key < lo) {
                current = current.forward[i];
            }
        }
        current = current.forward[0];

        // Walk level-0 list collecting results in O(k)
        List<Long> results = new ArrayList<>();
        while (current != null && current.key <= hi) {
            results.add(current.value);
            current = current.forward[0];
        }
        return results;
    }

    /**
     * Delete a key.
     * Expected time: O(log N)
     */
    public void delete(int key) {
        @SuppressWarnings("unchecked")
        SkipListNode[] update = new SkipListNode[MAX_LEVEL + 1];
        SkipListNode current = head;

        for (int i = currentLevel; i >= 0; i--) {
            while (current.forward[i] != null && current.forward[i].key < key) {
                current = current.forward[i];
            }
            update[i] = current;
        }

        current = current.forward[0];
        if (current == null || current.key != key) return; // not found

        for (int i = 0; i <= currentLevel; i++) {
            if (update[i].forward[i] != current) break;
            update[i].forward[i] = current.forward[i];
        }

        // Shrink current level if needed
        while (currentLevel > 0 && head.forward[currentLevel] == null) {
            currentLevel--;
        }
        size--;
    }

    public int size()         { return size; }
    public int currentLevel() { return currentLevel; }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    private int randomLevel() {
        int level = 0;
        while (level < MAX_LEVEL && ThreadLocalRandom.current().nextDouble() < PROBABILITY) {
            level++;
        }
        return level;
    }
}
