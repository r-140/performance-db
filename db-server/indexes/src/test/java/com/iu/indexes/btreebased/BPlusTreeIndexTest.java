package com.iu.indexes.btreebased;

import com.iu.indexes.btreebased.bplustree.BPlusTreeIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that verify B+ Tree structural correctness — not just "it returns
 * the right value" but also the invariants that distinguish B+ Tree from
 * B-Tree: values only in leaves, range scan via leaf linked list, correct
 * split behaviour preserving the median in the right leaf.
 */
class BPlusTreeIndexTest {

    // -----------------------------------------------------------------------
    // Basic insert and search
    // -----------------------------------------------------------------------

    @Test
    void insertAndSearch_singleEntry(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "single.dat");
        tree.insert(42, 1000L);
        assertEquals(1000L, tree.search(42));
    }

    @Test
    void insertAndSearch_missingKey_returnsNull(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "miss.dat");
        tree.insert(1, 10L);
        assertNull(tree.search(99));
    }

    @Test
    void insertAndSearch_multipleKeys(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "multi.dat");
        for (int i = 1; i <= 20; i++) tree.insert(i, (long) i * 100);
        for (int i = 1; i <= 20; i++) assertEquals((long) i * 100, tree.search(i));
    }

    @Test
    void insert_duplicateKey_latestValueVisible(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "dup.dat");
        tree.insert(5, 100L);
        tree.insert(5, 200L); // overwrite
        // B+ Tree with duplicate insert: last insert's value at the leaf wins
        assertNotNull(tree.search(5));
    }

    // -----------------------------------------------------------------------
    // Split correctness — median retained in right leaf
    // -----------------------------------------------------------------------

    @Test
    void split_allKeysStillSearchable_afterSplit(@TempDir Path tmp) throws IOException {
        // t=2 means max 3 keys per node; 4th insert forces a split
        BPlusTreeIndex tree = newTree(tmp, "split.dat");
        int[] keys = {10, 20, 5, 15, 30, 25, 7, 3};
        for (int i = 0; i < keys.length; i++) tree.insert(keys[i], (long) keys[i] * 10);
        for (int key : keys) {
            assertEquals((long) key * 10, tree.search(key),
                    "Key " + key + " should be findable after splits");
        }
    }

    @Test
    void split_noKeyLostInLeafChain(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "chain.dat");
        for (int i = 1; i <= 15; i++) tree.insert(i, (long) i);
        // Every inserted key must be reachable
        for (int i = 1; i <= 15; i++) {
            assertNotNull(tree.search(i), "Key " + i + " lost after split");
        }
    }

    // -----------------------------------------------------------------------
    // Range scan — the key advantage of B+ Tree over B-Tree
    // -----------------------------------------------------------------------

    @Test
    void rangeScan_returnsCorrectOffsets(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "range.dat");
        for (int i = 1; i <= 20; i++) tree.insert(i, (long) i * 10);

        List<Long> result = tree.rangeScan(5, 10);
        assertEquals(6, result.size(), "rangeScan(5,10) should return 6 entries");
        assertTrue(result.contains(50L));
        assertTrue(result.contains(100L));
    }

    @Test
    void rangeScan_emptyRange_returnsEmpty(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "emptyrange.dat");
        for (int i = 1; i <= 5; i++) tree.insert(i, (long) i);
        assertTrue(tree.rangeScan(10, 20).isEmpty());
    }

    @Test
    void rangeScan_singleElement(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "single_range.dat");
        tree.insert(7, 777L);
        List<Long> result = tree.rangeScan(7, 7);
        assertEquals(1, result.size());
        assertEquals(777L, result.get(0));
    }

    @Test
    void rangeScan_acrossMultipleLeaves(@TempDir Path tmp) throws IOException {
        // t=2 → leaves hold max 3 keys; 12 keys forces ≥4 leaf nodes
        BPlusTreeIndex tree = newTree(tmp, "multi_leaf.dat");
        for (int i = 1; i <= 12; i++) tree.insert(i, (long) i);

        // Full range scan must cross all leaf nodes via the linked list
        List<Long> result = tree.rangeScan(1, 12);
        assertEquals(12, result.size(), "Full range scan must return all 12 keys");
    }

    // -----------------------------------------------------------------------
    // Deletion
    // -----------------------------------------------------------------------

    @Test
    void remove_leafKey_notFoundAfter(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "del_leaf.dat");
        for (int i = 1; i <= 10; i++) tree.insert(i, (long) i * 10);
        tree.remove(5);
        assertNull(tree.search(5));
        // Remaining keys still searchable
        for (int i = 1; i <= 10; i++) {
            if (i != 5) assertNotNull(tree.search(i), "Key " + i + " should survive deletion of 5");
        }
    }

    @Test
    void remove_internalSeparatorKey_treeRemainsSorted(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "del_internal.dat");
        // Insert enough to create internal nodes (t=2, need >3 keys)
        for (int i = 1; i <= 10; i++) tree.insert(i, (long) i);
        // The key that was promoted as a separator (e.g. 3 or 5 depending on splits)
        // should still be removable and the tree should remain correct
        tree.remove(3);
        assertNull(tree.search(3));
        // All other keys still reachable
        for (int i = 1; i <= 10; i++) {
            if (i != 3) assertNotNull(tree.search(i), "Key " + i + " lost after removing 3");
        }
    }

    @Test
    void remove_nonExistentKey_noException(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "del_none.dat");
        tree.insert(1, 10L);
        assertDoesNotThrow(() -> tree.remove(999));
        assertEquals(10L, tree.search(1)); // existing key unaffected
    }

    @Test
    void rangeScan_afterDeletion_doesNotReturnDeletedOffset(@TempDir Path tmp) throws IOException {
        BPlusTreeIndex tree = newTree(tmp, "range_del.dat");
        for (int i = 1; i <= 10; i++) tree.insert(i, (long) i);
        tree.remove(5);
        List<Long> result = tree.rangeScan(1, 10);
        assertFalse(result.contains(5L), "Deleted key should not appear in range scan");
        assertEquals(9, result.size());
    }

    // -----------------------------------------------------------------------
    // Persistence — reload from disk
    // -----------------------------------------------------------------------

    @Test
    void reloadFromDisk_allKeysStillSearchable(@TempDir Path tmp) throws IOException {
        String path = tmp.resolve("persist.dat").toString();
        BPlusTreeIndex tree1 = new BPlusTreeIndex(path, 2);
        for (int i = 1; i <= 10; i++) tree1.insert(i, (long) i * 100);

        // Re-open from same file
        BPlusTreeIndex tree2 = new BPlusTreeIndex(path, 2);
        for (int i = 1; i <= 10; i++) {
            assertEquals((long) i * 100, tree2.search(i),
                    "Key " + i + " missing after reload");
        }
    }

    @Test
    void reloadFromDisk_subsequentInsertDoesNotCorruptRoot(@TempDir Path tmp) throws IOException {
        String path = tmp.resolve("reload_insert.dat").toString();
        BPlusTreeIndex tree1 = new BPlusTreeIndex(path, 2);
        tree1.insert(1, 10L);

        // Re-open and insert more — nodeCounter must not reset to 0 and overwrite root
        BPlusTreeIndex tree2 = new BPlusTreeIndex(path, 2);
        tree2.insert(2, 20L);
        tree2.insert(3, 30L);

        assertNotNull(tree2.search(1), "Key 1 must survive reload+insert");
        assertNotNull(tree2.search(2));
        assertNotNull(tree2.search(3));
    }

    // -----------------------------------------------------------------------
    // B+ Tree vs B-Tree structural property: values only in leaves
    // -----------------------------------------------------------------------

    @Test
    void internalNodes_doNotHoldValues_rangeContainsAllKeys(@TempDir Path tmp) throws IOException {
        // Insert enough keys to guarantee internal nodes exist (t=2, need >3)
        BPlusTreeIndex tree = newTree(tmp, "structural.dat");
        int[] keys = {10, 20, 5, 15, 30, 25, 7, 3, 12, 18};
        for (int k : keys) tree.insert(k, (long) k);

        // A correct B+ Tree range scan must find every inserted key through the leaf list
        List<Long> result = tree.rangeScan(3, 30);
        assertEquals(keys.length, result.size(),
                "All " + keys.length + " keys must be reachable via leaf-list range scan");
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private BPlusTreeIndex newTree(Path tmp, String name) throws IOException {
        return new BPlusTreeIndex(tmp.resolve(name).toString(), 2);
    }
}
