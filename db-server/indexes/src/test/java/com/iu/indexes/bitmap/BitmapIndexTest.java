package com.iu.indexes.bitmap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for BitmapIndex.
 *
 * A Bitmap Index is ideal for low-cardinality columns.  Each distinct value owns
 * a BitSet where bit N is set if document id N has that value.  AND/OR queries
 * across values reduce to bitwise operations, which is O(N/64) using the CPU's
 * native 64-bit word size.
 *
 * Tests cover single-value lookup, AND/OR predicates, deletion, cardinality,
 * and disk round-trip persistence.
 */
class BitmapIndexTest {

    private BitmapIndex index;

    @BeforeEach
    void setUp() {
        index = new BitmapIndex();
    }

    // -----------------------------------------------------------------------
    // Insert & basic search
    // -----------------------------------------------------------------------

    @Test
    void insert_singleEntry_searchFindsIt() {
        index.insert(5, "active");
        List<Integer> result = index.search("active");
        assertEquals(1, result.size());
        assertEquals(5, result.get(0));
    }

    @Test
    void insert_multipleDocsWithSameValue_searchReturnsAll() {
        index.insert(1, "active");
        index.insert(3, "active");
        index.insert(7, "active");
        List<Integer> result = index.search("active");
        assertEquals(3, result.size());
        assertTrue(result.containsAll(List.of(1, 3, 7)));
    }

    @Test
    void insert_differentValues_searchIsolated() {
        index.insert(1, "active");
        index.insert(2, "inactive");
        index.insert(3, "active");

        assertEquals(List.of(2), index.search("inactive"));
        assertEquals(2, index.search("active").size());
    }

    @Test
    void search_unknownValue_returnsEmpty() {
        index.insert(1, "active");
        assertTrue(index.search("deleted").isEmpty());
    }

    @Test
    void search_isCaseInsensitive() {
        index.insert(1, "Active");
        assertFalse(index.search("active").isEmpty());
        assertFalse(index.search("ACTIVE").isEmpty());
    }

    // -----------------------------------------------------------------------
    // AND predicate
    // -----------------------------------------------------------------------

    @Test
    void searchAnd_returnsIntersection() {
        // For multi-column bitmaps: doc must match ALL values
        index.insert(1, "active");
        index.insert(1, "premium");   // same doc, second value
        index.insert(2, "active");    // active but not premium
        index.insert(3, "premium");   // premium but not active

        // Only doc 1 is both active AND premium
        List<Integer> result = index.searchAnd(List.of("active", "premium"));
        assertEquals(List.of(1), result);
    }

    @Test
    void searchAnd_noIntersection_returnsEmpty() {
        index.insert(1, "active");
        index.insert(2, "inactive");
        assertTrue(index.searchAnd(List.of("active", "inactive")).isEmpty());
    }

    // -----------------------------------------------------------------------
    // OR predicate
    // -----------------------------------------------------------------------

    @Test
    void searchOr_returnsUnion() {
        index.insert(1, "active");
        index.insert(2, "inactive");
        index.insert(3, "pending");

        List<Integer> result = index.searchOr(List.of("active", "inactive"));
        assertEquals(2, result.size());
        assertTrue(result.containsAll(List.of(1, 2)));
    }

    @Test
    void searchOr_allUnknown_returnsEmpty() {
        assertTrue(index.searchOr(List.of("x", "y")).isEmpty());
    }

    // -----------------------------------------------------------------------
    // Deletion
    // -----------------------------------------------------------------------

    @Test
    void delete_removesDocFromBitmap() {
        index.insert(5, "active");
        index.insert(6, "active");

        index.delete(5);

        List<Integer> result = index.search("active");
        assertFalse(result.contains(5));
        assertTrue(result.contains(6));
    }

    @Test
    void delete_lastDocForValue_removesValue() {
        index.insert(5, "rare");
        index.delete(5);
        assertTrue(index.search("rare").isEmpty());
        assertFalse(index.distinctValues().contains("rare"));
    }

    @Test
    void delete_unknownId_noException() {
        // Deleting a non-existent id should not throw
        assertDoesNotThrow(() -> index.delete(999));
    }

    // -----------------------------------------------------------------------
    // Cardinality & distinct values
    // -----------------------------------------------------------------------

    @Test
    void cardinality_countsDocsForValue() {
        index.insert(1, "active");
        index.insert(2, "active");
        index.insert(3, "inactive");
        assertEquals(2, index.cardinality("active"));
        assertEquals(1, index.cardinality("inactive"));
        assertEquals(0, index.cardinality("unknown"));
    }

    @Test
    void distinctValues_returnsAllIndexedValues() {
        index.insert(1, "active");
        index.insert(2, "inactive");
        index.insert(3, "pending");
        assertEquals(3, index.distinctValues().size());
        assertTrue(index.distinctValues().containsAll(List.of("active", "inactive", "pending")));
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    @Test
    void saveToDiskAndLoad_roundTrip(@TempDir Path tmpDir) throws IOException {
        index.insert(1, "active");
        index.insert(2, "active");
        index.insert(3, "inactive");

        String filePath = tmpDir.resolve("bitmap.dat").toString();
        index.saveToDisk(filePath);

        BitmapIndex loaded = new BitmapIndex();
        loaded.loadFromDisk(filePath);

        List<Integer> active = loaded.search("active");
        assertEquals(2, active.size());
        assertTrue(active.containsAll(List.of(1, 2)));
        assertEquals(List.of(3), loaded.search("inactive"));
    }

    @Test
    void loadFromDisk_missingFile_noException(@TempDir Path tmpDir) throws IOException {
        assertDoesNotThrow(() ->
                new BitmapIndex().loadFromDisk(tmpDir.resolve("missing.dat").toString()));
    }
}
