package com.iu.indexes.skiplist;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SkipListIndexTest {

    private SkipListIndex index;

    @BeforeEach void setUp() { index = new SkipListIndex(); }

    @Test void insertAndSearch_found()       { index.insert(5, 50L); assertEquals(50L, index.search(5)); }
    @Test void search_missing_returnsNull()  { assertNull(index.search(99)); }
    @Test void insert_duplicate_updatesValue() {
        index.insert(5, 50L);
        index.insert(5, 99L);
        assertEquals(99L, index.search(5));
    }

    @Test void insert_many_allSearchable() {
        for (int i = 1; i <= 100; i++) index.insert(i, (long) i * 10);
        for (int i = 1; i <= 100; i++) assertEquals((long) i * 10, index.search(i));
    }

    @Test void delete_removesKey() {
        index.insert(7, 70L);
        index.delete(7);
        assertNull(index.search(7));
    }

    @Test void delete_nonExistent_noException() {
        assertDoesNotThrow(() -> index.delete(999));
    }

    @Test void delete_otherKeysUnaffected() {
        index.insert(1, 10L); index.insert(2, 20L); index.insert(3, 30L);
        index.delete(2);
        assertEquals(10L, index.search(1));
        assertNull(index.search(2));
        assertEquals(30L, index.search(3));
    }

    @Test void rangeScan_returnsCorrectRange() {
        for (int i = 1; i <= 20; i++) index.insert(i, (long) i);
        List<Long> result = index.rangeScan(5, 10);
        assertEquals(6, result.size());
        assertTrue(result.containsAll(List.of(5L, 6L, 7L, 8L, 9L, 10L)));
    }

    @Test void rangeScan_emptyRange_returnsEmpty() {
        index.insert(1, 1L); index.insert(2, 2L);
        assertTrue(index.rangeScan(10, 20).isEmpty());
    }

    @Test void rangeScan_afterDelete_deletedAbsent() {
        for (int i = 1; i <= 10; i++) index.insert(i, (long) i);
        index.delete(5);
        List<Long> result = index.rangeScan(1, 10);
        assertFalse(result.contains(5L));
        assertEquals(9, result.size());
    }

    @Test void size_tracksInsertAndDelete() {
        index.insert(1, 1L); index.insert(2, 2L); index.insert(3, 3L);
        assertEquals(3, index.size());
        index.delete(2);
        assertEquals(2, index.size());
    }
}
