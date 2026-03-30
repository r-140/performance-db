package com.iu.indexes.gin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for GINIndex.
 *
 * GIN (Generalized Inverted Index) maps each token extracted from a document
 * value to the list of file offsets where documents containing that token live.
 * These tests verify insertion, exact-token search, boolean AND/OR search,
 * deletion, and disk round-trip persistence.
 */
class GINIndexTest {

    private GINIndex index;

    @BeforeEach
    void setUp() {
        index = new GINIndex();
    }

    // -----------------------------------------------------------------------
    // Tokenization
    // -----------------------------------------------------------------------

    @Test
    void tokenize_stripsJsonNoise() {
        List<String> tokens = GINIndex.tokenize("{\"data\":\"testdata42\",\"id\":42}");
        assertTrue(tokens.contains("data"));
        assertTrue(tokens.contains("testdata42"));
        assertTrue(tokens.contains("id"));
        assertTrue(tokens.contains("42"));
    }

    @Test
    void tokenize_lowercasesAll() {
        List<String> tokens = GINIndex.tokenize("Hello World UPPER");
        assertTrue(tokens.contains("hello"));
        assertTrue(tokens.contains("world"));
        assertTrue(tokens.contains("upper"));
    }

    @Test
    void tokenize_emptyStringReturnsEmpty() {
        assertTrue(GINIndex.tokenize("").isEmpty());
        assertTrue(GINIndex.tokenize(null).isEmpty());
        assertTrue(GINIndex.tokenize("   ").isEmpty());
    }

    // -----------------------------------------------------------------------
    // Insert & search
    // -----------------------------------------------------------------------

    @Test
    void insert_singleDoc_searchFindsOffset() {
        index.insert("{\"data\":\"hello\",\"id\":1}", 0L);
        List<Long> result = index.search("hello");
        assertEquals(1, result.size());
        assertEquals(0L, result.get(0));
    }

    @Test
    void insert_multipleDocsWithSameToken_searchReturnsAll() {
        index.insert("hello world", 10L);
        index.insert("hello java",  20L);
        List<Long> result = index.search("hello");
        assertEquals(2, result.size());
        assertTrue(result.contains(10L));
        assertTrue(result.contains(20L));
    }

    @Test
    void search_unknownToken_returnsEmpty() {
        index.insert("hello", 5L);
        assertTrue(index.search("nothere").isEmpty());
    }

    // -----------------------------------------------------------------------
    // Boolean AND
    // -----------------------------------------------------------------------

    @Test
    void searchAll_returnsIntersection() {
        index.insert("hello world",  10L);
        index.insert("hello java",   20L);
        index.insert("world goodbye",30L);

        // "hello" AND "world" → only offset 10
        List<Long> result = index.searchAll(List.of("hello", "world"));
        assertEquals(1, result.size());
        assertTrue(result.contains(10L));
    }

    @Test
    void searchAll_noMatch_returnsEmpty() {
        index.insert("hello world", 10L);
        assertTrue(index.searchAll(List.of("hello", "nothere")).isEmpty());
    }

    @Test
    void searchAll_emptyTokenList_returnsEmpty() {
        index.insert("hello", 5L);
        assertTrue(index.searchAll(List.of()).isEmpty());
    }

    // -----------------------------------------------------------------------
    // Boolean OR
    // -----------------------------------------------------------------------

    @Test
    void searchAny_returnsUnion() {
        index.insert("hello world",  10L);
        index.insert("java spring",  20L);
        index.insert("world peace",  30L);

        List<Long> result = index.searchAny(List.of("hello", "java"));
        assertEquals(2, result.size());
        assertTrue(result.contains(10L));
        assertTrue(result.contains(20L));
    }

    // -----------------------------------------------------------------------
    // Deletion
    // -----------------------------------------------------------------------

    @Test
    void delete_removesOffsetFromAllPostingLists() {
        index.insert("hello world", 10L);
        index.insert("hello again", 20L);

        index.delete(10L);

        // offset 10 gone from "hello" and "world"
        assertFalse(index.search("hello").contains(10L));
        assertFalse(index.search("world").contains(10L));
        // offset 20 still present
        assertTrue(index.search("hello").contains(20L));
    }

    @Test
    void delete_emptyPostingListsAreRemoved() {
        index.insert("unique token", 5L);
        index.delete(5L);
        // "unique" and "token" lists are now empty and should be pruned
        assertTrue(index.search("unique").isEmpty());
        assertEquals(0, index.tokenCount());
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    @Test
    void saveToDiskAndLoad_roundTrip(@TempDir Path tmpDir) throws IOException {
        index.insert("hello world", 10L);
        index.insert("hello java",  20L);

        String filePath = tmpDir.resolve("gin.dat").toString();
        index.saveToDisk(filePath);

        GINIndex loaded = new GINIndex();
        loaded.loadFromDisk(filePath);

        List<Long> result = loaded.search("hello");
        assertEquals(2, result.size());
        assertTrue(result.contains(10L));
        assertTrue(result.contains(20L));
    }

    @Test
    void loadFromDisk_nonExistentFile_noException(@TempDir Path tmpDir) throws IOException {
        // Should silently return without throwing
        GINIndex empty = new GINIndex();
        empty.loadFromDisk(tmpDir.resolve("missing.dat").toString());
        assertEquals(0, empty.tokenCount());
    }

    // -----------------------------------------------------------------------
    // Token count
    // -----------------------------------------------------------------------

    @Test
    void tokenCount_reflectsDistinctTokens() {
        index.insert("a b c", 0L);
        index.insert("a d",   1L);
        // distinct tokens: a, b, c, d
        assertEquals(4, index.tokenCount());
    }
}
