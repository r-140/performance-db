package com.iu.buffer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the buffer pool — verifying the caching, LRU eviction,
 * dirty-page tracking, and hit-rate statistics.
 */
class BufferPoolTest {

    // -----------------------------------------------------------------------
    // Basic fetch and cache hit
    // -----------------------------------------------------------------------

    @Test
    void fetchPage_newPage_returnsEmptyPage(@TempDir Path tmp) throws IOException {
        try (BufferPool pool = new BufferPool(10, tmp.toString())) {
            Page p = pool.fetchPage(0);
            assertNotNull(p);
            assertEquals(0, p.pageId());
            assertEquals(Page.PAGE_SIZE, p.data().length);
        }
    }

    @Test
    void fetchPage_twice_returnsFromCache(@TempDir Path tmp) throws IOException {
        try (BufferPool pool = new BufferPool(10, tmp.toString())) {
            Page first  = pool.fetchPage(1);
            Page second = pool.fetchPage(1);
            assertSame(first, second, "Second fetch must return the same cached object");
        }
    }

    @Test
    void stats_hitRate_increasesOnCacheHit(@TempDir Path tmp) throws IOException {
        try (BufferPool pool = new BufferPool(10, tmp.toString())) {
            pool.fetchPage(0);             // miss
            pool.fetchPage(0);             // hit
            pool.fetchPage(0);             // hit

            BufferPool.Stats s = pool.stats();
            assertEquals(1, s.misses());
            assertEquals(2, s.hits());
            assertEquals(2.0 / 3, s.hitRate(), 0.01);
        }
    }

    // -----------------------------------------------------------------------
    // Dirty tracking
    // -----------------------------------------------------------------------

    @Test
    void markDirty_pageIsDirtyUntilFlushed(@TempDir Path tmp) throws IOException {
        try (BufferPool pool = new BufferPool(10, tmp.toString())) {
            pool.fetchPage(2);
            pool.markDirty(2);
            assertTrue(pool.fetchPage(2).isDirty());

            pool.flushAll();
            assertFalse(pool.fetchPage(2).isDirty(), "Page must be clean after flushAll");
        }
    }

    // -----------------------------------------------------------------------
    // LRU eviction
    // -----------------------------------------------------------------------

    @Test
    void lruEviction_poolFull_eldestPageEvicted(@TempDir Path tmp) throws IOException {
        // Pool capacity = 3; fetch 4 pages → first page evicted
        int capacity = 3;
        try (BufferPool pool = new BufferPool(capacity, tmp.toString())) {
            pool.fetchPage(0);
            pool.fetchPage(1);
            pool.fetchPage(2);
            // Pool is full; fetching page 3 evicts page 0 (LRU)
            pool.fetchPage(3);

            BufferPool.Stats s = pool.stats();
            assertEquals(capacity, s.poolSize(),
                "Pool size must not exceed capacity after eviction");
        }
    }

    @Test
    void lruEviction_accessedPage_notEvictedFirst(@TempDir Path tmp) throws IOException {
        // Page 0 accessed again after 1 and 2 → 1 is LRU and evicted first
        try (BufferPool pool = new BufferPool(3, tmp.toString())) {
            pool.fetchPage(0);
            pool.fetchPage(1);
            pool.fetchPage(2);
            pool.fetchPage(0); // refresh page 0 — now page 1 is LRU
            pool.fetchPage(3); // evicts page 1

            // Page 0 should still be in pool (it was re-accessed)
            BufferPool.Stats before = pool.stats();
            pool.fetchPage(0); // should be a hit
            BufferPool.Stats after = pool.stats();
            assertEquals(before.hits() + 1, after.hits(), "Page 0 should still be cached");
        }
    }

    // -----------------------------------------------------------------------
    // Persistence round-trip
    // -----------------------------------------------------------------------

    @Test
    void flushAndReload_pageDataPersists(@TempDir Path tmp) throws IOException {
        byte[] written = new byte[16];
        for (int i = 0; i < 16; i++) written[i] = (byte) (i + 1);

        try (BufferPool pool = new BufferPool(10, tmp.toString())) {
            Page p = pool.fetchPage(5);
            p.write(0, written);
            pool.markDirty(5);
        } // close flushes dirty pages

        try (BufferPool pool2 = new BufferPool(10, tmp.toString())) {
            Page p2 = pool2.fetchPage(5);
            byte[] loaded = p2.read(0, 16);
            assertArrayEquals(written, loaded, "Page data must survive flush+reload");
        }
    }
}
