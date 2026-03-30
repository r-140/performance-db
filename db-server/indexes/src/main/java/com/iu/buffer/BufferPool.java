package com.iu.buffer;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Fixed-size in-memory buffer pool with LRU eviction.
 *
 * HOW A BUFFER POOL WORKS
 * =======================
 * A database never reads individual bytes from disk. It reads fixed-size
 * PAGES (typically 4 KB or 8 KB). The buffer pool is a cache of pages in
 * memory. When a page is needed:
 *
 *   1. If it's in the pool (cache hit)  → return it directly. Zero disk I/O.
 *   2. If it's not (cache miss)         → read from disk, store in pool, return.
 *      If the pool is full              → evict the LRU (least-recently-used) page
 *                                         first (writing it back to disk if dirty).
 *
 * This is why a warm database with a large buffer pool outperforms one
 * with a small pool: frequently-accessed B+ Tree internal nodes stay in
 * memory and cost zero disk I/O, even though the tree is O(log N) deep.
 *
 * Dirty pages (modified but not yet written to disk) must be flushed
 * before eviction — this is where WAL interacts with the buffer pool:
 * the WAL must be flushed first (write-ahead rule), then the dirty page
 * can be evicted safely.
 *
 * Implementation: LinkedHashMap with access-order=true gives O(1) LRU
 * eviction. A ReentrantReadWriteLock protects concurrent access.
 */
public class BufferPool implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(BufferPool.class.getName());

    private final int      capacity;
    private final String   dataDir;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Access-order LinkedHashMap: eldest (LRU) entry evicted when full. */
    private final LinkedHashMap<Integer, Page> pool;

    // Statistics (Java 21 record for snapshot)
    private long hits   = 0;
    private long misses = 0;

    public record Stats(long hits, long misses, int poolSize, int capacity) {
        public double hitRate() {
            long total = hits + misses;
            return total == 0 ? 0.0 : (double) hits / total;
        }
    }

    public BufferPool(int capacity, String dataDir) {
        this.capacity = capacity;
        this.dataDir  = dataDir;
        this.pool     = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, Page> eldest) {
                if (size() > capacity) {
                    try { evict(eldest.getValue()); } catch (IOException e) {
                        LOGGER.log(Level.WARNING, "Eviction failed for page " + eldest.getKey(), e);
                    }
                    return true;
                }
                return false;
            }
        };
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Fetch a page from the pool (cache hit) or disk (cache miss).
     * On miss: reads from disk and inserts into pool, potentially evicting LRU.
     */
    public Page fetchPage(int pageId) throws IOException {
        var readLock = lock.readLock();
        readLock.lock();
        try {
            Page cached = pool.get(pageId);
            if (cached != null) {
                hits++;
                LOGGER.log(Level.FINEST, "Buffer hit  page=" + pageId);
                return cached;
            }
        } finally {
            readLock.unlock();
        }

        // Cache miss — upgrade to write lock and load from disk
        var writeLock = lock.writeLock();
        writeLock.lock();
        try {
            // Double-check after acquiring write lock
            Page cached = pool.get(pageId);
            if (cached != null) { hits++; return cached; }

            misses++;
            LOGGER.log(Level.FINE, "Buffer miss page=" + pageId + " — loading from disk");
            Page page = loadFromDisk(pageId);
            pool.put(pageId, page);
            return page;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Mark a page as dirty (it was modified).
     * The page will be written back to disk when evicted or at checkpoint.
     */
    public void markDirty(int pageId) {
        lock.readLock().lock();
        try {
            Page p = pool.get(pageId);
            if (p != null) p.markDirty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Flush all dirty pages to disk (checkpoint).
     * Called periodically to keep WAL size manageable.
     */
    public void flushAll() throws IOException {
        lock.writeLock().lock();
        try {
            for (Page page : pool.values()) {
                if (page.isDirty()) {
                    writeToDisk(page);
                    page.clearDirty();
                }
            }
            LOGGER.info("Buffer pool checkpoint: all dirty pages flushed");
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Stats stats() {
        lock.readLock().lock();
        try { return new Stats(hits, misses, pool.size(), capacity); }
        finally { lock.readLock().unlock(); }
    }

    @Override
    public void close() throws IOException {
        flushAll();
    }

    // -----------------------------------------------------------------------
    // Disk I/O
    // -----------------------------------------------------------------------

    private Page loadFromDisk(int pageId) throws IOException {
        Path path = pagePath(pageId);
        Page page = new Page(pageId);
        if (Files.exists(path)) {
            byte[] bytes = Files.readAllBytes(path);
            page.write(0, Arrays.copyOf(bytes, Math.min(bytes.length, Page.PAGE_SIZE)));
            page.clearDirty();
        }
        return page;
    }

    private void writeToDisk(Page page) throws IOException {
        Path path = pagePath(page.pageId());
        Files.createDirectories(path.getParent());
        Files.write(path, page.data());
        LOGGER.log(Level.FINE, "Flushed page " + page.pageId() + " to disk");
    }

    private void evict(Page page) throws IOException {
        if (page.isDirty()) {
            writeToDisk(page);
            LOGGER.log(Level.FINE, "Evicted dirty page " + page.pageId());
        } else {
            LOGGER.log(Level.FINEST, "Evicted clean page " + page.pageId());
        }
    }

    private Path pagePath(int pageId) {
        return Paths.get(dataDir, "pages", "page_" + pageId + ".bin");
    }
}
