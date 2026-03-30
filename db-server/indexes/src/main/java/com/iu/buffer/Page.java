package com.iu.buffer;

import java.util.Arrays;

/**
 * A fixed-size memory page — the unit of I/O in a real database.
 *
 * WHY PAGES?
 * Real disks read/write in sectors (512B–4KB). Reading a single byte from
 * a file still causes the OS to load a full 4KB page into the page cache.
 * A database buffer pool mirrors this: instead of one seek per record, it
 * loads whole pages and amortises the I/O cost across all records on that page.
 *
 * A B+ Tree node is sized to fit exactly in one page, so one disk read
 * loads one node — the "disk page = tree node" alignment is the reason
 * B+ Trees are the dominant on-disk index structure.
 *
 * Page is a simple value type — a record (Java 16) wrapping an id and
 * a byte array. The buffer pool manages the lifecycle.
 */
public final class Page {

    /** Default page size: 4 KB, matches typical OS page and SSD sector. */
    public static final int PAGE_SIZE = 4096;

    private final int    pageId;
    private final byte[] data;
    private       boolean dirty;   // true if this page has been modified but not yet flushed

    public Page(int pageId) {
        this.pageId = pageId;
        this.data   = new byte[PAGE_SIZE];
        this.dirty  = false;
    }

    public int    pageId()         { return pageId; }
    public byte[] data()           { return data; }
    public boolean isDirty()       { return dirty; }
    public void   markDirty()      { this.dirty = true; }
    public void   clearDirty()     { this.dirty = false; }

    /** Write bytes into the page at the given offset. */
    public void write(int offset, byte[] bytes) {
        if (offset + bytes.length > PAGE_SIZE)
            throw new IllegalArgumentException(
                "Write of " + bytes.length + " bytes at offset " + offset
                + " exceeds page size " + PAGE_SIZE);
        System.arraycopy(bytes, 0, data, offset, bytes.length);
        markDirty();
    }

    /** Read bytes from the page. */
    public byte[] read(int offset, int length) {
        byte[] result = new byte[length];
        System.arraycopy(data, offset, result, 0, length);
        return result;
    }

    @Override public String toString() {
        return "Page{id=" + pageId + ", dirty=" + dirty + "}";
    }
}
