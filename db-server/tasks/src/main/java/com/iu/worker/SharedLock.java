package com.iu.worker;

import java.util.concurrent.locks.StampedLock;

/**
 * Single shared StampedLock for all DB tasks.
 *
 * All write tasks (Append, Delete, CreateIndex, DeleteIndex, DeleteDB)
 * acquire a write stamp; read tasks (Find, SQL) acquire a read stamp.
 * This allows concurrent reads while serialising writes.
 *
 * Previously the lock was a static field on FindDocumentTask which all
 * other tasks imported via `static import` — confusing and fragile.
 * Having it here makes the dependency explicit.
 */
public final class SharedLock {
    private SharedLock() {}
    public static final StampedLock lock = new StampedLock();
}
