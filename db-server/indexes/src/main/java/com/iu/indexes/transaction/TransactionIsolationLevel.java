package com.iu.indexes.transaction;

/**
 * Standard SQL transaction isolation levels.
 *
 * Demonstrates the classic anomalies that each level prevents or allows,
 * especially in the context of index reads.
 *
 * | Level              | Dirty Read | Non-repeatable Read | Phantom Read |
 * |--------------------|:----------:|:-------------------:|:------------:|
 * | READ_UNCOMMITTED   |   yes      |       yes           |     yes      |
 * | READ_COMMITTED     |   no       |       yes           |     yes      |
 * | REPEATABLE_READ    |   no       |       no            |     yes (*)  |
 * | SERIALIZABLE       |   no       |       no            |     no       |
 *
 * (*) Phantom reads are especially relevant for index range scans: a second
 *     scan on [lo, hi] can return rows inserted by another concurrent
 *     transaction that committed between the two scans.
 */
public enum TransactionIsolationLevel {
    READ_UNCOMMITTED,
    READ_COMMITTED,
    REPEATABLE_READ,
    SERIALIZABLE
}
