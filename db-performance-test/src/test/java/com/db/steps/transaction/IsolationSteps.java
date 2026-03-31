package com.db.steps.transaction;

import com.iu.indexes.transaction.*;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Cucumber step definitions for transaction isolation level scenarios.
 *
 * Uses MVCCStore in-process — no live DB server needed.
 * These steps make the isolation anomalies directly observable in BDD output.
 */
public class IsolationSteps {

    private MVCCStore store;
    private TransactionContext txA;
    private TransactionContext txB;
    private TransactionIsolationLevel level;
    private List<VersionedRecord> firstScan;
    private List<VersionedRecord> secondScan;
    private VersionedRecord readResult;

    // ── Setup ────────────────────────────────────────────────────────────

    @Given("a transaction at isolation level READ_UNCOMMITTED")
    public void givenReadUncommitted() { setup(TransactionIsolationLevel.READ_UNCOMMITTED); }

    @Given("a transaction at isolation level READ_COMMITTED")
    public void givenReadCommitted()   { setup(TransactionIsolationLevel.READ_COMMITTED); }

    @Given("a transaction at isolation level REPEATABLE_READ")
    public void givenRepeatableRead()  { setup(TransactionIsolationLevel.REPEATABLE_READ); }

    private void setup(TransactionIsolationLevel lvl) {
        store = new MVCCStore();
        level = lvl;
        txA   = store.beginTransaction(lvl);
    }

    // ── Actions ───────────────────────────────────────────────────────────

    @And("another transaction inserts a document without committing")
    public void anotherTransactionInsertsWithoutCommitting() {
        txB = store.beginTransaction(TransactionIsolationLevel.READ_COMMITTED);
        store.insert(txB, 999, "dirty-value");
        // txB intentionally NOT committed
    }

    @And("the transaction scans documents with id between {int} and {int}")
    public void transactionScansRange(int lo, int hi) {
        // Pre-populate with some committed docs
        TransactionContext setup = store.beginTransaction(TransactionIsolationLevel.READ_COMMITTED);
        store.insert(setup, lo,    "doc-lo");
        store.insert(setup, hi,    "doc-hi");
        store.commit(setup);

        // txA takes its first snapshot scan
        firstScan = store.rangeScan(txA, lo, hi);
    }

    @And("another transaction inserts a document with id {int} and commits")
    public void anotherTransactionInsertsAndCommits(int id) {
        txB = store.beginTransaction(TransactionIsolationLevel.READ_COMMITTED);
        store.insert(txB, id, "phantom-doc");
        store.commit(txB);
    }

    // ── Reads ─────────────────────────────────────────────────────────────

    @When("the first transaction reads that document")
    public void firstTransactionReadsThatDocument() {
        readResult = store.read(txA, 999);
    }

    @When("the first transaction rescans the same range")
    public void firstTransactionRescansRange() {
        // Find the lo/hi bounds from firstScan
        int lo = firstScan.stream().mapToInt(r -> r.docId).min().orElse(10);
        int hi = firstScan.stream().mapToInt(r -> r.docId).max().orElse(20);
        secondScan = store.rangeScan(txA, lo, hi + 10); // extend range to include mid-point
    }

    // ── Assertions ────────────────────────────────────────────────────────

    @Then("the dirty data is visible")
    public void dirtyDataIsVisible() {
        assertNotNull(readResult,
            "READ_UNCOMMITTED must see uncommitted (dirty) writes");
        assertEquals("dirty-value", readResult.value);
    }

    @Then("the uncommitted data is not visible")
    public void uncommittedDataIsNotVisible() {
        assertNull(readResult,
            "READ_COMMITTED must NOT see uncommitted writes (no dirty reads)");
    }

    @Then("the second scan returns the same row count as the first")
    public void secondScanSameCount() {
        assertEquals(firstScan.size(), secondScan.size(),
            "REPEATABLE_READ must prevent phantom reads: both scans return the same count");
    }

    @Then("the second scan returns more rows than the first")
    public void secondScanMoreRows() {
        assertTrue(secondScan.size() > firstScan.size(),
            "READ_COMMITTED exhibits phantom reads: second scan sees newly committed row");
    }
}
