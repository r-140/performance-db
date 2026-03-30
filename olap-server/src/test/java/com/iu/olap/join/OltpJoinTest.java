package com.iu.olap.join;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for OLTP join strategies demonstrating the difference from OLAP joins.
 *
 * Key insight: OLTP joins work on SMALL result sets fetched via index.
 * The typical flow:
 *   index.get(42) → 1 row → join → 1 index probe on the other table
 * Total: 2 lookups. Join algorithm almost irrelevant at this scale.
 *
 * The algorithm matters when N > ~100, which in OLTP usually means
 * "you're running a report query, move it to the data warehouse".
 */
class OltpJoinTest {

    private OltpJoin join;
    private List<Map<String, Object>> orders;
    private List<Map<String, Object>> customers;

    @BeforeEach
    void setUp() {
        join = new OltpJoin();

        customers = List.of(
            row("customer_id", 1, "name", "Alice",   "region", "EU"),
            row("customer_id", 2, "name", "Bob",     "region", "US"),
            row("customer_id", 3, "name", "Carol",   "region", "EU"),
            row("customer_id", 4, "name", "Dave",    "region", "APAC")
        );

        orders = List.of(
            row("order_id", 101, "customer_id", 1, "amount", 500),
            row("order_id", 102, "customer_id", 2, "amount", 300),
            row("order_id", 103, "customer_id", 1, "amount", 200),
            row("order_id", 104, "customer_id", 3, "amount", 750)
        );
    }

    // -----------------------------------------------------------------------
    // Nested-Loop Join
    // -----------------------------------------------------------------------

    @Test
    void nestedLoopJoin_allOrdersMatched() {
        List<Map<String, Object>> result = join.nestedLoopJoin(
            orders, "customer_id",
            customers, "customer_id",
            List.of("order_id", "name", "amount"));

        assertEquals(4, result.size());
    }

    @Test
    void nestedLoopJoin_singleRowOuter_typicalOltpCase() {
        // Typical OLTP: outer = 1 row from index lookup → NLJ is just M comparisons
        List<Map<String, Object>> singleOrder = List.of(
            row("order_id", 101, "customer_id", 1, "amount", 500));

        List<Map<String, Object>> result = join.nestedLoopJoin(
            singleOrder, "customer_id",
            customers, "customer_id",
            List.of("order_id", "name"));

        assertEquals(1, result.size());
        assertEquals("Alice", result.get(0).get("name"));
    }

    @Test
    void nestedLoopJoin_noMatch_emptyResult() {
        List<Map<String, Object>> unknownOrder = List.of(
            row("order_id", 999, "customer_id", 999, "amount", 0));

        assertTrue(join.nestedLoopJoin(
            unknownOrder, "customer_id", customers, "customer_id",
            List.of("order_id", "name")).isEmpty());
    }

    @Test
    void nestedLoopJoin_multipleMatchesPerOuter() {
        // Alice (customer_id=1) has 2 orders
        List<Map<String, Object>> aliceCustomer = List.of(
            row("customer_id", 1, "name", "Alice", "region", "EU"));

        List<Map<String, Object>> result = join.nestedLoopJoin(
            orders, "customer_id",
            aliceCustomer, "customer_id",
            List.of("order_id", "name"));

        assertEquals(2, result.size(), "2 orders for customer_id=1");
    }

    // -----------------------------------------------------------------------
    // Index Nested-Loop Join
    // -----------------------------------------------------------------------

    @Test
    void indexNestedLoopJoin_usesIndexForEachOuter() {
        // Build a simulated index: customer_id → customer rows
        Map<Object, List<Map<String, Object>>> index = new HashMap<>();
        for (Map<String, Object> c : customers) {
            index.computeIfAbsent(c.get("customer_id"), k -> new ArrayList<>()).add(c);
        }

        List<Map<String, Object>> result = join.indexNestedLoopJoin(
            orders, "customer_id",
            key -> index.getOrDefault(key, List.of()),
            List.of("order_id", "name", "amount"));

        assertEquals(4, result.size());
    }

    @Test
    void indexNestedLoopJoin_singleOuterRow_oneProbe() {
        // In OLTP: outer = 1 row, index does 1 lookup → total cost = O(log M)
        Map<Object, List<Map<String, Object>>> index = new HashMap<>();
        for (Map<String, Object> c : customers) {
            index.computeIfAbsent(c.get("customer_id"), k -> new ArrayList<>()).add(c);
        }

        List<Map<String, Object>> oneOrder = List.of(
            row("order_id", 102, "customer_id", 2, "amount", 300));

        List<Map<String, Object>> result = join.indexNestedLoopJoin(
            oneOrder, "customer_id",
            key -> index.getOrDefault(key, List.of()),
            List.of("order_id", "name"));

        assertEquals(1, result.size());
        assertEquals("Bob", result.get(0).get("name"));
    }

    @Test
    void indexNestedLoopJoin_vs_nestedLoop_sameResults() {
        Map<Object, List<Map<String, Object>>> index = new HashMap<>();
        for (Map<String, Object> c : customers)
            index.computeIfAbsent(c.get("customer_id"), k -> new ArrayList<>()).add(c);

        var cols = List.of("order_id", "customer_id", "name");
        var nlj  = join.nestedLoopJoin(orders, "customer_id", customers, "customer_id", cols);
        var inlj = join.indexNestedLoopJoin(orders, "customer_id",
            key -> index.getOrDefault(key, List.of()), cols);

        assertEquals(nlj.size(), inlj.size(),
            "NLJ and INLJ must produce the same number of rows");
    }

    // -----------------------------------------------------------------------
    // Hash Join
    // -----------------------------------------------------------------------

    @Test
    void hashJoin_sameResultAsNestedLoop() {
        var cols = List.of("order_id", "customer_id", "name");
        var nlj  = join.nestedLoopJoin(orders, "customer_id", customers, "customer_id", cols);
        var hj   = join.hashJoin(customers, "customer_id", orders, "customer_id", cols);

        assertEquals(nlj.size(), hj.size());
    }

    @Test
    void hashJoin_noIndex_worksOnRawRows() {
        List<Map<String, Object>> result = join.hashJoin(
            customers, "customer_id",
            orders, "customer_id",
            List.of("order_id", "name", "amount"));

        assertEquals(4, result.size());
        assertTrue(result.stream().anyMatch(r -> "Alice".equals(r.get("name"))));
        assertTrue(result.stream().anyMatch(r -> "Bob".equals(r.get("name"))));
    }

    // -----------------------------------------------------------------------
    // OLTP vs OLAP contrast — all three OLTP strategies agree
    // -----------------------------------------------------------------------

    @Test
    void allOltpStrategiesAgreeOnRowCount() {
        Map<Object, List<Map<String, Object>>> index = new HashMap<>();
        for (Map<String, Object> c : customers)
            index.computeIfAbsent(c.get("customer_id"), k -> new ArrayList<>()).add(c);

        var cols = List.of("order_id", "name");
        int nlj  = join.nestedLoopJoin(orders, "customer_id", customers, "customer_id", cols).size();
        int inlj = join.indexNestedLoopJoin(orders, "customer_id",
            k -> index.getOrDefault(k, List.of()), cols).size();
        int hj   = join.hashJoin(customers, "customer_id", orders, "customer_id", cols).size();

        assertEquals(nlj, inlj, "NLJ and INLJ must agree");
        assertEquals(nlj, hj,   "NLJ and Hash join must agree");
    }

    // -----------------------------------------------------------------------
    // Utility
    // -----------------------------------------------------------------------

    private static Map<String, Object> row(Object... kvPairs) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kvPairs.length; i += 2)
            m.put((String) kvPairs[i], kvPairs[i + 1]);
        return m;
    }
}
