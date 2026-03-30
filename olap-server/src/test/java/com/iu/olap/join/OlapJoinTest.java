package com.iu.olap.join;

import com.iu.olap.storage.OlapTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for all OLAP join strategies.
 *
 * Schema used throughout:
 *   orders:    order_id, customer_id, amount
 *   customers: customer_id, name, region
 *
 * Expected join result columns: order_id, customer_id, amount, name, region
 */
class OlapJoinTest {

    private OlapTable orders;
    private OlapTable customers;
    private OlapJoin  join;

    @BeforeEach
    void setUp() {
        orders    = new OlapTable("orders",    List.of("order_id", "customer_id", "amount"));
        customers = new OlapTable("customers", List.of("customer_id", "name", "region"));

        // 5 customers
        customers.insert(List.of(1, "Alice",   "EU"));
        customers.insert(List.of(2, "Bob",     "US"));
        customers.insert(List.of(3, "Carol",   "EU"));
        customers.insert(List.of(4, "Dave",    "APAC"));
        customers.insert(List.of(5, "Eve",     "US"));

        // 8 orders (some customers have multiple)
        orders.insert(List.of(101, 1, 500));
        orders.insert(List.of(102, 2, 300));
        orders.insert(List.of(103, 1, 200)); // Alice's 2nd order
        orders.insert(List.of(104, 3, 750));
        orders.insert(List.of(105, 2, 400)); // Bob's 2nd order
        orders.insert(List.of(106, 5, 100));
        orders.insert(List.of(107, 4, 950));
        orders.insert(List.of(108, 3, 620)); // Carol's 2nd order

        join = new OlapJoin();
    }

    // -----------------------------------------------------------------------
    // Broadcast join
    // -----------------------------------------------------------------------

    @Test
    void broadcastJoin_correctRowCount() {
        // 8 orders, each with a matching customer → 8 joined rows
        List<Map<String, Object>> result = join.broadcastJoin(
            orders, "customer_id",
            customers, "customer_id",
            List.of("order_id", "customer_id", "amount", "name", "region"));

        assertEquals(8, result.size(), "Every order must match exactly one customer");
    }

    @Test
    void broadcastJoin_correctColumnsInResult() {
        List<Map<String, Object>> result = join.broadcastJoin(
            orders, "customer_id",
            customers, "customer_id",
            List.of("order_id", "name", "amount"));

        for (Map<String, Object> row : result) {
            assertTrue(row.containsKey("order_id"));
            assertTrue(row.containsKey("name"));
            assertTrue(row.containsKey("amount"));
        }
    }

    @Test
    void broadcastJoin_aliceHasTwoOrders() {
        List<Map<String, Object>> result = join.broadcastJoin(
            orders, "customer_id",
            customers, "customer_id",
            List.of("order_id", "name", "amount"));

        long aliceOrders = result.stream()
            .filter(r -> "Alice".equals(r.get("name")))
            .count();
        assertEquals(2, aliceOrders, "Alice should appear twice (2 orders)");
    }

    @Test
    void broadcastJoin_correctAmountForOrder101() {
        List<Map<String, Object>> result = join.broadcastJoin(
            orders, "customer_id",
            customers, "customer_id",
            List.of("order_id", "name", "amount"));

        Optional<Map<String, Object>> order101 = result.stream()
            .filter(r -> Integer.valueOf(101).equals(r.get("order_id")))
            .findFirst();
        assertTrue(order101.isPresent());
        assertEquals("Alice", order101.get().get("name"));
        assertEquals(500, order101.get().get("amount"));
    }

    @Test
    void broadcastJoin_noMatchingInner_emptyResult() {
        OlapTable emptyCustomers = new OlapTable("customers", List.of("customer_id", "name"));
        List<Map<String, Object>> result = join.broadcastJoin(
            orders, "customer_id",
            emptyCustomers, "customer_id",
            List.of("order_id", "name"));
        assertTrue(result.isEmpty(), "No customers → no join results");
    }

    // -----------------------------------------------------------------------
    // Hash join (redistribute / shuffle)
    // -----------------------------------------------------------------------

    @Test
    void hashJoin_sameResultAsBroadcast() {
        List<Map<String, Object>> broadcast = join.broadcastJoin(
            orders, "customer_id",
            customers, "customer_id",
            List.of("order_id", "customer_id", "amount"));

        List<Map<String, Object>> hash = join.hashJoin(
            orders, "customer_id",
            customers, "customer_id",
            List.of("order_id", "customer_id", "amount"), 4);

        assertEquals(broadcast.size(), hash.size(),
            "Hash join and broadcast join must produce the same row count");
    }

    @Test
    void hashJoin_multipleOrdersPerCustomer() {
        List<Map<String, Object>> result = join.hashJoin(
            orders, "customer_id",
            customers, "customer_id",
            List.of("order_id", "name"), 4);

        long carolOrders = result.stream()
            .filter(r -> "Carol".equals(r.get("name")))
            .count();
        assertEquals(2, carolOrders, "Carol has 2 orders");
    }

    @Test
    void hashJoin_singleBucket_worksCorrectly() {
        List<Map<String, Object>> result = join.hashJoin(
            orders, "customer_id",
            customers, "customer_id",
            List.of("order_id", "name"), 1);
        assertEquals(8, result.size());
    }

    // -----------------------------------------------------------------------
    // Sort-merge join
    // -----------------------------------------------------------------------

    @Test
    void sortMergeJoin_unsorted_producesCorrectResult() {
        List<Map<String, Object>> result = join.sortMergeJoin(
            orders, "customer_id", false,
            customers, "customer_id", false,
            List.of("order_id", "name", "amount"));

        assertEquals(8, result.size());
    }

    @Test
    void sortMergeJoin_clusteredInput_sameResult() {
        // Cluster orders and customers on customer_id (simulates CLUSTER BY)
        orders.setClusteredKey("customer_id");
        customers.setClusteredKey("customer_id");

        // Re-insert with clustering applied
        OlapTable sortedOrders    = new OlapTable("orders",    List.of("order_id", "customer_id", "amount"));
        OlapTable sortedCustomers = new OlapTable("customers", List.of("customer_id", "name"));
        sortedOrders.setClusteredKey("customer_id");
        sortedCustomers.setClusteredKey("customer_id");

        for (var row : List.of(
            List.<Object>of(101,1,500), List.<Object>of(103,1,200),
            List.<Object>of(102,2,300), List.<Object>of(105,2,400),
            List.<Object>of(104,3,750), List.<Object>of(108,3,620))) {
            sortedOrders.insert(row);
        }
        for (var row : List.of(
            List.<Object>of(1,"Alice"), List.<Object>of(2,"Bob"), List.<Object>of(3,"Carol"))) {
            sortedCustomers.insert(row);
        }

        List<Map<String, Object>> result = join.sortMergeJoin(
            sortedOrders, "customer_id", true,
            sortedCustomers, "customer_id", true,
            List.of("order_id", "name"));

        assertEquals(6, result.size());
    }

    @Test
    void sortMergeJoin_verifyAliceMappings() {
        List<Map<String, Object>> result = join.sortMergeJoin(
            orders, "customer_id", false,
            customers, "customer_id", false,
            List.of("order_id", "name", "amount"));

        long alice = result.stream().filter(r -> "Alice".equals(r.get("name"))).count();
        assertEquals(2, alice, "Alice has 2 orders in sort-merge join");
    }

    // -----------------------------------------------------------------------
    // Strategy comparison — all three must agree on row count
    // -----------------------------------------------------------------------

    @Test
    void allStrategiesAgreeOnRowCount() {
        List<String> cols = List.of("order_id", "customer_id", "name", "amount");

        int broadcast = join.broadcastJoin(
            orders, "customer_id", customers, "customer_id", cols).size();
        int hash = join.hashJoin(
            orders, "customer_id", customers, "customer_id", cols, 3).size();
        int sortMerge = join.sortMergeJoin(
            orders, "customer_id", false, customers, "customer_id", false, cols).size();

        assertEquals(broadcast, hash,
            "Broadcast and hash join must produce the same count");
        assertEquals(broadcast, sortMerge,
            "Broadcast and sort-merge join must produce the same count");
    }
}
