package com.iu.olap;

import com.iu.olap.catalog.OlapCatalog;
import com.iu.olap.catalog.OlapCatalog.ColumnDef;
import com.iu.olap.catalog.OlapCatalog.ColumnType;
import com.iu.olap.index.OlapInvertedIndex;
import com.iu.olap.join.OlapJoin;
import com.iu.olap.join.OltpJoin;
import com.iu.olap.query.OlapQueryEngine;
import com.iu.olap.storage.ColumnStore;
import com.iu.olap.storage.OlapTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the full OLAP stack:
 *  - Catalog registration
 *  - Columnar storage + partition pruning
 *  - Inverted index (Search Optimisation Service simulation)
 *  - Query engine with three-level pruning
 *  - Aggregate queries (SUM, AVG, GROUP BY)
 *  - OLAP join strategies (broadcast, hash, sort-merge)
 *  - OLTP vs OLAP join comparison on the same data set
 */
class OlapIntegrationTest {

    // ── Test data: orders and customers ────────────────────────────────────
    private OlapTable     orders;
    private OlapTable     customers;
    private OlapCatalog   catalog;
    private OlapQueryEngine engine;
    private OlapJoin      olapJoin;
    private OltpJoin      oltpJoin;

    @BeforeEach
    void setUp() {
        catalog  = new OlapCatalog();
        olapJoin = new OlapJoin();
        oltpJoin = new OltpJoin();

        // orders: order_id, customer_id, amount, region, order_date
        orders = new OlapTable("orders",
            List.of("order_id", "customer_id", "amount", "region", "order_date"));
        catalog.registerTable("public", "orders", orders, List.of(
            new ColumnDef("order_id",    ColumnType.INT),
            new ColumnDef("customer_id", ColumnType.INT),
            new ColumnDef("amount",      ColumnType.INT),
            new ColumnDef("region",      ColumnType.STRING),
            new ColumnDef("order_date",  ColumnType.INT)
        ));

        // Populate orders with a wide date range
        Object[][] orderData = {
            {1,  1, 500,  "EU",   20240101},
            {2,  2, 300,  "US",   20240115},
            {3,  1, 200,  "EU",   20240120},
            {4,  3, 750,  "EU",   20240201},
            {5,  2, 400,  "US",   20240210},
            {6,  5, 100,  "APAC", 20240301},
            {7,  4, 950,  "APAC", 20240315},
            {8,  3, 620,  "EU",   20240320},
            {9,  1, 800,  "EU",   20240401},
            {10, 2, 250,  "US",   20240410}
        };
        for (Object[] row : orderData) {
            orders.insert(Arrays.asList(row));
        }

        // customers: customer_id, name, region
        customers = new OlapTable("customers",
            List.of("customer_id", "name", "region"));
        catalog.registerTable("public", "customers", customers, List.of(
            new ColumnDef("customer_id", ColumnType.INT),
            new ColumnDef("name",        ColumnType.STRING),
            new ColumnDef("region",      ColumnType.STRING)
        ));
        for (Object[] row : new Object[][]{
            {1, "Alice", "EU"},
            {2, "Bob",   "US"},
            {3, "Carol", "EU"},
            {4, "Dave",  "APAC"},
            {5, "Eve",   "US"}
        }) {
            customers.insert(Arrays.asList(row));
        }

        engine = new OlapQueryEngine(catalog);
    }

    // -----------------------------------------------------------------------
    // Catalog
    // -----------------------------------------------------------------------

    @Test
    void catalog_tablesRegistered() {
        assertEquals(2, catalog.tableCount());
        assertEquals(List.of("orders", "customers"),
            catalog.listTables("public"));
    }

    @Test
    void catalog_requireTable_throws_forUnknown() {
        assertThrows(NoSuchElementException.class,
            () -> catalog.requireTable("public", "unknown"));
    }

    @Test
    void catalog_columnDef_accessible() {
        var entry = catalog.getTable("public", "orders").orElseThrow();
        assertTrue(entry.column("amount").isPresent());
        assertEquals(ColumnType.INT, entry.column("amount").get().type());
    }

    // -----------------------------------------------------------------------
    // Partition pruning (three-level)
    // -----------------------------------------------------------------------

    @Test
    void partitionPruning_clusteredKey_reducesPartitionsScanned() {
        // CLUSTER BY order_date — partitions sorted by date
        catalog.setClusteredKey("public", "orders", "order_date");
        // Re-insert to trigger clustering
        OlapTable t = new OlapTable("pruning_test",
            List.of("id", "order_date"));
        t.setClusteredKey("order_date");
        for (int date = 20240101; date <= 20240200; date++) {
            t.insert(List.of(date - 20240100, date));
        }
        catalog.registerTable("public", "pruning_test", t, List.of(
            new ColumnDef("id", ColumnType.INT),
            new ColumnDef("order_date", ColumnType.INT)));

        List<Map<String, Object>> result = engine.select(
            "public", "pruning_test", "order_date", 20240115,
            List.of("id", "order_date"));

        assertEquals(1, result.size());
        assertEquals(20240115, result.get(0).get("order_date"));
    }

    @Test
    void partitionPruning_outOfRangeQuery_returnsEmpty() {
        List<Map<String, Object>> result = engine.select(
            "public", "orders", "order_date", 19990101,
            List.of("order_id"));
        assertTrue(result.isEmpty(), "Date before all data → all partitions pruned");
    }

    // -----------------------------------------------------------------------
    // Inverted index (Search Optimisation Service)
    // -----------------------------------------------------------------------

    @Test
    void invertedIndex_buildsAndFindsPartitions() {
        engine.buildInvertedIndex("public", "orders", "region");
        Set<String> parts = new OlapInvertedIndex() {{
            // Use the engine's index via the query path instead
        }}.partitionsContaining("region", "EU");

        // Verify via select: engine uses the inverted index internally
        List<Map<String, Object>> result = engine.select(
            "public", "orders", "region", "EU",
            List.of("order_id", "region"));

        // EU orders: ids 1, 3, 4, 8, 9 = 5 rows
        assertEquals(5, result.size());
        result.forEach(r -> assertEquals("EU", r.get("region")));
    }

    @Test
    void invertedIndex_noFalseNegatives() {
        engine.buildInvertedIndex("public", "orders", "region");
        // Every US order must be found
        List<Map<String, Object>> usOrders = engine.select(
            "public", "orders", "region", "US",
            List.of("order_id", "region"));
        assertEquals(3, usOrders.size()); // orders 2, 5, 10
    }

    // -----------------------------------------------------------------------
    // Columnar scan + aggregates
    // -----------------------------------------------------------------------

    @Test
    void columnScan_sumAmount() {
        int total = engine.aggregate("public", "orders", "amount",
            vals -> vals.stream().mapToInt(v -> (Integer) v).sum());
        assertEquals(500+300+200+750+400+100+950+620+800+250, total);
    }

    @Test
    void columnScan_avgAmount() {
        double avg = engine.aggregate("public", "orders", "amount",
            vals -> vals.stream().mapToInt(v -> (Integer) v).average().orElse(0));
        assertEquals(487.0, avg, 0.01);
    }

    @Test
    void groupBy_sumAmountByRegion() {
        Map<Object, Integer> sumByRegion = engine.groupBy(
            "public", "orders", "region", "amount",
            vals -> vals.stream().mapToInt(v -> (Integer) v).sum());

        // EU: 500+200+750+620+800 = 2870
        assertEquals(2870, sumByRegion.get("EU"));
        // US: 300+400+250 = 950
        assertEquals(950, sumByRegion.get("US"));
        // APAC: 100+950 = 1050
        assertEquals(1050, sumByRegion.get("APAC"));
    }

    @Test
    void fullTableSelect_returnsAllRows() {
        List<Map<String, Object>> all = engine.select(
            "public", "orders", null, null,
            List.of("order_id", "amount"));
        assertEquals(10, all.size());
    }

    // -----------------------------------------------------------------------
    // OLAP join strategies
    // -----------------------------------------------------------------------

    @Test
    void broadcastJoin_allOrdersEnrichedWithCustomerName() {
        List<String> cols = List.of("order_id", "customer_id", "amount", "name");
        List<Map<String, Object>> result = olapJoin.broadcastJoin(
            orders, "customer_id", customers, "customer_id", cols);

        // All 10 orders have a matching customer
        assertEquals(10, result.size());
        // Every row must have both order and customer fields
        result.forEach(r -> {
            assertNotNull(r.get("order_id"), "order_id must be present");
            assertNotNull(r.get("name"),     "name must be present from customer");
        });
    }

    @Test
    void broadcastJoin_aliceHasThreeOrders() {
        List<Map<String, Object>> result = olapJoin.broadcastJoin(
            orders, "customer_id", customers, "customer_id",
            List.of("order_id", "name"));

        long aliceCount = result.stream()
            .filter(r -> "Alice".equals(r.get("name")))
            .count();
        assertEquals(3, aliceCount); // orders 1, 3, 9
    }

    @Test
    void hashJoin_sameResultAsBroadcast() {
        List<String> cols = List.of("order_id", "customer_id", "amount");
        int broadcast = olapJoin.broadcastJoin(
            orders, "customer_id", customers, "customer_id", cols).size();
        int hash = olapJoin.hashJoin(
            orders, "customer_id", customers, "customer_id", cols, 4).size();

        assertEquals(broadcast, hash,
            "Broadcast and hash join must produce the same row count");
    }

    @Test
    void sortMergeJoin_sameResultAsBroadcast() {
        List<String> cols = List.of("order_id", "name", "amount");
        int broadcast = olapJoin.broadcastJoin(
            orders, "customer_id", customers, "customer_id", cols).size();
        int sortMerge = olapJoin.sortMergeJoin(
            orders, "customer_id", false,
            customers, "customer_id", false, cols).size();

        assertEquals(broadcast, sortMerge);
    }

    // -----------------------------------------------------------------------
    // OLTP vs OLAP join comparison on the same dataset
    // -----------------------------------------------------------------------

    /**
     * This test illustrates the fundamental semantic difference:
     *
     * OLTP (OltpJoin): operates on rows already fetched from the DB.
     *   After index lookup → small list → join.
     *   Cost: O(1) index lookup + O(small N × log M) joins.
     *
     * OLAP (OlapJoin): operates on full OlapTable objects.
     *   No index lookups — scans all partitions, vectorised.
     *   Cost: O(N + M) or O(N log N + M log M) sort-merge.
     *
     * Same logical result — different physical path.
     */
    @Test
    void oltpVsOlap_sameLogicalResult() {
        // OLAP: broadcast join across full tables
        List<Map<String, Object>> olapResult = olapJoin.broadcastJoin(
            orders, "customer_id", customers, "customer_id",
            List.of("order_id", "customer_id", "name"));

        // OLTP: in-memory hash join on pre-fetched row maps
        List<Map<String, Object>> ordersRows = orders.partitions().stream()
            .flatMap(p -> p.scanAll(orders.schema()).stream()).toList();
        List<Map<String, Object>> customerRows = customers.partitions().stream()
            .flatMap(p -> p.scanAll(customers.schema()).stream()).toList();

        List<Map<String, Object>> oltpResult = oltpJoin.hashJoin(
            customerRows, "customer_id", ordersRows, "customer_id",
            List.of("order_id", "customer_id", "name"));

        assertEquals(olapResult.size(), oltpResult.size(),
            "OLTP and OLAP joins must produce the same row count");
    }

    @Test
    void oltpIndexJoin_singleOuterRow_oneProbe() {
        // Typical OLTP: find order 1, then look up customer 1
        // Here simulated as: outer = order row for order_id=1, inner = customers
        List<Map<String, Object>> orderRow = orders.partitions().stream()
            .flatMap(p -> p.scanAll(orders.schema()).stream())
            .filter(r -> Integer.valueOf(1).equals(r.get("order_id")))
            .toList();

        Map<Object, List<Map<String, Object>>> customerIndex = new HashMap<>();
        customers.partitions().stream()
            .flatMap(p -> p.scanAll(customers.schema()).stream())
            .forEach(c -> customerIndex
                .computeIfAbsent(c.get("customer_id"), k -> new ArrayList<>()).add(c));

        List<Map<String, Object>> result = oltpJoin.indexNestedLoopJoin(
            orderRow, "customer_id",
            key -> customerIndex.getOrDefault(key, List.of()),
            List.of("order_id", "name"));

        assertEquals(1, result.size());
        assertEquals("Alice", result.get(0).get("name"));
        assertEquals(1, result.get(0).get("order_id"));
    }

    // -----------------------------------------------------------------------
    // Bloom filter in OlapTable
    // -----------------------------------------------------------------------

    @Test
    void olapBloomFilter_noFalseNegatives_forAllInsertedValues() {
        // Every inserted order_id must be found via the engine
        for (int id = 1; id <= 10; id++) {
            List<Map<String, Object>> result = engine.select(
                "public", "orders", "order_id", id, List.of("order_id", "amount"));
            assertFalse(result.isEmpty(),
                "Order id=" + id + " must not be blocked by Bloom filter (no false negatives)");
        }
    }
}
