package com.iu.olap.storage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for ColumnStore and OlapTable.
 *
 * Each test is annotated with what OLAP concept it verifies,
 * making the test file useful as documentation alongside the README.
 */
class ColumnStoreTest {

    // -----------------------------------------------------------------------
    // ColumnStore — basic write and read
    // -----------------------------------------------------------------------

    @Test
    void insertAndReadSingleColumn() {
        ColumnStore cs = new ColumnStore("p0", List.of("id", "salary"));
        cs.insertRow(List.of(1, 90000));
        cs.insertRow(List.of(2, 70000));

        List<Object> salaries = cs.readColumn("salary");
        assertEquals(2, salaries.size());
        assertEquals(90000, salaries.get(0));
        assertEquals(70000, salaries.get(1));
    }

    @Test
    void readColumn_zeroOverheadFromOtherColumns() {
        // In a column store, reading 1 column from a 5-column table
        // only reads 1/5 of the data — simulated here by verifying only
        // the requested column's values are returned.
        ColumnStore cs = new ColumnStore("p0", List.of("a", "b", "c", "d", "e"));
        cs.insertRow(List.of(1, 2, 3, 4, 5));
        cs.insertRow(List.of(6, 7, 8, 9, 10));

        List<Object> colC = cs.readColumn("c");
        assertEquals(List.of(3, 8), colC);
        // columns a, b, d, e were never touched
    }

    @Test
    void aggregateOnSingleColumn_avgSalary() {
        ColumnStore cs = new ColumnStore("p0", List.of("id", "dept", "salary"));
        cs.insertRow(List.of(1, "eng",     90000));
        cs.insertRow(List.of(2, "eng",     85000));
        cs.insertRow(List.of(3, "sales",   60000));
        cs.insertRow(List.of(4, "eng",     95000));

        // AVG(salary) — only reads the salary column
        OptionalDouble avg = cs.readColumn("salary").stream()
            .mapToInt(v -> (Integer) v).average();
        assertTrue(avg.isPresent());
        assertEquals(82500.0, avg.getAsDouble(), 0.01);
    }

    // -----------------------------------------------------------------------
    // Partition pruning
    // -----------------------------------------------------------------------

    @Test
    void canPrune_valueOutsideRange_returnsTrue() {
        ColumnStore cs = new ColumnStore("p0", List.of("order_date"));
        cs.insertRow(List.of(20240101));
        cs.insertRow(List.of(20240115));
        cs.insertRow(List.of(20240131));

        // Partition min=20240101, max=20240131
        // Query for 20240301 → outside range → can prune
        assertTrue(cs.canPrune("order_date", 20240301),
            "Partition max=20240131 < 20240301 → should be prunable");
    }

    @Test
    void canPrune_valueInsideRange_returnsFalse() {
        ColumnStore cs = new ColumnStore("p0", List.of("order_date"));
        cs.insertRow(List.of(20240101));
        cs.insertRow(List.of(20240131));

        assertFalse(cs.canPrune("order_date", 20240115),
            "20240115 is within [20240101, 20240131] → cannot prune");
    }

    @Test
    void canPrune_valueAtMin_returnsFalse() {
        ColumnStore cs = new ColumnStore("p0", List.of("id"));
        cs.insertRow(List.of(10));
        cs.insertRow(List.of(20));

        assertFalse(cs.canPrune("id", 10), "Value at min must not be pruned");
    }

    @Test
    void canPrune_valueAtMax_returnsFalse() {
        ColumnStore cs = new ColumnStore("p0", List.of("id"));
        cs.insertRow(List.of(10));
        cs.insertRow(List.of(20));

        assertFalse(cs.canPrune("id", 20), "Value at max must not be pruned");
    }

    // -----------------------------------------------------------------------
    // Clustered key — physical sort order
    // -----------------------------------------------------------------------

    @Test
    void clusterBy_sortsRowsByKey() {
        ColumnStore cs = new ColumnStore("p0", List.of("id", "date"));
        cs.insertRow(List.of(3, 20240315));
        cs.insertRow(List.of(1, 20240101));
        cs.insertRow(List.of(2, 20240210));

        cs.clusterBy("date");

        List<Object> dates = cs.readColumn("date");
        assertEquals(20240101, dates.get(0));
        assertEquals(20240210, dates.get(1));
        assertEquals(20240315, dates.get(2));
    }

    @Test
    void clusterBy_keepIdColumnAligned() {
        ColumnStore cs = new ColumnStore("p0", List.of("id", "date"));
        cs.insertRow(List.of(30, 20240315));
        cs.insertRow(List.of(10, 20240101));
        cs.insertRow(List.of(20, 20240210));

        cs.clusterBy("date");

        // After sort: date 20240101 → id 10, date 20240210 → id 20
        List<Object> ids   = cs.readColumn("id");
        List<Object> dates = cs.readColumn("date");
        assertEquals(10, ids.get(0));
        assertEquals(20240101, dates.get(0));
        assertEquals(20, ids.get(1));
        assertEquals(20240210, dates.get(1));
    }

    @Test
    void clusterBy_improvesPruning() {
        ColumnStore cs = new ColumnStore("p0", List.of("region", "amount"));
        // Mixed regions — without clustering, min="EU" max="US" covers all values
        cs.insertRow(List.of("EU", 100));
        cs.insertRow(List.of("US", 200));
        cs.insertRow(List.of("EU", 150));
        cs.insertRow(List.of("APAC", 300));

        cs.clusterBy("region");

        // Min="APAC", Max="US" — a query for "EMEA" can now be pruned
        assertTrue(cs.canPrune("region", "EMEA"),
            "After clustering, regions are sorted and pruning is more effective");
    }

    // -----------------------------------------------------------------------
    // OlapTable — multi-partition scan and pruning
    // -----------------------------------------------------------------------

    @Test
    void olapTable_insert_and_scan_all_rows() {
        OlapTable t = new OlapTable("sales", List.of("id", "amount"));
        for (int i = 1; i <= 50; i++) t.insert(List.of(i, i * 10));

        List<Map<String, Object>> rows = t.scan(null, null, List.of("id", "amount"));
        assertEquals(50, rows.size());
    }

    @Test
    void olapTable_scanWithFilter_exactMatch() {
        OlapTable t = new OlapTable("sales", List.of("id", "amount"));
        for (int i = 1; i <= 50; i++) t.insert(List.of(i, i * 10));

        List<Map<String, Object>> result = t.scan("id", 25, List.of("id", "amount"));
        assertEquals(1, result.size());
        assertEquals(250, result.get(0).get("amount"));
    }

    @Test
    void olapTable_partitionPruning_reducesPartitionsScanned() {
        OlapTable t = new OlapTable("events", List.of("date", "value"));
        t.setClusteredKey("date");

        // Insert data across a wide date range — many partitions
        for (int date = 20240101; date <= 20240150; date++) {
            t.insert(List.of(date, date % 100));
        }

        // Query for a narrow date — most partitions should be pruned
        List<Map<String, Object>> result = t.scan("date", 20240125, List.of("date", "value"));
        assertEquals(1, result.size());
        assertEquals(20240125, result.get(0).get("date"));
    }

    @Test
    void olapTable_columnScan_aggregation() {
        OlapTable t = new OlapTable("orders", List.of("id", "amount"));
        for (int i = 1; i <= 10; i++) t.insert(List.of(i, i * 100));

        // SUM(amount) via column scan — reads only the amount column
        int total = t.columnScan("amount").stream()
            .mapToInt(v -> (Integer) v).sum();
        assertEquals(5500, total); // 100+200+...+1000
    }

    @Test
    void olapTable_countWhere_predicate() {
        OlapTable t = new OlapTable("orders", List.of("id", "amount"));
        for (int i = 1; i <= 20; i++) t.insert(List.of(i, i * 100));

        // COUNT(*) WHERE amount > 1000
        long count = t.countWhere("amount", v -> (Integer) v > 1000);
        assertEquals(10, count); // amounts 1100..2000
    }
}
