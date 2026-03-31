package com.iu.sql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests QueryPlanner plan selection logic.
 *
 * The planner reads the index registry file to decide which plan to use.
 * Since these tests run without a live DB or index files, the planner
 * always falls back to FullScan or the default join type. We verify
 * the logic structure rather than actual index selection.
 */
class QueryPlannerTest {

    private final SqlParser    parser  = new SqlParser();
    private final QueryPlanner planner = new QueryPlanner();

    // ── No-index fallbacks ────────────────────────────────────────────────

    @Test void noWhere_givesFullScan() {
        var plan = planner.plan(parser.parse("SELECT * FROM data"));
        assertInstanceOf(QueryPlan.FullScan.class, plan);
        assertNull(((QueryPlan.FullScan) plan).field());
    }

    @Test void whereId_noIndex_givesFullScanOnId() {
        var plan = planner.plan(parser.parse("SELECT * FROM data WHERE id = 42"));
        // Without an index registry, falls back to FullScan
        assertInstanceOf(QueryPlan.FullScan.class, plan);
        assertEquals("id", ((QueryPlan.FullScan) plan).field());
        assertEquals("42", ((QueryPlan.FullScan) plan).value());
    }

    @Test void whereField_noIndex_givesFullScan() {
        var plan = planner.plan(parser.parse("SELECT * FROM data WHERE data = 'testdata5'"));
        assertInstanceOf(QueryPlan.FullScan.class, plan);
        assertEquals("data", ((QueryPlan.FullScan) plan).field());
    }

    // ── Join plan selection ────────────────────────────────────────────────

    @Test void join_withFullScanOuter_defaultsToHashJoin() {
        // FullScan outer (large) → planner chooses HashJoinPlan
        var plan = planner.plan(parser.parse(
            "SELECT * FROM data JOIN ref ON id = cid"));
        assertInstanceOf(QueryPlan.HashJoinPlan.class, plan,
            "Large outer (FullScan) with no index → HashJoin expected");
        var hj = (QueryPlan.HashJoinPlan) plan;
        assertEquals("ref", hj.rightTable());
        assertEquals("id",  hj.outerJoinCol());
        assertEquals("cid", hj.innerJoinCol());
    }

    @Test void join_hashHint_overridesPlannerChoice() {
        var plan = planner.plan(parser.parse(
            "SELECT * FROM data JOIN ref ON id = cid HASH"));
        assertInstanceOf(QueryPlan.HashJoinPlan.class, plan);
    }

    @Test void join_nestedLoopHint_overridesPlannerChoice() {
        var plan = planner.plan(parser.parse(
            "SELECT * FROM data JOIN ref ON id = cid NESTED_LOOP"));
        assertInstanceOf(QueryPlan.NestedLoopJoinPlan.class, plan);
        var nl = (QueryPlan.NestedLoopJoinPlan) plan;
        assertEquals("ref", nl.rightTable());
    }

    @Test void join_whereWithJoin_outerPlanWrappedInsideJoinPlan() {
        var plan = planner.plan(parser.parse(
            "SELECT * FROM data JOIN ref ON id = cid WHERE id = 5 NESTED_LOOP"));
        assertInstanceOf(QueryPlan.NestedLoopJoinPlan.class, plan);
        var nl = (QueryPlan.NestedLoopJoinPlan) plan;
        // The outer plan is a FullScan on id (no index available in test env)
        assertInstanceOf(QueryPlan.FullScan.class, nl.outerPlan());
    }

    @Test void join_rightTableIsPreserved() {
        var plan = planner.plan(parser.parse(
            "SELECT * FROM data JOIN customers ON customer_id = id HASH"));
        assertInstanceOf(QueryPlan.HashJoinPlan.class, plan);
        assertEquals("customers", ((QueryPlan.HashJoinPlan) plan).rightTable());
    }

    @Test void join_joinColumnsArePreserved() {
        var plan = planner.plan(parser.parse(
            "SELECT * FROM data JOIN orders ON data.id = orders.customer_id HASH"));
        var hj = (QueryPlan.HashJoinPlan) plan;
        assertEquals("id",          hj.outerJoinCol());
        assertEquals("customer_id", hj.innerJoinCol());
    }
}
