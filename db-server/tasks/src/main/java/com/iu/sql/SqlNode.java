package com.iu.sql;

import java.util.List;

/**
 * SQL AST nodes — sealed interfaces + records (Java 17/21).
 *
 * Supported grammar:
 *   SELECT cols FROM table [JOIN table ON col = col] [WHERE col = val] [LIMIT n]
 */
public sealed interface SqlNode
        permits SqlNode.SelectStatement,
                SqlNode.JoinClause,
                SqlNode.WhereClause,
                SqlNode.EqPredicate {

    /**
     * SELECT [cols] FROM table [JOIN ...] [WHERE ...] [LIMIT n]
     *
     * @param table       primary (left) table — always "data" in our OLTP DB
     * @param join        optional inner join clause
     * @param where       optional equality filter on the primary table
     * @param selectCols  columns to return (* = null = all)
     * @param limit       max rows (-1 = unlimited)
     */
    record SelectStatement(
            String        table,
            JoinClause    join,        // nullable
            WhereClause   where,       // nullable
            List<String>  selectCols,  // null → SELECT *
            int           limit
    ) implements SqlNode {}

    /**
     * INNER JOIN rightTable ON leftCol = rightCol
     *
     * @param rightTable  table to join against
     * @param leftCol     join column from the left table
     * @param rightCol    join column from the right table
     * @param joinType    algorithm hint (HASH, NESTED_LOOP, or AUTO)
     */
    record JoinClause(
            String   rightTable,
            String   leftCol,
            String   rightCol,
            JoinType joinType
    ) implements SqlNode {}

    record WhereClause(EqPredicate predicate) implements SqlNode {}

    record EqPredicate(String field, String value) implements SqlNode {}

    enum JoinType { HASH, NESTED_LOOP, AUTO }
}
