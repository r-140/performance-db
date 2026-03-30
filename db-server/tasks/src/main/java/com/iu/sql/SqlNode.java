package com.iu.sql;

import java.util.List;

/**
 * Minimal SQL AST using sealed interfaces + records (Java 17/21).
 *
 * Only SELECT is supported:
 *   SELECT * FROM <table> [WHERE <field> = <value>] [LIMIT <n>]
 *
 * The sealed hierarchy makes the planner's switch exhaustive — adding a
 * new node type is a compile error until all switches handle it.
 */
public sealed interface SqlNode
        permits SqlNode.SelectStatement,
                SqlNode.WhereClause,
                SqlNode.EqPredicate {

    /**
     * SELECT * FROM data [WHERE ...] [LIMIT n]
     *
     * @param table  table name (only "data" is supported currently)
     * @param where  optional filter (null = full scan)
     * @param limit  max rows (-1 = unlimited)
     */
    record SelectStatement(
            String table,
            WhereClause where,   // nullable
            int limit            // -1 = no limit
    ) implements SqlNode {}

    /**
     * Wraps a single equality predicate for the WHERE clause.
     * Compound predicates (AND/OR) can be added as additional subtypes.
     */
    record WhereClause(EqPredicate predicate) implements SqlNode {}

    /**
     * field = value  (e.g. id = 42  or  data = 'testdata5')
     *
     * @param field  column name
     * @param value  literal value as string
     */
    record EqPredicate(String field, String value) implements SqlNode {}
}
