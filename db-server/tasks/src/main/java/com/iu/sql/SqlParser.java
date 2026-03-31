package com.iu.sql;

import java.util.Arrays;
import java.util.List;
import java.util.regex.*;

/**
 * SQL parser supporting SELECT, optional JOIN, optional WHERE, optional LIMIT.
 *
 * Supported grammar (case-insensitive):
 *
 *   SELECT * FROM data
 *   SELECT * FROM data WHERE id = 42
 *   SELECT * FROM data WHERE id = 42 LIMIT 1
 *   SELECT * FROM data INNER JOIN lookup ON data.category = lookup.id
 *   SELECT * FROM data JOIN lookup ON data.category = lookup.id WHERE id = 5
 *   SELECT * FROM data JOIN lookup ON data.category = lookup.id LIMIT 10
 *   SELECT * FROM data JOIN lookup ON data.category = lookup.id [HASH|NESTED_LOOP]
 *
 * Uses text blocks (Java 15) for readable regex patterns.
 */
public class SqlParser {

    // Pattern: SELECT * FROM left [JOIN right ON lc = rc [HASH|NESTED_LOOP]] [WHERE f = v] [LIMIT n]
    private static final Pattern SELECT_PATTERN = Pattern.compile(
        "(?i)SELECT\\s+(\\*|[\\w,\\s]+)\\s+FROM\\s+(\\w+)" +
        "(?:\\s+(?:INNER\\s+)?JOIN\\s+(\\w+)\\s+ON\\s+(\\w+(?:\\.\\w+)?)\\s*=\\s*(\\w+(?:\\.\\w+)?)" +
        "(?:\\s+(HASH|NESTED_LOOP))?)?" +
        "(?:\\s+WHERE\\s+(\\w+)\\s*=\\s*'?([^'\\s;]+)'?)?" +
        "(?:\\s+LIMIT\\s+(\\d+))?\\s*;?\\s*"
    );

    public SqlNode.SelectStatement parse(String sql) {
        if (sql == null || sql.isBlank()) throw new SqlParseException("Empty query");

        Matcher m = SELECT_PATTERN.matcher(sql.trim());
        if (!m.matches()) throw new SqlParseException(
            "Unsupported SQL. Supported: SELECT [*|cols] FROM table " +
            "[JOIN table ON col=col [HASH|NESTED_LOOP]] [WHERE col=val] [LIMIT n]");

        // SELECT columns
        String rawCols = m.group(1).trim();
        List<String> selectCols = rawCols.equals("*") ? null :
            Arrays.stream(rawCols.split(",")).map(String::trim).toList();

        String table = m.group(2).toLowerCase();

        // JOIN clause
        SqlNode.JoinClause join = null;
        if (m.group(3) != null) {
            String rightTable = m.group(3).toLowerCase();
            // Strip table prefix from ON columns (data.id → id)
            String leftCol  = stripPrefix(m.group(4));
            String rightCol = stripPrefix(m.group(5));
            SqlNode.JoinType joinType = m.group(6) != null
                ? SqlNode.JoinType.valueOf(m.group(6).toUpperCase())
                : SqlNode.JoinType.AUTO;
            join = new SqlNode.JoinClause(rightTable, leftCol, rightCol, joinType);
        }

        // WHERE clause
        SqlNode.WhereClause where = null;
        if (m.group(7) != null) {
            where = new SqlNode.WhereClause(
                new SqlNode.EqPredicate(m.group(7).toLowerCase(), m.group(8)));
        }

        int limit = m.group(9) != null ? Integer.parseInt(m.group(9)) : -1;

        return new SqlNode.SelectStatement(table, join, where, selectCols, limit);
    }

    private static String stripPrefix(String col) {
        int dot = col.indexOf('.');
        return dot >= 0 ? col.substring(dot + 1).toLowerCase() : col.toLowerCase();
    }

    public static class SqlParseException extends RuntimeException {
        public SqlParseException(String msg) { super(msg); }
    }
}
