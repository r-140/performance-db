package com.iu.sql;

import java.util.regex.*;

/**
 * Hand-written recursive-descent parser for the minimal SQL subset.
 *
 * Supported grammar:
 *   SELECT * FROM <table> [WHERE <field> = <value>] [LIMIT <n>]
 *
 * Intentionally simple — the goal is to show how a query flows from
 * text through parse → plan → execute, not to implement a full SQL parser.
 *
 * Uses text blocks (JEP 378, Java 15) for the regex patterns so they
 * are readable inline.
 */
public class SqlParser {

    // Text block (Java 15) — multiline string without escape noise
    private static final Pattern SELECT_PATTERN = Pattern.compile(
            """
            (?i)SELECT\\s+\\*\\s+FROM\\s+(\\w+)
            (?:\\s+WHERE\\s+(\\w+)\\s*=\\s*'?([^'\\s]+)'?)?
            (?:\\s+LIMIT\\s+(\\d+))?
            \\s*;?\\s*
            """.trim().replace("\n", ""),
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Parse a SQL string into a SelectStatement.
     *
     * @param sql the query string
     * @return parsed AST node
     * @throws SqlParseException if the query doesn't match the supported grammar
     */
    public SqlNode.SelectStatement parse(String sql) {
        if (sql == null || sql.isBlank())
            throw new SqlParseException("Empty query");

        Matcher m = SELECT_PATTERN.matcher(sql.trim());
        if (!m.matches())
            throw new SqlParseException(
                "Unsupported SQL. Only: SELECT * FROM <table> [WHERE <field> = <value>] [LIMIT <n>]");

        String table = m.group(1).toLowerCase();

        // WHERE clause — optional
        SqlNode.WhereClause where = null;
        if (m.group(2) != null) {
            String field = m.group(2).toLowerCase();
            String value = m.group(3);
            where = new SqlNode.WhereClause(new SqlNode.EqPredicate(field, value));
        }

        // LIMIT — optional, -1 = unlimited
        int limit = m.group(4) != null ? Integer.parseInt(m.group(4)) : -1;

        return new SqlNode.SelectStatement(table, where, limit);
    }

    public static class SqlParseException extends RuntimeException {
        public SqlParseException(String message) { super(message); }
    }
}
