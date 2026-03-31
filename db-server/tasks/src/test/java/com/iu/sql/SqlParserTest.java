package com.iu.sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class SqlParserTest {

    private final SqlParser parser = new SqlParser();

    // ── Simple SELECT ────────────────────────────────────────────────────

    @Test void selectAll() {
        var s = parser.parse("SELECT * FROM data");
        assertEquals("data", s.table()); assertNull(s.join()); assertNull(s.where()); assertEquals(-1, s.limit());
    }

    @Test void selectWithWhereId() {
        var s = parser.parse("SELECT * FROM data WHERE id = 42");
        assertNotNull(s.where()); assertEquals("id", s.where().predicate().field()); assertEquals("42", s.where().predicate().value());
    }

    @Test void selectWithStringValue() {
        var s = parser.parse("SELECT * FROM data WHERE data = 'testdata5'");
        assertEquals("testdata5", s.where().predicate().value());
    }

    @Test void selectWithLimit() {
        var s = parser.parse("SELECT * FROM data LIMIT 10");
        assertEquals(10, s.limit()); assertNull(s.where());
    }

    @Test void selectWhereAndLimit() {
        var s = parser.parse("SELECT * FROM data WHERE id = 5 LIMIT 1");
        assertEquals("5", s.where().predicate().value()); assertEquals(1, s.limit());
    }

    // ── JOIN queries ──────────────────────────────────────────────────────

    @Test void parseJoin_basicInnerJoin() {
        var s = parser.parse("SELECT * FROM data JOIN lookup ON data.customer_id = lookup.id");
        assertNotNull(s.join(), "JoinClause must be parsed");
        assertEquals("lookup",      s.join().rightTable());
        assertEquals("customer_id", s.join().leftCol());
        assertEquals("id",          s.join().rightCol());
        assertEquals(SqlNode.JoinType.AUTO, s.join().joinType());
    }

    @Test void parseJoin_withInnerKeyword() {
        var s = parser.parse("SELECT * FROM data INNER JOIN lookup ON id = customer_id");
        assertNotNull(s.join());
        assertEquals("lookup", s.join().rightTable());
    }

    @Test void parseJoin_hashHint() {
        var s = parser.parse("SELECT * FROM data JOIN lookup ON id = cid HASH");
        assertNotNull(s.join());
        assertEquals(SqlNode.JoinType.HASH, s.join().joinType());
    }

    @Test void parseJoin_nestedLoopHint() {
        var s = parser.parse("SELECT * FROM data JOIN lookup ON id = cid NESTED_LOOP");
        assertEquals(SqlNode.JoinType.NESTED_LOOP, s.join().joinType());
    }

    @Test void parseJoin_withWhereClause() {
        var s = parser.parse("SELECT * FROM data JOIN lookup ON id = cid WHERE id = 42");
        assertNotNull(s.join());
        assertNotNull(s.where());
        assertEquals("42", s.where().predicate().value());
    }

    @Test void parseJoin_withLimit() {
        var s = parser.parse("SELECT * FROM data JOIN lookup ON id = cid LIMIT 5");
        assertNotNull(s.join());
        assertEquals(5, s.limit());
    }

    @Test void parseJoin_withWhereAndLimit() {
        var s = parser.parse("SELECT * FROM data JOIN lookup ON id = cid WHERE id = 1 LIMIT 10");
        assertNotNull(s.join()); assertNotNull(s.where()); assertEquals(10, s.limit());
    }

    @Test void parseJoin_stripsTablePrefix() {
        // "data.customer_id" → "customer_id", "lookup.id" → "id"
        var s = parser.parse("SELECT * FROM data JOIN ref ON data.customer_id = ref.id");
        assertEquals("customer_id", s.join().leftCol());
        assertEquals("id",          s.join().rightCol());
    }

    // ── Format variants ──────────────────────────────────────────────────

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT * FROM data;",
        "select * from DATA where id = 1",
        "SELECT  *  FROM  data  WHERE  id  =  7",
        "SELECT * FROM data JOIN ref ON id = cid",
        "SELECT * FROM data INNER JOIN ref ON id = cid HASH",
    })
    void toleratesFormatVariants(String sql) {
        assertDoesNotThrow(() -> parser.parse(sql));
    }

    // ── Unsupported statements ────────────────────────────────────────────

    @ParameterizedTest
    @ValueSource(strings = {
        "INSERT INTO data VALUES (1)",
        "DELETE FROM data",
        "UPDATE data SET x=1",
        "",
    })
    void unsupportedSql_throws(String sql) {
        assertThrows(SqlParser.SqlParseException.class, () -> parser.parse(sql));
    }
}
