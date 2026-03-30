package com.iu.sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class SqlParserTest {

    private final SqlParser parser = new SqlParser();

    @Test
    void parse_selectAll() {
        var stmt = parser.parse("SELECT * FROM data");
        assertEquals("data", stmt.table());
        assertNull(stmt.where());
        assertEquals(-1, stmt.limit());
    }

    @Test
    void parse_whereId() {
        var stmt = parser.parse("SELECT * FROM data WHERE id = 42");
        assertNotNull(stmt.where());
        assertEquals("id",  stmt.where().predicate().field());
        assertEquals("42",  stmt.where().predicate().value());
    }

    @Test
    void parse_whereStringValue() {
        var stmt = parser.parse("SELECT * FROM data WHERE data = 'testdata5'");
        assertEquals("data",      stmt.where().predicate().field());
        assertEquals("testdata5", stmt.where().predicate().value());
    }

    @Test
    void parse_withLimit() {
        var stmt = parser.parse("SELECT * FROM data LIMIT 10");
        assertEquals(10, stmt.limit());
        assertNull(stmt.where());
    }

    @Test
    void parse_fullQuery() {
        var stmt = parser.parse("SELECT * FROM data WHERE id = 5 LIMIT 1");
        assertEquals("id",  stmt.where().predicate().field());
        assertEquals("5",   stmt.where().predicate().value());
        assertEquals(1,     stmt.limit());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT * FROM data;",          // semicolon ok
        "select * from DATA where id = 1", // case-insensitive
        "SELECT  *  FROM  data  WHERE  id  =  7", // extra spaces
    })
    void parse_toleratesFormatVariants(String sql) {
        assertDoesNotThrow(() -> parser.parse(sql));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "INSERT INTO data VALUES (1)",
        "DELETE FROM data",
        "UPDATE data SET x=1",
        "",
    })
    void parse_unsupportedSql_throws(String sql) {
        assertThrows(SqlParser.SqlParseException.class, () -> parser.parse(sql));
    }
}
