package com.iu.olap.catalog;

import com.iu.olap.storage.OlapTable;

import java.util.*;

/**
 * Snowflake-style Information Schema / Catalog.
 *
 * In Snowflake the catalog is called the "Information Schema" and stores
 * metadata about databases, schemas, tables, columns, and clustering keys.
 * Queries like SHOW TABLES, DESCRIBE TABLE, and SELECT * FROM INFORMATION_SCHEMA.TABLES
 * all go through this layer.
 *
 * This simulation tracks table registrations and their schemas so the OLAP
 * query engine can look up column types and plan joins.
 *
 * Snowflake three-level namespace: database.schema.table
 * Simplified here to: schemaName.tableName
 */
public class OlapCatalog {

    /** Table entry holding the live OlapTable and its registered schema. */
    public record TableEntry(OlapTable table, List<ColumnDef> columns, String clusteredKey) {
        public Optional<ColumnDef> column(String name) {
            return columns.stream().filter(c -> c.name().equals(name)).findFirst();
        }
    }

    /** Column definition — name, type, and whether it is the join/cluster key. */
    public record ColumnDef(String name, ColumnType type) {}

    public enum ColumnType { INT, STRING, DOUBLE, DATE }

    // Registry: qualified name "schema.table" → entry
    private final Map<String, TableEntry> registry = new LinkedHashMap<>();

    // -----------------------------------------------------------------------
    // DDL operations
    // -----------------------------------------------------------------------

    /**
     * Register an OlapTable with its schema definition.
     * In Snowflake this corresponds to CREATE TABLE.
     */
    public void registerTable(String schemaName, String tableName,
                               OlapTable table, List<ColumnDef> columns) {
        String key = qualify(schemaName, tableName);
        registry.put(key, new TableEntry(table, List.copyOf(columns), null));
    }

    /**
     * Set the clustered key for a table.
     * In Snowflake: ALTER TABLE t CLUSTER BY (column)
     */
    public void setClusteredKey(String schemaName, String tableName, String column) {
        String key  = qualify(schemaName, tableName);
        TableEntry e = registry.get(key);
        if (e == null) throw new NoSuchElementException("Table not found: " + key);
        registry.put(key, new TableEntry(e.table(), e.columns(), column));
        e.table().setClusteredKey(column);
    }

    // -----------------------------------------------------------------------
    // Lookup
    // -----------------------------------------------------------------------

    public Optional<TableEntry> getTable(String schemaName, String tableName) {
        return Optional.ofNullable(registry.get(qualify(schemaName, tableName)));
    }

    public OlapTable requireTable(String schemaName, String tableName) {
        return getTable(schemaName, tableName)
            .map(TableEntry::table)
            .orElseThrow(() -> new NoSuchElementException(
                "Table not registered: " + qualify(schemaName, tableName)));
    }

    /** List all registered table names in a schema. */
    public List<String> listTables(String schemaName) {
        String prefix = schemaName + ".";
        return registry.keySet().stream()
            .filter(k -> k.startsWith(prefix))
            .map(k -> k.substring(prefix.length()))
            .toList();
    }

    /** Total number of registered tables. */
    public int tableCount() { return registry.size(); }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    private static String qualify(String schema, String table) {
        return schema.toLowerCase() + "." + table.toLowerCase();
    }
}
