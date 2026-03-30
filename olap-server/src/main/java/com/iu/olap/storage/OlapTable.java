package com.iu.olap.storage;

import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An OLAP table composed of multiple ColumnStore micro-partitions.
 *
 * SNOWFLAKE TABLE ARCHITECTURE
 * ────────────────────────────
 * A Snowflake table is logically a single table but physically stored as
 * thousands of immutable micro-partitions spread across cloud object storage
 * (S3/Azure Blob/GCS). Each micro-partition is:
 *   - Compressed with LZ4 or Zstd (5–10x compression typical)
 *   - Encrypted at rest
 *   - Independently replaceable (INSERT creates new partitions; old ones
 *     are deleted by micro-partition reclustering)
 *
 * This class manages the collection of ColumnStore instances representing
 * micro-partitions and adds:
 *   1. Partition pruning via min/max metadata (skipPartitionIfPossible)
 *   2. A simulated Bloom filter per column per partition (OlapBloomFilter)
 *   3. Clustered-key awareness for partition selection
 */
public class OlapTable {
    private static final Logger LOGGER = Logger.getLogger(OlapTable.class.getName());

    private static final int MAX_ROWS_PER_PARTITION = 100; // small for testing

    private final String       tableName;
    private final List<String> schema;
    private final List<ColumnStore> partitions = new ArrayList<>();
    private       ColumnStore  currentPartition;
    private       String       clusteredKeyColumn = null;

    /** Bloom filter per partition per column for fast "definitely absent" checks. */
    private final Map<String, OlapBloomFilter> bloomFilters = new HashMap<>();

    public OlapTable(String tableName, List<String> schema) {
        this.tableName = tableName;
        this.schema    = List.copyOf(schema);
        newPartition();
    }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    public void insert(List<Object> row) {
        if (currentPartition.rowCount() >= MAX_ROWS_PER_PARTITION) {
            sealAndRecluster(currentPartition);
            newPartition();
        }
        currentPartition.insertRow(row);
        // Update Bloom filters for each column value
        for (int i = 0; i < schema.size(); i++) {
            bloomFilter(currentPartition.partitionId(), schema.get(i))
                .add(row.get(i));
        }
    }

    /** Define the clustered key for this table (applied on all future seals). */
    public void setClusteredKey(String column) {
        this.clusteredKeyColumn = column;
        LOGGER.info("Table " + tableName + " clustered on: " + column);
    }

    // -----------------------------------------------------------------------
    // Read with partition pruning
    // -----------------------------------------------------------------------

    /**
     * Scan the table, returning rows matching the predicate.
     *
     * Uses three levels of optimisation:
     *  1. Bloom filter check — O(k) bit ops, "definitely absent" → skip partition
     *  2. Min/max pruning   — O(1) compare, out-of-range → skip partition
     *  3. Full column scan  — only reaches here if both above say "maybe present"
     *
     * @param filterColumn  column to apply the equality filter on (null = no filter)
     * @param filterValue   value to match (null = return all rows)
     * @param selectColumns columns to include in result
     */
    public List<Map<String, Object>> scan(String filterColumn, Object filterValue,
                                          List<String> selectColumns) {
        sealCurrentPartition(); // ensure current partition's stats are up-to-date

        List<Map<String, Object>> result = new ArrayList<>();
        int totalPartitions = partitions.size();
        int scanned = 0;
        int bloomPruned = 0;
        int minMaxPruned = 0;

        for (ColumnStore partition : partitions) {
            if (filterColumn != null && filterValue != null) {

                // Level 1: Bloom filter — O(k) ops
                OlapBloomFilter bf = bloomFilters.get(
                    bloomKey(partition.partitionId(), filterColumn));
                if (bf != null && !bf.mightContain(filterValue)) {
                    bloomPruned++;
                    continue; // definitely not in this partition
                }

                // Level 2: min/max — O(1)
                if (partition.canPrune(filterColumn, filterValue)) {
                    minMaxPruned++;
                    continue; // value outside partition range
                }
            }

            scanned++;
            // Level 3: actual columnar scan
            List<Map<String, Object>> rows = partition.scanAll(selectColumns);
            if (filterColumn != null && filterValue != null) {
                for (Map<String, Object> row : rows) {
                    if (filterValue.equals(row.get(filterColumn))) result.add(row);
                }
            } else {
                result.addAll(rows);
            }
        }

        LOGGER.log(Level.INFO, String.format(
            "Table %s scan: total=%d bloom_pruned=%d minmax_pruned=%d scanned=%d rows_returned=%d",
            tableName, totalPartitions, bloomPruned, minMaxPruned, scanned, result.size()));

        return result;
    }

    /** Read a single column's values across all partitions — pure columnar I/O. */
    public List<Object> columnScan(String column) {
        sealCurrentPartition();
        List<Object> values = new ArrayList<>();
        for (ColumnStore p : partitions) values.addAll(p.readColumn(column));
        return values;
    }

    /** Count rows matching a predicate without reading other columns. */
    public long countWhere(String column, Predicate<Object> predicate) {
        return columnScan(column).stream().filter(predicate).count();
    }

    public String      tableName()   { return tableName; }
    public List<String> schema()     { return schema; }
    public int         partitionCount() { sealCurrentPartition(); return partitions.size(); }
    public long        totalRows()   { return partitions.stream().mapToLong(ColumnStore::rowCount).sum(); }
    public List<ColumnStore> partitions() { sealCurrentPartition(); return Collections.unmodifiableList(partitions); }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    private void newPartition() {
        String pid = tableName + "_p" + partitions.size();
        currentPartition = new ColumnStore(pid, schema);
    }

    private void sealAndRecluster(ColumnStore p) {
        if (clusteredKeyColumn != null) {
            p.clusterBy(clusteredKeyColumn);
        }
        partitions.add(p);
    }

    private void sealCurrentPartition() {
        if (currentPartition != null && currentPartition.rowCount() > 0) {
            sealAndRecluster(currentPartition);
            currentPartition = new ColumnStore(tableName + "_p" + partitions.size(), schema);
        }
    }

    private OlapBloomFilter bloomFilter(String partitionId, String column) {
        return bloomFilters.computeIfAbsent(
            bloomKey(partitionId, column), k -> new OlapBloomFilter(MAX_ROWS_PER_PARTITION, 0.01));
    }

    private static String bloomKey(String partitionId, String column) {
        return partitionId + ":" + column;
    }
}
