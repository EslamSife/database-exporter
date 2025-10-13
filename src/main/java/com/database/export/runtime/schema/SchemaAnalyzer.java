package com.database.export.runtime.schema;


import com.database.export.runtime.config.ExportConfig;
import com.database.export.runtime.connection.DatabaseConnectionManager;
import com.database.export.runtime.metadata.ColumnInfo;
import com.database.export.runtime.metadata.ForeignKeyInfo;
import com.database.export.runtime.metadata.SortStrategy;
import com.database.export.runtime.metadata.TableMetadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;


public class SchemaAnalyzer {

    private static final Logger logger = Logger.getLogger(SchemaAnalyzer.class.getName());

    private final ExportConfig config;
    private final DatabaseConnectionManager connectionManager;
    private final MetadataExtractor metadataExtractor;
    private final SortStrategyResolver sortStrategyResolver;

    public SchemaAnalyzer(
            ExportConfig config,
            DatabaseConnectionManager connectionManager,
            MetadataExtractor metadataExtractor,
            SortStrategyResolver sortStrategyResolver) {
        this.config = config;
        this.connectionManager = connectionManager;
        this.metadataExtractor = metadataExtractor;
        this.sortStrategyResolver = sortStrategyResolver;
    }

    /**
     * Analyzes database schema using optimized bulk queries.
     */
    public Map<String, TableMetadata> analyzeSchema() throws SQLException {
        logger.info("========================================");
        logger.info("Starting OPTIMIZED schema analysis...");
        logger.info("========================================");

        long startTime = System.currentTimeMillis();

        Connection connection = connectionManager.getConnection();

        // Step 1: Get list of all user tables (1 query)
        List<String> tableNames = getAllUserTables(connection);
        logger.info(String.format("Found %d tables to analyze", tableNames.size()));

        // Step 2: Extract ALL metadata in bulk (4 queries total)
        MetadataExtractor.BulkMetadata bulkMetadata =
                metadataExtractor.extractAllMetadata(connection);

        // Step 3: Build TableMetadata objects from bulk data (in-memory processing)
        Map<String, TableMetadata> tableMetadataMap = buildTableMetadata(tableNames, bulkMetadata);

        long duration = System.currentTimeMillis() - startTime;

        logger.info("========================================");
        logger.info(String.format("✅ Schema analysis complete in %.2f seconds", duration / 1000.0));
        logger.info(String.format("   Total tables analyzed: %d", tableMetadataMap.size()));
        logger.info(String.format("   Speed: ~%.1f tables/second",
                tableMetadataMap.size() / (duration / 1000.0)));
        logger.info("========================================");

        return tableMetadataMap;
    }

    /**
     * Get all user tables from database (1 query)
     */
    private List<String> getAllUserTables(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();
        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet rs = metaData.getTables(
                config.dbName(),
                config.schemaName(),
                "%",
                new String[]{"TABLE"})) {

            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");

                if (!config.includeSystemTables() && isSystemTable(tableName)) {
                    continue;
                }

                tables.add(tableName);
            }
        }

        return tables;
    }

    /**
     * Build TableMetadata objects from bulk metadata (in-memory, no DB queries)
     */
    private Map<String, TableMetadata> buildTableMetadata(
            List<String> tableNames,
            MetadataExtractor.BulkMetadata bulkMetadata) {

        logger.info("Building TableMetadata objects from bulk data...");

        Map<String, TableMetadata> result = new LinkedHashMap<>();
        int processed = 0;

        for (String tableName : tableNames) {
            processed++;

            // All data is already fetched - just retrieve from maps
            List<String> primaryKeys = bulkMetadata.getPrimaryKeys(tableName);
            List<ForeignKeyInfo> foreignKeys = bulkMetadata.getForeignKeys(tableName);
            List<ColumnInfo> columns = bulkMetadata.getColumns(tableName);
            long rowCount = bulkMetadata.getRowCount(tableName);

            // Determine sort strategy
            SortStrategy sortStrategy = sortStrategyResolver.determineSortStrategy(
                    tableName, primaryKeys, columns);

            // Create TableMetadata
            TableMetadata metadata = new TableMetadata(
                    tableName,
                    config.schemaName(),
                    primaryKeys,
                    foreignKeys,
                    columns,
                    sortStrategy,
                    rowCount,
                    primaryKeys.size() > 1
            );

            result.put(tableName, metadata);

            // Log progress every 100 tables
            if (processed % 100 == 0) {
                logger.info(String.format("  Progress: %d/%d tables processed", processed, tableNames.size()));
            }
        }

        logger.info(String.format("✓ Built metadata for %d tables", result.size()));
        return result;
    }

    /**
     * Check if table is a system table
     */
    private boolean isSystemTable(String tableName) {
        if (tableName == null) return true;
        String lowerName = tableName.toLowerCase();
        return lowerName.startsWith("sys")
                || lowerName.startsWith("msreplication")
                || lowerName.startsWith("spt_")
                || lowerName.startsWith("__")
                || lowerName.contains("$")
                || lowerName.equals("trace_xe_action_map")
                || lowerName.equals("trace_xe_event_map")
                || lowerName.equals("sysdiagrams");
    }
}