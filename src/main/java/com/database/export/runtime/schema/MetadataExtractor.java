package com.database.export.runtime.schema;

import com.database.export.runtime.config.ExportConfig;
import com.database.export.runtime.metadata.ColumnInfo;
import com.database.export.runtime.metadata.ForeignKeyInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


public class MetadataExtractor {

    private static final Logger logger = Logger.getLogger(MetadataExtractor.class.getName());

    private final ExportConfig config;

    public MetadataExtractor(ExportConfig config) {
        this.config = config;
    }

    /**
     * Container for all database metadata
     */
    public static class BulkMetadata {
        private final Map<String, List<String>> primaryKeys;
        private final Map<String, List<ForeignKeyInfo>> foreignKeys;
        private final Map<String, List<ColumnInfo>> columns;
        private final Map<String, Long> rowCounts;

        public BulkMetadata(
                Map<String, List<String>> primaryKeys,
                Map<String, List<ForeignKeyInfo>> foreignKeys,
                Map<String, List<ColumnInfo>> columns,
                Map<String, Long> rowCounts) {
            this.primaryKeys = primaryKeys;
            this.foreignKeys = foreignKeys;
            this.columns = columns;
            this.rowCounts = rowCounts;
        }

        public List<String> getPrimaryKeys(String tableName) {
            return primaryKeys.getOrDefault(tableName, Collections.emptyList());
        }

        public List<ForeignKeyInfo> getForeignKeys(String tableName) {
            return foreignKeys.getOrDefault(tableName, Collections.emptyList());
        }

        public List<ColumnInfo> getColumns(String tableName) {
            return columns.getOrDefault(tableName, Collections.emptyList());
        }

        public long getRowCount(String tableName) {
            return rowCounts.getOrDefault(tableName, -1L);
        }
    }

    /**
     * Extracts ALL metadata in bulk (4 queries total instead of 3,200+)
     */
    public BulkMetadata extractAllMetadata(Connection connection) throws SQLException {
        logger.info("🚀 Starting OPTIMIZED bulk metadata extraction...");
        long startTime = System.currentTimeMillis();

        Map<String, List<String>> primaryKeys = extractAllPrimaryKeys(connection);
        Map<String, List<ForeignKeyInfo>> foreignKeys = extractAllForeignKeys(connection);
        Map<String, List<ColumnInfo>> columns = extractAllColumns(connection);
        Map<String, Long> rowCounts = extractAllRowCounts(connection);

        long duration = System.currentTimeMillis() - startTime;
        logger.info(String.format("✅ Bulk metadata extraction complete in %d ms", duration));
        logger.info(String.format("   - Primary Keys: %d tables", primaryKeys.size()));
        logger.info(String.format("   - Foreign Keys: %d tables", foreignKeys.size()));
        logger.info(String.format("   - Columns: %d tables", columns.size()));
        logger.info(String.format("   - Row Counts: %d tables", rowCounts.size()));

        return new BulkMetadata(primaryKeys, foreignKeys, columns, rowCounts);
    }

    /**
     * Extract ALL primary keys in ONE query
     */
    private Map<String, List<String>> extractAllPrimaryKeys(Connection connection) throws SQLException {
        logger.info("  → Extracting all primary keys (1 query)...");

        String query = """
            SELECT 
                tc.TABLE_NAME,
                kcu.COLUMN_NAME,
                kcu.ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND tc.TABLE_SCHEMA = ?
            ORDER BY tc.TABLE_NAME, kcu.ORDINAL_POSITION
            """;

        Map<String, List<String>> result = new HashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, config.schemaName());

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    String columnName = rs.getString("COLUMN_NAME");

                    result.computeIfAbsent(tableName, k -> new ArrayList<>()).add(columnName);
                }
            }
        }

        logger.info(String.format("    ✓ Found primary keys for %d tables", result.size()));
        return result;
    }

    /**
     * Extract ALL foreign keys in ONE query
     */
    private Map<String, List<ForeignKeyInfo>> extractAllForeignKeys(Connection connection) throws SQLException {
        logger.info("  → Extracting all foreign keys (1 query)...");

        String query = """
            SELECT 
                fk.name AS FK_NAME,
                OBJECT_NAME(fk.parent_object_id) AS TABLE_NAME,
                COL_NAME(fkc.parent_object_id, fkc.parent_column_id) AS COLUMN_NAME,
                SCHEMA_NAME(ref_tab.schema_id) AS REFERENCED_SCHEMA,
                ref_tab.name AS REFERENCED_TABLE,
                COL_NAME(fkc.referenced_object_id, fkc.referenced_column_id) AS REFERENCED_COLUMN,
                fkc.constraint_column_id AS KEY_SEQUENCE
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc 
                ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables tab 
                ON fk.parent_object_id = tab.object_id
            INNER JOIN sys.tables ref_tab 
                ON fkc.referenced_object_id = ref_tab.object_id
            WHERE SCHEMA_NAME(tab.schema_id) = ?
            ORDER BY OBJECT_NAME(fk.parent_object_id), fkc.constraint_column_id
            """;

        Map<String, List<ForeignKeyInfo>> result = new HashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, config.schemaName());

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");

                    ForeignKeyInfo fkInfo = new ForeignKeyInfo(
                            rs.getString("FK_NAME"),
                            rs.getString("COLUMN_NAME"),
                            rs.getString("REFERENCED_SCHEMA"),
                            rs.getString("REFERENCED_TABLE"),
                            rs.getString("REFERENCED_COLUMN"),
                            rs.getInt("KEY_SEQUENCE")
                    );

                    result.computeIfAbsent(tableName, k -> new ArrayList<>()).add(fkInfo);
                }
            }
        }

        logger.info(String.format("    ✓ Found foreign keys for %d tables", result.size()));
        return result;
    }

    /**
     * Extract ALL columns in ONE query
     */
    private Map<String, List<ColumnInfo>> extractAllColumns(Connection connection) throws SQLException {
        logger.info("  → Extracting all columns (1 query)...");

        String query = """
            SELECT 
                c.TABLE_NAME,
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH AS COLUMN_SIZE,
                c.IS_NULLABLE,
                c.ORDINAL_POSITION,
                CASE 
                    WHEN COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') = 1 
                    THEN 'YES' 
                    ELSE 'NO' 
                END AS IS_AUTOINCREMENT
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE c.TABLE_SCHEMA = ?
            ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION
            """;

        Map<String, List<ColumnInfo>> result = new HashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, config.schemaName());

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");

                    ColumnInfo columnInfo = new ColumnInfo(
                            rs.getString("COLUMN_NAME"),
                            mapDataTypeToJdbcType(rs.getString("DATA_TYPE")),
                            rs.getString("DATA_TYPE"),
                            rs.getInt("COLUMN_SIZE"),
                            "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")),
                            "YES".equalsIgnoreCase(rs.getString("IS_AUTOINCREMENT")),
                            rs.getInt("ORDINAL_POSITION")
                    );

                    result.computeIfAbsent(tableName, k -> new ArrayList<>()).add(columnInfo);
                }
            }
        }

        logger.info(String.format("    ✓ Found columns for %d tables", result.size()));
        return result;
    }

    /**
     * Extract ALL row counts in ONE query
     */
    private Map<String, Long> extractAllRowCounts(Connection connection) throws SQLException {
        logger.info("  → Extracting all row counts (1 query)...");

        String query = """
            SELECT 
                t.name AS TABLE_NAME,
                SUM(p.rows) AS ROW_COUNT
            FROM sys.tables t
            INNER JOIN sys.partitions p 
                ON t.object_id = p.object_id
            INNER JOIN sys.schemas s 
                ON t.schema_id = s.schema_id
            WHERE p.index_id IN (0, 1)  -- Heap or clustered index
                AND s.name = ?
            GROUP BY t.name
            ORDER BY t.name
            """;

        Map<String, Long> result = new HashMap<>();

        try (PreparedStatement ps = connection.prepareStatement(query)) {
            ps.setString(1, config.schemaName());

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    long rowCount = rs.getLong("ROW_COUNT");

                    result.put(tableName, rowCount);
                }
            }
        }

        logger.info(String.format("    ✓ Found row counts for %d tables", result.size()));
        return result;
    }

    /**
     * Map SQL Server data type names to JDBC Types constants
     */
    private int mapDataTypeToJdbcType(String dataType) {
        return switch (dataType.toLowerCase()) {
            case "varchar", "char" -> java.sql.Types.VARCHAR;
            case "nvarchar", "nchar" -> java.sql.Types.NVARCHAR;
            case "text" -> java.sql.Types.LONGVARCHAR;
            case "ntext" -> java.sql.Types.LONGNVARCHAR;
            case "int" -> java.sql.Types.INTEGER;
            case "bigint" -> java.sql.Types.BIGINT;
            case "smallint" -> java.sql.Types.SMALLINT;
            case "tinyint" -> java.sql.Types.TINYINT;
            case "bit" -> java.sql.Types.BIT;
            case "decimal", "numeric" -> java.sql.Types.DECIMAL;
            case "money", "smallmoney" -> java.sql.Types.DECIMAL;
            case "float" -> java.sql.Types.FLOAT;
            case "real" -> java.sql.Types.REAL;
            case "date" -> java.sql.Types.DATE;
            case "time" -> java.sql.Types.TIME;
            case "datetime", "datetime2", "smalldatetime" -> java.sql.Types.TIMESTAMP;
            case "datetimeoffset" -> java.sql.Types.TIMESTAMP_WITH_TIMEZONE;
            case "binary", "varbinary" -> java.sql.Types.VARBINARY;
            case "image" -> java.sql.Types.LONGVARBINARY;
            case "uniqueidentifier" -> java.sql.Types.CHAR;
            case "xml" -> java.sql.Types.SQLXML;
            default -> java.sql.Types.VARCHAR;
        };
    }
}