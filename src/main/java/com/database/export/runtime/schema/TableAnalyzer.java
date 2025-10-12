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
import java.sql.Statement;
import java.util.List;

/**
 * Analyzes a single table and returns its complete metadata.
 */
public class TableAnalyzer {

  private final ExportConfig config;
  private final DatabaseConnectionManager connectionManager;
  private final MetadataExtractor metadataExtractor;
  private final SortStrategyResolver sortStrategyResolver;

  public TableAnalyzer(
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
   * Analyzes a single table and returns its metadata.
   */
  public TableMetadata analyzeTable(String tableName) throws SQLException {
    try (Connection conn = connectionManager.createNewConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();

      List<String> primaryKeys = metadataExtractor.extractPrimaryKeys(metaData, tableName);
      List<ForeignKeyInfo> foreignKeys = metadataExtractor.extractForeignKeys(metaData, tableName);
      List<ColumnInfo> columns = metadataExtractor.extractColumns(metaData, tableName);

      SortStrategy sortStrategy =
          sortStrategyResolver.determineSortStrategy(tableName, primaryKeys, columns);

      long rowCount = estimateRowCount(conn, tableName);

      return new TableMetadata(
          tableName,
          config.schemaName(),
          primaryKeys,
          foreignKeys,
          columns,
          sortStrategy,
          rowCount,
          primaryKeys.size() > 1);
    }
  }

  private long estimateRowCount(Connection conn, String tableName) {
    String query =
        String.format(
            "SELECT CAST(SUM(p.rows) AS BIGINT) AS row_count "
                + "FROM sys.tables t "
                + "INNER JOIN sys.partitions p ON t.object_id = p.object_id "
                + "WHERE t.name = '%s' AND p.index_id IN (0, 1) "
                + "GROUP BY t.name",
            tableName.replace("'", "''"));

    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {

      if (rs.next()) {
        return rs.getLong("row_count");
      }
    } catch (SQLException e) {
      // Ignore and return -1
    }

    return -1;
  }
}
