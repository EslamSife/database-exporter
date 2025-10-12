package com.database.export.runtime.schema;

import com.database.export.runtime.config.ExportConfig;
import com.database.export.runtime.connection.DatabaseConnectionManager;
import com.database.export.runtime.metadata.TableMetadata;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Analyzes database schema with parallel processing for performance.
 */
public class SchemaAnalyzer {

  private static final Logger logger = Logger.getLogger(SchemaAnalyzer.class.getName());

  private final ExportConfig config;
  private final DatabaseConnectionManager connectionManager;
  private final TableAnalyzer tableAnalyzer;

  public SchemaAnalyzer(
      ExportConfig config,
      DatabaseConnectionManager connectionManager,
      TableAnalyzer tableAnalyzer) {
    this.config = config;
    this.connectionManager = connectionManager;
    this.tableAnalyzer = tableAnalyzer;
  }

  /**
   * Analyzes database schema with parallel processing.
   */
  public Map<String, TableMetadata> analyzeSchema()
      throws SQLException, InterruptedException, ExecutionException {
    logger.info("Analyzing database schema...");

    Connection connection = connectionManager.getConnection();
    DatabaseMetaData metaData = connection.getMetaData();

    List<String> tableNames = getAllUserTables(metaData);
    logger.info(String.format("Found %d tables to analyze", tableNames.size()));

    return analyzeTablesInParallel(tableNames);
  }

  private Map<String, TableMetadata> analyzeTablesInParallel(List<String> tableNames)
      throws InterruptedException, ExecutionException {

    ConcurrentHashMap<String, TableMetadata> tableMetadataMap = new ConcurrentHashMap<>();
    ExecutorService executor = Executors.newFixedThreadPool(config.parallelThreads());

    try {
      List<CompletableFuture<TableMetadata>> futures =
          tableNames.stream()
              .map(
                  tableName ->
                      CompletableFuture.supplyAsync(
                          () -> {
                            try {
                              return tableAnalyzer.analyzeTable(tableName);
                            } catch (SQLException e) {
                              logger.warning(
                                  "Failed to analyze table " + tableName + ": " + e.getMessage());
                              return null;
                            }
                          },
                          executor))
              .toList();

      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

      int successCount = 0;
      for (CompletableFuture<TableMetadata> future : futures) {
        TableMetadata metadata = future.get();
        if (metadata != null) {
          tableMetadataMap.put(metadata.tableName(), metadata);
          successCount++;

          if (successCount % 20 == 0) {
            logger.info(
                String.format("  Progress: %d/%d tables analyzed", successCount, tableNames.size()));
          }
        }
      }

      logger.info(
          String.format(
              "✓ Schema analysis complete: %d/%d tables successfully analyzed",
              successCount, tableNames.size()));

    } finally {
      executor.shutdown();
      executor.awaitTermination(5, TimeUnit.MINUTES);
    }

    return Map.copyOf(tableMetadataMap);
  }

  private List<String> getAllUserTables(DatabaseMetaData metaData) throws SQLException {
    List<String> tables = new ArrayList<>();

    try (ResultSet rs =
        metaData.getTables(config.dbName(), config.schemaName(), "%", new String[] {"TABLE"})) {
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

  private boolean isSystemTable(String tableName) {
    if (tableName == null) return true;

    String lowerName = tableName.toLowerCase();
    return lowerName.startsWith("sys")
        || lowerName.startsWith("msreplication")
        || lowerName.startsWith("spt_")
        || lowerName.startsWith("__")
        || lowerName.contains("$")
        || lowerName.equals("trace_xe_action_map")
        || lowerName.equals("trace_xe_event_map");
  }
}
