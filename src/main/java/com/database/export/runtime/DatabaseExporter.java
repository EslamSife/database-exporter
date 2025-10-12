package com.database.export.runtime;

import com.database.export.runtime.config.ExportConfig;
import com.database.export.runtime.connection.DatabaseConnectionManager;
import com.database.export.runtime.dependency.TableDependencyResolver;
import com.database.export.runtime.exporter.TableDataExporter;
import com.database.export.runtime.metadata.SortStrategy;
import com.database.export.runtime.metadata.TableMetadata;
import com.database.export.runtime.schema.SchemaAnalyzer;
import com.database.export.runtime.sql.SqlWriter;
import com.database.export.runtime.statistics.ExportStatistics;
import com.database.export.runtime.statistics.ReportGenerator;
import lombok.Getter;

import java.sql.Connection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Main database exporter that orchestrates the export process.
 * Coordinates schema analysis, dependency resolution, data export, and reporting.
 */
public class DatabaseExporter {

  private static final Logger logger = Logger.getLogger(DatabaseExporter.class.getName());
  private static final String SEPARATOR = "=".repeat(60);

  private final ExportConfig config;
  private final DatabaseConnectionManager connectionManager;
  private final SchemaAnalyzer schemaAnalyzer;
  private final TableDependencyResolver dependencyResolver;
  private final TableDataExporter tableDataExporter;
  private final SqlWriter sqlWriter;
  @Getter
  private final ExportStatistics statistics;
  private final ReportGenerator reportGenerator;

  public DatabaseExporter(
      ExportConfig config,
      DatabaseConnectionManager connectionManager,
      SchemaAnalyzer schemaAnalyzer,
      TableDependencyResolver dependencyResolver,
      TableDataExporter tableDataExporter,
      SqlWriter sqlWriter,
      ExportStatistics statistics,
      ReportGenerator reportGenerator) {
    this.config = config;
    this.connectionManager = connectionManager;
    this.schemaAnalyzer = schemaAnalyzer;
    this.dependencyResolver = dependencyResolver;
    this.tableDataExporter = tableDataExporter;
    this.sqlWriter = sqlWriter;
    this.statistics = statistics;
    this.reportGenerator = reportGenerator;
  }

  /**
   * Executes the complete database export process.
   */
  public void export() throws Exception {
    statistics.start();

    try {
      logExportStart();

      Connection connection = connectionManager.createConnection();
      Map<String, TableMetadata> tableMetadata = schemaAnalyzer.analyzeSchema();

      validateMetadata(tableMetadata);
      displayMetadataSummary(tableMetadata);

      sqlWriter.initialize(tableMetadata.size());
      exportAllTables(connection, tableMetadata);
      sqlWriter.close(tableMetadata.size());

      statistics.end();
      logExportComplete();

    } catch (Exception e) {
      logger.severe("Export failed: " + e.getMessage());
      throw e;
    } finally {
      connectionManager.closeConnection();
    }
  }

  public void generateReport() throws Exception {
    reportGenerator.generateReport();
  }

    private void logExportStart() {
    logger.info(SEPARATOR);
    logger.info("Database Export Starting");
    logger.info(SEPARATOR);
    logger.info("Database: " + config.dbName());
    logger.info("Row Limit: " + config.rowLimit() + " per table");
    logger.info("Parallel Threads: " + config.parallelThreads());
    logger.info("Batch Size: " + config.batchSize());
    logger.info(SEPARATOR);
  }

  private void exportAllTables(Connection connection, Map<String, TableMetadata> tableMetadata)
      throws Exception {
    logger.info("\nStarting data export...");

    List<TableMetadata> sortedTables = dependencyResolver.topologicalSort(tableMetadata);
    logExportOrder(sortedTables);

    int tableCount = 0;
    for (TableMetadata metadata : sortedTables) {
      tableCount++;
      exportSingleTable(metadata, connection, tableCount, sortedTables.size());
    }
  }

  private void logExportOrder(List<TableMetadata> sortedTables) {
    logger.info("Export order (maintaining referential integrity):");
    for (int i = 0; i < Math.min(10, sortedTables.size()); i++) {
      logger.info(String.format("  %d. %s", i + 1, sortedTables.get(i).tableName()));
    }
    if (sortedTables.size() > 10) {
      logger.info("  ... and " + (sortedTables.size() - 10) + " more tables");
    }
  }

  private void exportSingleTable(
      TableMetadata metadata, Connection connection, int tableCount, int totalTables)
      throws Exception {
    logger.info(
        String.format("\n[%d/%d] Exporting table: %s", tableCount, totalTables, metadata.tableName()));

    long rowsExported = tableDataExporter.exportTable(metadata, connection);
    statistics.recordTableExport(metadata.tableName(), rowsExported);

    logger.info(String.format("  ✓ Exported %,d rows from %s", rowsExported, metadata.tableName()));
  }

  private void logExportComplete() {
    logger.info(statistics.getSummary());
    logger.info("✓ Export completed successfully!");
  }

  private void validateMetadata(Map<String, TableMetadata> tableMetadataMap) {
    logger.info("\nValidating metadata...");

    int tablesWithoutPK = 0;
    int tablesWithoutSort = 0;
    int tablesWithCompositePK = 0;

    for (TableMetadata metadata : tableMetadataMap.values()) {
      if (!metadata.hasPrimaryKey()) {
        tablesWithoutPK++;
        logger.warning("Table without primary key: " + metadata.tableName());
      }

      if (metadata.sortStrategy() instanceof SortStrategy.NoSort) {
        tablesWithoutSort++;
      }

      if (metadata.hasCompositeKey()) {
        tablesWithCompositePK++;
      }
    }

    logger.info(
        String.format(
            """
            Validation Summary:
              - Tables without PK: %d
              - Tables without sort strategy: %d
              - Tables with composite PK: %d
            """,
            tablesWithoutPK, tablesWithoutSort, tablesWithCompositePK));

    if (tablesWithoutPK > 0) {
      logger.warning("⚠ Warning: " + tablesWithoutPK + " tables without primary keys!");
    }
  }

  private void displayMetadataSummary(Map<String, TableMetadata> tableMetadataMap) {
    logger.info("\n" + SEPARATOR);
    logger.info("Database Schema Metadata Summary");
    logger.info(SEPARATOR);
    logger.info(String.format("Total Tables: %d", tableMetadataMap.size()));

    Map<String, List<TableMetadata>> tablesByFKCount =
        tableMetadataMap.values().stream()
            .collect(
                Collectors.groupingBy(
                    t -> {
                      int fkCount = t.foreignKeys().size();
                      if (fkCount == 0) return "No FK";
                      if (fkCount <= 2) return "1-2 FKs";
                      if (fkCount <= 5) return "3-5 FKs";
                      return "6+ FKs";
                    }));

    logger.info("\nTables by Foreign Key Count:");
    tablesByFKCount.forEach(
        (category, tables) ->
            logger.info(String.format("  %s: %d tables", category, tables.size())));

    logger.info("\nTop 10 Most Complex Tables:");
    tableMetadataMap.values().stream()
        .sorted(Comparator.comparingInt((TableMetadata t) -> t.foreignKeys().size()).reversed())
        .limit(10)
        .forEach(
            t ->
                logger.info(
                    String.format(
                        "  %-40s - %d FKs, %d cols",
                        t.tableName(), t.foreignKeys().size(), t.columns().size())));

    logger.info(SEPARATOR);
  }
}
