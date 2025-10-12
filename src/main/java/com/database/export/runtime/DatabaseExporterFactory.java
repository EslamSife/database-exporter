package com.database.export.runtime;

import com.database.export.runtime.config.ExportConfig;
import com.database.export.runtime.connection.DatabaseConnectionManager;
import com.database.export.runtime.dependency.TableDependencyResolver;
import com.database.export.runtime.exporter.TableDataExporter;
import com.database.export.runtime.logging.ExportLogger;
import com.database.export.runtime.schema.MetadataExtractor;
import com.database.export.runtime.schema.SchemaAnalyzer;
import com.database.export.runtime.schema.SortStrategyResolver;
import com.database.export.runtime.schema.TableAnalyzer;
import com.database.export.runtime.sql.SqlStatementGenerator;
import com.database.export.runtime.sql.SqlValueFormatter;
import com.database.export.runtime.sql.SqlWriter;
import com.database.export.runtime.statistics.ExportStatistics;
import com.database.export.runtime.statistics.ReportGenerator;

/**
 * Factory for creating DatabaseExporter instances with all required dependencies.
 */
public class DatabaseExporterFactory {

  /**
   * Creates a fully configured DatabaseExporter.
   */
  public static DatabaseExporter create(ExportConfig config) throws Exception {
    ExportLogger.configure(config.outputDirectory());

    DatabaseConnectionManager connectionManager = new DatabaseConnectionManager(config);
    SchemaAnalyzer schemaAnalyzer = createSchemaAnalyzer(config, connectionManager);
    TableDependencyResolver dependencyResolver = new TableDependencyResolver();

    SqlValueFormatter valueFormatter = new SqlValueFormatter();
    SqlStatementGenerator sqlGenerator = new SqlStatementGenerator(valueFormatter);
    SqlWriter sqlWriter = new SqlWriter(config);
    TableDataExporter tableDataExporter = new TableDataExporter(config, sqlGenerator, sqlWriter);

    ExportStatistics statistics = new ExportStatistics();
    ReportGenerator reportGenerator = new ReportGenerator(config, statistics);

    return new DatabaseExporter(
        config,
        connectionManager,
        schemaAnalyzer,
        dependencyResolver,
        tableDataExporter,
        sqlWriter,
        statistics,
        reportGenerator);
  }

  private static SchemaAnalyzer createSchemaAnalyzer(
      ExportConfig config, DatabaseConnectionManager connectionManager) {
    MetadataExtractor metadataExtractor = new MetadataExtractor(config);
    SortStrategyResolver sortStrategyResolver = new SortStrategyResolver();

    TableAnalyzer tableAnalyzer =
        new TableAnalyzer(config, connectionManager, metadataExtractor, sortStrategyResolver);

    return new SchemaAnalyzer(config, connectionManager, tableAnalyzer);
  }
}
