package com.database.export.runtime;

import com.database.export.runtime.config.ExportConfig;
import com.database.export.runtime.config.ExportConfigBuilder;
import com.database.export.runtime.config.ExporterProperties;
import com.database.export.runtime.connection.ConnectionPool;
import com.database.export.runtime.connection.DatabaseConnectionManager;
import com.database.export.runtime.exporter.TableDataExporter;
import com.database.export.runtime.filter.SmartTableFilter;
import com.database.export.runtime.logging.ExportLogger;
import com.database.export.runtime.metadata.TableMetadata;
import com.database.export.runtime.parallel.DependencyLevelParallelExporter;
import com.database.export.runtime.schema.MetadataExtractor;
import com.database.export.runtime.schema.SchemaAnalyzer;
import com.database.export.runtime.schema.SortStrategyResolver;
import com.database.export.runtime.sql.SqlStatementGenerator;
import com.database.export.runtime.sql.SqlValueFormatter;
import com.database.export.runtime.sql.SqlWriter;
import com.database.export.runtime.statistics.ExportStatistics;
import com.database.export.runtime.statistics.ReportGenerator;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;


@Service
public class DatabaseExportService {

    private final ExporterProperties properties;

    public DatabaseExportService(ExporterProperties properties) {
        this.properties = properties;
    }

    /**
     * Main export execution method.
     */
    public void executeExport() throws Exception {
        System.out.println("Initializing export from YAML configuration...\n");

        printConfiguration();

        long overallStartTime = System.currentTimeMillis();

        // Convert Spring properties to ExportConfig
        ExportConfig config = buildExportConfig();

        // Configure logging
        ExportLogger.configure(config.outputDirectory());

        // Execute export
        performOptimizedExport(config, overallStartTime);
    }

    /**
     * Builds ExportConfig from Spring ExporterProperties.
     */
    private ExportConfig buildExportConfig() {
        ExporterProperties.DatabaseConfig dbConfig = properties.getDatabase();
        ExporterProperties.ExportConfig exportConfig = properties.getExport();
        ExporterProperties.ParallelConfig parallelConfig = properties.getParallel();

        return new ExportConfigBuilder()
                .dbHost(dbConfig.getHost())
                .dbPort(String.valueOf(dbConfig.getPort()))
                .dbName(dbConfig.getName())
                .dbUser(dbConfig.getUsername())
                .dbPassword(dbConfig.getPassword())
                .schemaName(dbConfig.getSchema())
                .rowLimit(exportConfig.getRowLimit())
                .outputDirectory(exportConfig.getOutputDirectory())
                .parallelThreads(parallelConfig.getThreads())
                .batchSize(exportConfig.getBatchSize())
                .generateCreateStatements(exportConfig.isGenerateCreateStatements())
                .generateDropStatements(exportConfig.isGenerateDropStatements())
                .build();
    }

    /**
     * Performs the optimized export.
     */
    private void performOptimizedExport(ExportConfig config, long overallStartTime) throws Exception {
        // Phase 1: Initialize connections
        System.out.println("Phase 1: Initializing connections...");
        long phase1Start = System.currentTimeMillis();

        DatabaseConnectionManager connManager = new DatabaseConnectionManager(config);
        connManager.createConnection();

        ConnectionPool connectionPool = new ConnectionPool(config, config.parallelThreads());

        System.out.printf("✓ Connections ready (1 main + %d pooled) in %.2f seconds\n\n",
                config.parallelThreads(), (System.currentTimeMillis() - phase1Start) / 1000.0);

        try {
            // Phase 2: Schema analysis
            System.out.println("Phase 2: Analyzing database schema...");
            long phase2Start = System.currentTimeMillis();

            MetadataExtractor metadataExtractor = new MetadataExtractor(config);
            SortStrategyResolver sortStrategyResolver = new SortStrategyResolver();
            SchemaAnalyzer schemaAnalyzer = new SchemaAnalyzer(
                    config, connManager, metadataExtractor, sortStrategyResolver);

            Map<String, TableMetadata> allTables = schemaAnalyzer.analyzeSchema();

            double phase2Duration = (System.currentTimeMillis() - phase2Start) / 1000.0;
            System.out.printf("✓ Schema analyzed in %.2f seconds\n\n", phase2Duration);

            // Phase 3: Filter tables
            System.out.println("Phase 3: Filtering tables...");
            long phase3Start = System.currentTimeMillis();

            SmartTableFilter tableFilter = new SmartTableFilter(config, properties.getFilter());
            List<TableMetadata> filteredTables = tableFilter.filterTables(allTables);

            double phase3Duration = (System.currentTimeMillis() - phase3Start) / 1000.0;
            System.out.printf("✓ Tables filtered in %.2f seconds\n\n", phase3Duration);

            // Phase 4: Initialize export components
            System.out.println("Phase 4: Initializing export components...");

            SqlValueFormatter valueFormatter = new SqlValueFormatter();
            SqlStatementGenerator sqlGenerator = new SqlStatementGenerator(valueFormatter);
            SqlWriter sqlWriter = new SqlWriter(config);
            sqlWriter.initialize(filteredTables.size());

            TableDataExporter tableExporter = new TableDataExporter(
                    config, sqlGenerator, sqlWriter);

            System.out.println("✓ Export components ready\n");

            // Phase 5: Parallel data export
            System.out.println("Phase 5: Exporting data in parallel...");
            long phase5Start = System.currentTimeMillis();

            DependencyLevelParallelExporter parallelExporter = new DependencyLevelParallelExporter(
                    config, connectionPool, tableExporter);

            Map<String, Long> exportResults = parallelExporter.exportInParallel(filteredTables);

            double phase5Duration = (System.currentTimeMillis() - phase5Start) / 1000.0;

            parallelExporter.shutdown();

            // Phase 6: Finalize
            System.out.println("\nPhase 6: Finalizing export...");

            sqlWriter.close(filteredTables.size());

            ExportStatistics statistics = new ExportStatistics();
            statistics.start();
            exportResults.forEach(statistics::recordTableExport);
            statistics.end();

            ReportGenerator reportGenerator = new ReportGenerator(config, statistics);
            reportGenerator.generateReport();

            // Final summary
            printSummary(overallStartTime, allTables.size(), filteredTables.size(),
                    exportResults, phase2Duration, phase3Duration, phase5Duration, config);

        } finally {
            connectionPool.shutdown();
            connManager.closeConnection();
        }
    }

    /**
     * Prints configuration being used.
     */
    private void printConfiguration() {
        ExporterProperties.DatabaseConfig dbConfig = properties.getDatabase();
        ExporterProperties.ExportConfig exportConfig = properties.getExport();
        ExporterProperties.ParallelConfig parallelConfig = properties.getParallel();

        System.out.println("📋 CONFIGURATION:");
        System.out.println("─────────────────────────────────────────────────────────────────");
        System.out.printf("  Database Host:        %s:%d\n", dbConfig.getHost(), dbConfig.getPort());
        System.out.printf("  Database Name:        %s\n", dbConfig.getName());
        System.out.printf("  Schema:               %s\n", dbConfig.getSchema());
        System.out.printf("  Row Limit:            %d\n", exportConfig.getRowLimit());
        System.out.printf("  Parallel Threads:     %d\n", parallelConfig.getThreads());
        System.out.printf("  Output Directory:     %s\n", exportConfig.getOutputDirectory());
        System.out.println("─────────────────────────────────────────────────────────────────");
        System.out.println();
    }

    /**
     * Prints final summary.
     */
    private void printSummary(long startTime, int totalTables, int exportedTables,
                              Map<String, Long> exportResults, double phase2Duration,
                              double phase3Duration, double phase5Duration, ExportConfig config) {
        double totalDuration = (System.currentTimeMillis() - startTime) / 1000.0;
        long totalRows = exportResults.values().stream().mapToLong(Long::longValue).sum();
        int tablesWithData = (int) exportResults.values().stream().filter(count -> count > 0).count();

        System.out.println();
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║                        EXPORT COMPLETE!                        ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("📊 PERFORMANCE SUMMARY:");
        System.out.println("─────────────────────────────────────────────────────────────────");
        System.out.printf("  Schema Analysis:      %.2f seconds\n", phase2Duration);
        System.out.printf("  Table Filtering:      %.2f seconds\n", phase3Duration);
        System.out.printf("  Data Export:          %.2f seconds\n", phase5Duration);
        System.out.printf("  Total Duration:       %.2f seconds\n", totalDuration);
        System.out.println("─────────────────────────────────────────────────────────────────");
        System.out.printf("  Tables Analyzed:      %d\n", totalTables);
        System.out.printf("  Tables Filtered:      %d\n", totalTables - exportedTables);
        System.out.printf("  Tables Exported:      %d\n", exportedTables);
        System.out.printf("  Tables with Data:     %d\n", tablesWithData);
        System.out.printf("  Total Rows Exported:  %,d\n", totalRows);
        System.out.println("─────────────────────────────────────────────────────────────────");
        System.out.printf("  Throughput:           %.0f rows/second\n", totalRows / phase5Duration);
        System.out.printf("  Parallel Threads:     %d\n", config.parallelThreads());
        System.out.println("─────────────────────────────────────────────────────────────────");
        System.out.println();
        System.out.println("✅ Export files created in: " + config.outputDirectory());
        System.out.println();
    }
}
