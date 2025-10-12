package com.database.export.runtime;

import com.database.export.runtime.config.ExportConfig;
import com.database.export.runtime.config.ExportConfigBuilder;

/**
 * Application entry point for database export.
 */
public class ExporterApplication {

  public static void main(String[] args) {
    try {
         exportLocalContainerData();
       // exportWithStaticConfig();
    } catch (Exception e) {
      System.err.println("FATAL ERROR: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

    public static void exportLocalContainerData() throws Exception {

        ExportConfig config = new ExportConfigBuilder()
                .dbHost("localhost")
                .dbPort("1433")
                .dbName("MES_Database")
                .dbUser("sa")
                .dbPassword("MesAdmin123!")
                .schemaName("dbo")
                .rowLimit(200)
                .outputDirectory("./exports")
                .parallelThreads(8)
                .batchSize(2000)
                .build();

        DatabaseExporter exporter = DatabaseExporterFactory.create(config);
        exporter.export();
        exporter.generateReport();

        System.out.println("✓ Export completed successfully!");
    }
}
