package com.database.export.runtime.statistics;

import com.database.export.runtime.config.ExportConfig;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Generates detailed export reports.
 */
public class ReportGenerator {

  private static final Logger logger = Logger.getLogger(ReportGenerator.class.getName());
  private static final String SEPARATOR = "=".repeat(100);
  private static final String LINE = "-".repeat(100);

  private final ExportConfig config;
  private final ExportStatistics statistics;

  public ReportGenerator(ExportConfig config, ExportStatistics statistics) {
    this.config = config;
    this.statistics = statistics;
  }

  /**
   * Generates a detailed export report.
   */
  public void generateReport() throws IOException {
    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
    String filename = config.outputDirectory() + "/export_report_" + timestamp + ".txt";

    try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
      writeReportHeader(writer);
      writeStatisticsSummary(writer);
      writeTableDetails(writer);
      writeReportFooter(writer);
    }

    logger.info("✓ Export report generated: " + filename);
  }

  private void writeReportHeader(PrintWriter writer) {
    writer.println(SEPARATOR);
    writer.println("DATABASE EXPORT REPORT");
    writer.println(SEPARATOR);
    writer.println();
    writer.println("Database: " + config.dbName());
    writer.println("Generated: " + LocalDateTime.now());
    writer.println("Row Limit per Table: " + config.rowLimit());
    writer.println();
  }

  private void writeStatisticsSummary(PrintWriter writer) {
    writer.println(statistics.getSummary());
  }

  private void writeTableDetails(PrintWriter writer) {
    writer.println("\nDETAILED TABLE EXPORT COUNTS");
    writer.println(LINE);

    Map<String, Long> tableRowCounts = statistics.getTableRowCounts();
    tableRowCounts.entrySet().stream()
        .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
        .forEach(
            entry ->
                writer.printf("  %-50s : %,10d rows%n", entry.getKey(), entry.getValue()));
  }

  private void writeReportFooter(PrintWriter writer) {
    writer.println();
    writer.println(SEPARATOR);
    writer.println("END OF REPORT");
    writer.println(SEPARATOR);
  }
}
