package com.database.export.runtime.sql;

import com.database.export.runtime.config.ExportConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.logging.Logger;

/**
 * Writes SQL statements to output file.
 */
public class SqlWriter {

  private static final Logger logger = Logger.getLogger(SqlWriter.class.getName());

  private final ExportConfig config;
  private PrintWriter writer;
  private String outputFileName;

  public SqlWriter(ExportConfig config) {
    this.config = config;
  }

  public void initialize(int totalTables) throws IOException {
    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
    outputFileName = config.outputDirectory() + "/export_" + timestamp + ".sql";

    Path outputPath = Paths.get(config.outputDirectory());
    Files.createDirectories(outputPath);

    writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFileName)), true);
    writeHeader(totalTables);

    logger.info("✓ SQL output file created: " + outputFileName);
  }

  public void writeTableHeader(String tableName, List<String> primaryKeys, int foreignKeyCount) {
    writer.println();
    writer.println("-- ============================================");
    writer.println("-- Table: " + tableName);
    writer.println("-- Primary Key: " + primaryKeys);
    writer.println("-- Foreign Keys: " + foreignKeyCount);
    writer.println("-- ============================================");
    writer.println();
  }

  public void writeInsertStatements(List<String> statements) {
    for (String stmt : statements) {
      writer.println(stmt);
    }
  }

  public void writeTableFooter() {
    writer.println("GO");
    writer.println();
  }

  public void close(int totalTables) {
    if (writer != null) {
      writer.println();
      writer.println("-- ============================================");
      writer.println("-- Export Complete");
      writer.println("-- Total Tables: " + totalTables);
      writer.println("-- Generated: " + LocalDateTime.now());
      writer.println("-- ============================================");
      writer.close();
      logger.info("✓ SQL output file closed: " + outputFileName);
    }
  }

  public String getOutputFileName() {
    return outputFileName;
  }

  private void writeHeader(int totalTables) {
    writer.println("-- ============================================");
    writer.println("-- Database Export");
    writer.println("-- Database: " + config.dbName());
    writer.println("-- Generated: " + LocalDateTime.now());
    writer.println("-- Tables: " + totalTables);
    writer.println("-- Row Limit per Table: " + config.rowLimit());
    writer.println("-- ============================================");
    writer.println();
    writer.println("SET NOCOUNT ON;");
    writer.println("GO");
    writer.println();
  }
}
