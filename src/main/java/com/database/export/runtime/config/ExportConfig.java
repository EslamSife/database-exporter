package com.database.export.runtime.config;

/**
 * Database export configuration.
 * Validates all parameters in compact constructor.
 */
public record ExportConfig(
    String dbHost,
    String dbPort,
    String dbName,
    String dbUser,
    String dbPassword,
    int rowLimit,
    String outputDirectory,
    int parallelThreads,
    boolean includeSystemTables,
    String schemaName,
    boolean generateCreateStatements,
    boolean generateDropStatements,
    int batchSize) {

  public ExportConfig {
    validateRequired(dbHost, "Database host");
    validateRequired(dbPort, "Database port");
    validateRequired(dbName, "Database name");
    validateRequired(dbUser, "Database user");
    validateRequired(outputDirectory, "Output directory");
    validateRequired(schemaName, "Schema name");

    if (dbPassword == null) {
      throw new IllegalArgumentException("Database password cannot be null");
    }

    validatePort(dbPort);
    validatePositive(rowLimit, "Row limit");
    validatePositive(batchSize, "Batch size");
    validatePositive(parallelThreads, "Parallel threads");

    validateRange(rowLimit, 1, 1_000_000, "Row limit");
    validateRange(batchSize, 1, 10_000, "Batch size");
    validateRange(parallelThreads, 1, 32, "Parallel threads");
  }

  public String jdbcUrl() {
    return String.format(
        "jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false;"
            + "trustServerCertificate=true;integratedSecurity=false",
        dbHost, dbPort, dbName);
  }

  private static void validateRequired(String value, String fieldName) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(fieldName + " cannot be empty");
    }
  }

  private static void validatePositive(int value, String fieldName) {
    if (value <= 0) {
      throw new IllegalArgumentException(fieldName + " must be positive");
    }
  }

  private static void validateRange(int value, int min, int max, String fieldName) {
    if (value < min || value > max) {
      throw new IllegalArgumentException(
          String.format("%s must be between %d and %d", fieldName, min, max));
    }
  }

  private static void validatePort(String port) {
    try {
      int portNum = Integer.parseInt(port);
      validateRange(portNum, 1, 65535, "Database port");
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Database port must be a valid number");
    }
  }
}
