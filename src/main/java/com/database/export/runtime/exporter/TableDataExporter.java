package com.database.export.runtime.exporter;

import com.database.export.runtime.config.ExportConfig;
import com.database.export.runtime.metadata.TableMetadata;
import com.database.export.runtime.sql.SqlStatementGenerator;
import com.database.export.runtime.sql.SqlWriter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Exports data from a single table to SQL statements.
 */
public class TableDataExporter {

  private final ExportConfig config;
  private final SqlStatementGenerator sqlGenerator;
  private final SqlWriter sqlWriter;

  public TableDataExporter(
      ExportConfig config, SqlStatementGenerator sqlGenerator, SqlWriter sqlWriter) {
    this.config = config;
    this.sqlGenerator = sqlGenerator;
    this.sqlWriter = sqlWriter;
  }

  /**
   * Exports data from a single table.
   */
  public long exportTable(TableMetadata metadata, Connection connection) throws SQLException {
    writeTableHeader(metadata);

    String selectQuery = sqlGenerator.buildSelectQuery(metadata, config.rowLimit());
    long rowCount = executeExport(metadata, connection, selectQuery);

    sqlWriter.writeTableFooter();
    return rowCount;
  }

  private void writeTableHeader(TableMetadata metadata) {
    sqlWriter.writeTableHeader(
        metadata.fullTableName(), metadata.primaryKeyColumns(), metadata.foreignKeys().size());
  }

  private long executeExport(TableMetadata metadata, Connection connection, String selectQuery)
      throws SQLException {

    long rowCount = 0;
    List<String> insertStatements = new ArrayList<>();

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(selectQuery)) {

      while (rs.next() && rowCount < config.rowLimit()) {
        String insertStmt = sqlGenerator.generateInsertStatement(metadata, rs);
        insertStatements.add(insertStmt);
        rowCount++;

        if (insertStatements.size() >= config.batchSize()) {
          sqlWriter.writeInsertStatements(insertStatements);
          insertStatements.clear();
        }
      }

      if (!insertStatements.isEmpty()) {
        sqlWriter.writeInsertStatements(insertStatements);
      }
    }

    return rowCount;
  }
}
