package com.database.export.runtime.sql;

import com.database.export.runtime.metadata.TableMetadata;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.stream.Collectors;

/**
 * Generates SQL SELECT and INSERT statements.
 */
public class SqlStatementGenerator {

  private final SqlValueFormatter valueFormatter;

  public SqlStatementGenerator(SqlValueFormatter valueFormatter) {
    this.valueFormatter = valueFormatter;
  }

  /**
   * Builds a SELECT query for table export.
   */
  public String buildSelectQuery(TableMetadata metadata, int rowLimit) {
    StringBuilder query = new StringBuilder();

    query.append("SELECT ");

    if (rowLimit > 0) {
      query.append("TOP ").append(rowLimit).append(" ");
    }

    query.append(
        metadata.columns().stream()
            .map(col -> "[" + col.columnName() + "]")
            .collect(Collectors.joining(", ")));

    query.append(" FROM ").append(metadata.fullTableName());

    String sortClause = metadata.sortStrategy().getSortClause();
    if (!sortClause.isEmpty()) {
      query.append(" ORDER BY ").append(sortClause);
    }

    return query.toString();
  }

  /**
   * Generates an INSERT statement from a result set row.
   */
  public String generateInsertStatement(TableMetadata metadata, ResultSet rs) throws SQLException {
    ResultSetMetaData rsMetaData = rs.getMetaData();
    int columnCount = rsMetaData.getColumnCount();

    StringBuilder insertStmt = new StringBuilder();
    insertStmt.append("INSERT INTO ").append(metadata.fullTableName()).append(" (");

    for (int i = 1; i <= columnCount; i++) {
      if (i > 1) insertStmt.append(", ");
      insertStmt.append("[").append(rsMetaData.getColumnName(i)).append("]");
    }

    insertStmt.append(") VALUES (");

    for (int i = 1; i <= columnCount; i++) {
      if (i > 1) insertStmt.append(", ");
      insertStmt.append(valueFormatter.formatValue(rs, i, rsMetaData.getColumnType(i)));
    }

    insertStmt.append(");");

    return insertStmt.toString();
  }
}
