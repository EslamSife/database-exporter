package com.database.export.runtime.schema;

import com.database.export.runtime.config.ExportConfig;
import com.database.export.runtime.metadata.ColumnInfo;
import com.database.export.runtime.metadata.ForeignKeyInfo;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Extracts metadata (primary keys, foreign keys, columns) from database tables.
 */
public class MetadataExtractor {

  private final ExportConfig config;

  public MetadataExtractor(ExportConfig config) {
    this.config = config;
  }

  public List<String> extractPrimaryKeys(DatabaseMetaData metaData, String tableName)
      throws SQLException {
    List<String> primaryKeys = new ArrayList<>();

    try (ResultSet rs = metaData.getPrimaryKeys(config.dbName(), config.schemaName(), tableName)) {
      while (rs.next()) {
        primaryKeys.add(rs.getString("COLUMN_NAME"));
      }
    }

    return primaryKeys;
  }

  public List<ForeignKeyInfo> extractForeignKeys(DatabaseMetaData metaData, String tableName)
      throws SQLException {
    List<ForeignKeyInfo> foreignKeys = new ArrayList<>();

    try (ResultSet rs = metaData.getImportedKeys(config.dbName(), config.schemaName(), tableName)) {
      while (rs.next()) {
        foreignKeys.add(
            new ForeignKeyInfo(
                rs.getString("FK_NAME"),
                rs.getString("FKCOLUMN_NAME"),
                rs.getString("PKTABLE_SCHEM"),
                rs.getString("PKTABLE_NAME"),
                rs.getString("PKCOLUMN_NAME"),
                rs.getInt("KEY_SEQ")));
      }
    }

    foreignKeys.sort(Comparator.naturalOrder());
    return foreignKeys;
  }

  public List<ColumnInfo> extractColumns(DatabaseMetaData metaData, String tableName)
      throws SQLException {
    List<ColumnInfo> columns = new ArrayList<>();

    try (ResultSet rs = metaData.getColumns(config.dbName(), config.schemaName(), tableName, "%")) {
      while (rs.next()) {
        columns.add(
            new ColumnInfo(
                rs.getString("COLUMN_NAME"),
                rs.getInt("DATA_TYPE"),
                rs.getString("TYPE_NAME"),
                rs.getInt("COLUMN_SIZE"),
                "YES".equals(rs.getString("IS_NULLABLE")),
                "YES".equals(rs.getString("IS_AUTOINCREMENT")),
                rs.getInt("ORDINAL_POSITION")));
      }
    }

    return columns;
  }
}
