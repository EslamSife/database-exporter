package com.database.export.runtime.sql;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Formats SQL values for INSERT statements.
 */
public class SqlValueFormatter {

  /**
   * Formats a value from result set for SQL INSERT statement.
   */
  public String formatValue(ResultSet rs, int columnIndex, int sqlType) throws SQLException {
    Object value = rs.getObject(columnIndex);

    if (value == null) {
      return "NULL";
    }

    return switch (sqlType) {
      case Types.VARCHAR, Types.CHAR, Types.NVARCHAR, Types.NCHAR, Types.LONGVARCHAR,
              Types.LONGNVARCHAR, Types.CLOB, Types.NCLOB ->
          formatStringValue(value);
      case Types.DATE -> formatDateValue(rs.getDate(columnIndex));
      case Types.TIME -> formatTimeValue(rs.getTime(columnIndex));
      case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE ->
          formatTimestampValue(rs.getTimestamp(columnIndex));
      case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB ->
          formatBinaryValue(rs.getBytes(columnIndex));
      case Types.BIT, Types.BOOLEAN -> formatBooleanValue(rs.getBoolean(columnIndex));
      case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.NUMERIC,
              Types.DECIMAL, Types.REAL, Types.FLOAT, Types.DOUBLE ->
          formatNumericValue(value);
      default -> formatDefaultValue(value);
    };
  }

  private String formatStringValue(Object value) {
    String strValue =
        value
            .toString()
            .replace("'", "''")
            .replace("\r\n", " ")
            .replace("\n", " ")
            .replace("\r", " ");
    return "N'" + strValue + "'";
  }

  private String formatDateValue(Date date) {
    return "'" + date.toString() + "'";
  }

  private String formatTimeValue(Time time) {
    return "'" + time.toString() + "'";
  }

  private String formatTimestampValue(Timestamp timestamp) {
    return "'" + timestamp.toString() + "'";
  }

  private String formatBinaryValue(byte[] bytes) {
    if (bytes.length > 8000) {
      return "NULL /* Binary data too large */";
    }
    return "0x" + bytesToHex(bytes);
  }

  private String formatBooleanValue(boolean bool) {
    return bool ? "1" : "0";
  }

  private String formatNumericValue(Object value) {
    return value.toString();
  }

  private String formatDefaultValue(Object value) {
    String strValue = value.toString().replace("'", "''");
    return "'" + strValue + "'";
  }

  private String bytesToHex(byte[] bytes) {
    StringBuilder hexString = new StringBuilder();
    for (byte b : bytes) {
      String hex = Integer.toHexString(0xff & b);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return hexString.toString();
  }
}
