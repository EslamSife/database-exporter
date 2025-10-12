package com.database.export.runtime.metadata;

import java.sql.Types;

public record ColumnInfo(
        String columnName,
        int dataType,
        String typeName,
        int columnSize,
        boolean isNullable,
        boolean isAutoIncrement,
        int ordinalPosition
) {
    public boolean isDateTimeType() {
        return dataType == Types.TIMESTAMP
                || dataType == Types.DATE
                || dataType == Types.TIME
                || dataType == Types.TIMESTAMP_WITH_TIMEZONE
                || typeName.toLowerCase().contains("datetime")
                || typeName.toLowerCase().contains("timestamp");
    }

    public boolean isNumericType() {
        return dataType == Types.INTEGER
                || dataType == Types.BIGINT
                || dataType == Types.SMALLINT
                || dataType == Types.TINYINT
                || dataType == Types.NUMERIC
                || dataType == Types.DECIMAL;
    }

    public boolean isStringType() {
        return dataType == Types.VARCHAR
                || dataType == Types.CHAR
                || dataType == Types.NVARCHAR
                || dataType == Types.NCHAR
                || dataType == Types.LONGVARCHAR
                || dataType == Types.LONGNVARCHAR;
    }
}
