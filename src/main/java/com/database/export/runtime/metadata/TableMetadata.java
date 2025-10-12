package com.database.export.runtime.metadata;

import java.util.List;

public record TableMetadata(
        String tableName,
        String schemaName,
        List<String> primaryKeyColumns,
        List<ForeignKeyInfo> foreignKeys,
        List<ColumnInfo> columns,
        SortStrategy sortStrategy,
        long estimatedRowCount,
        boolean hasCompositeKey
) {
    public TableMetadata {
        primaryKeyColumns = List.copyOf(primaryKeyColumns);
        foreignKeys = List.copyOf(foreignKeys);
        columns = List.copyOf(columns);
    }

    public boolean hasPrimaryKey() {
        return !primaryKeyColumns.isEmpty();
    }

    public String fullTableName() {
        return schemaName != null && !schemaName.isEmpty()
                ? "[" + schemaName + "].[" + tableName + "]"
                : "[" + tableName + "]";
    }

    @Override
    public String toString() {
        return String.format(
                "Table[%s, PK=%s, FKs=%d, Cols=%d, Rows≈%d]",
                fullTableName(),
                primaryKeyColumns,
                foreignKeys.size(),
                columns.size(),
                estimatedRowCount
        );
    }
}
