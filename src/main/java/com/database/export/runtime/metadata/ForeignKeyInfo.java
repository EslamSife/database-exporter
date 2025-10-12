package com.database.export.runtime.metadata;

public record ForeignKeyInfo(
        String constraintName,
        String columnName,
        String referencedSchema,
        String referencedTable,
        String referencedColumn,
        int keySequence
) implements Comparable<ForeignKeyInfo> {

    public String referencedFullTableName() {
        return referencedSchema != null && !referencedSchema.isEmpty()
                ? referencedSchema + "." + referencedTable
                : referencedTable;
    }

    @Override
    public int compareTo(ForeignKeyInfo other) {
        return Integer.compare(this.keySequence, other.keySequence);
    }

    @Override
    public String toString() {
        return String.format(
                "%s -> %s.%s",
                columnName,
                referencedFullTableName(),
                referencedColumn
        );
    }
}
