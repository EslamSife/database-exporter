package com.database.export.runtime.schema;

import com.database.export.runtime.metadata.ColumnInfo;
import com.database.export.runtime.metadata.SortStrategy;

import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Determines the optimal sort strategy for a table.
 * Priority: Updated date > Created date > Any date > Primary key > No sort
 */
public class SortStrategyResolver {

  private static final Logger logger = Logger.getLogger(SortStrategyResolver.class.getName());

  public SortStrategy determineSortStrategy(
      String tableName, List<String> primaryKeys, List<ColumnInfo> columns) {

    SortStrategy dateStrategy = findDateBasedStrategy(columns);
    if (dateStrategy != null) {
      return dateStrategy;
    }

    if (!primaryKeys.isEmpty()) {
      return new SortStrategy.PrimaryKeyBased(primaryKeys);
    }

    logger.warning(
        String.format(
            "Table '%s' has no date columns or primary key - records may not be in latest order",
            tableName));

    return new SortStrategy.NoSort();
  }

  private SortStrategy findDateBasedStrategy(List<ColumnInfo> columns) {
    List<ColumnInfo> dateColumns =
        columns.stream().filter(ColumnInfo::isDateTimeType).toList();

    if (dateColumns.isEmpty()) {
      return null;
    }

    Optional<ColumnInfo> updatedCol = findColumnByPattern(dateColumns, "updated", "modify", "modified");
    if (updatedCol.isPresent()) {
      return new SortStrategy.DateTimeBased(
          updatedCol.get().columnName(), SortStrategy.DateTimeBased.DateColumnType.UPDATED_DATE);
    }

    Optional<ColumnInfo> createdCol = findColumnByPattern(dateColumns, "created", "insert");
    if (createdCol.isPresent()) {
      return new SortStrategy.DateTimeBased(
          createdCol.get().columnName(), SortStrategy.DateTimeBased.DateColumnType.CREATED_DATE);
    }

    return new SortStrategy.DateTimeBased(
        dateColumns.get(0).columnName(), SortStrategy.DateTimeBased.DateColumnType.GENERIC_DATE);
  }

  private Optional<ColumnInfo> findColumnByPattern(List<ColumnInfo> columns, String... patterns) {
    return columns.stream()
        .filter(
            col -> {
              String lower = col.columnName().toLowerCase();
              for (String pattern : patterns) {
                if (lower.contains(pattern)) {
                  return true;
                }
              }
              return false;
            })
        .findFirst();
  }
}
