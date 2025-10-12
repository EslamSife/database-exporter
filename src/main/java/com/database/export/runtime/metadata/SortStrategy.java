package com.database.export.runtime.metadata;

import java.util.List;
import java.util.stream.Collectors;

public sealed interface SortStrategy
        permits SortStrategy.DateTimeBased, SortStrategy.PrimaryKeyBased, SortStrategy.NoSort {

    String getSortClause();

    record DateTimeBased(String columnName, DateColumnType type) implements SortStrategy {
        @Override
        public String getSortClause() {
            return columnName + " DESC";
        }

        public enum DateColumnType {
            UPDATED_DATE,
            MODIFIED_DATE,
            CREATED_DATE,
            GENERIC_DATE
        }
    }

    record PrimaryKeyBased(List<String> keyColumns) implements SortStrategy {
        public PrimaryKeyBased {
            keyColumns = List.copyOf(keyColumns);
        }

        @Override
        public String getSortClause() {
            return keyColumns.stream()
                    .map(col -> "[" + col + "] DESC")
                    .collect(Collectors.joining(", "));
        }
    }

    record NoSort() implements SortStrategy {
        @Override
        public String getSortClause() {
            return "";
        }
    }
}
