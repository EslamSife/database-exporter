package com.database.export.runtime.statistics;

import lombok.Getter;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks export statistics.
 */
public class ExportStatistics {

  private final Map<String, Long> tableRowCounts = new ConcurrentHashMap<>();
  @Getter
  private long totalTablesProcessed = 0;
  @Getter
  private long totalRowsExported = 0;
  @Getter
  private LocalDateTime startTime;
  @Getter
  private LocalDateTime endTime;

  public synchronized void recordTableExport(String tableName, long rowCount) {
    tableRowCounts.put(tableName, rowCount);
    totalTablesProcessed++;
    totalRowsExported += rowCount;
  }

  public void start() {
    startTime = LocalDateTime.now();
  }

  public void end() {
    endTime = LocalDateTime.now();
  }

  public String getSummary() {
    long durationSeconds = Duration.between(startTime, endTime).getSeconds();

    return String.format(
        """
        
        ========================================
        Export Statistics
        ========================================
        Tables Processed: %d
        Total Rows Exported: %,d
        Duration: %d seconds (%.2f minutes)
        Avg Rows/Table: %.2f
        Throughput: %.2f rows/second
        ========================================
        """,
        totalTablesProcessed,
        totalRowsExported,
        durationSeconds,
        durationSeconds / 60.0,
        totalTablesProcessed > 0 ? (double) totalRowsExported / totalTablesProcessed : 0.0,
        durationSeconds > 0 ? (double) totalRowsExported / durationSeconds : 0.0);
  }

  public Map<String, Long> getTableRowCounts() {
    return new HashMap<>(tableRowCounts);
  }

}
