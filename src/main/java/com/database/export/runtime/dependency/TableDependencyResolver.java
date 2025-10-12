package com.database.export.runtime.dependency;

import com.database.export.runtime.metadata.ForeignKeyInfo;
import com.database.export.runtime.metadata.TableMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Resolves table dependencies using topological sort to maintain referential integrity during
 * export.
 */
public class TableDependencyResolver {

  private static final Logger logger = Logger.getLogger(TableDependencyResolver.class.getName());

  /**
   * Performs topological sort to ensure referential integrity during export.
   */
  public List<TableMetadata> topologicalSort(Map<String, TableMetadata> tableMetadataMap) {
    logger.info("Performing topological sort for referential integrity...");

    List<TableMetadata> result = new ArrayList<>();
    Set<String> visited = new HashSet<>();
    Map<String, Integer> inDegree = calculateInDegree(tableMetadataMap);
    Queue<String> queue = initializeQueue(inDegree);

    processDependencies(tableMetadataMap, result, visited, inDegree, queue);
    handleCircularDependencies(tableMetadataMap, result, visited);

    logger.info(String.format("✓ Topological sort complete: %d tables ordered", result.size()));
    return result;
  }

  private Map<String, Integer> calculateInDegree(Map<String, TableMetadata> tableMetadataMap) {
    Map<String, Integer> inDegree = new HashMap<>();

    for (TableMetadata metadata : tableMetadataMap.values()) {
      inDegree.put(metadata.tableName(), 0);
    }

    for (TableMetadata metadata : tableMetadataMap.values()) {
      for (ForeignKeyInfo fk : metadata.foreignKeys()) {
        String refTable = fk.referencedTable();
        if (tableMetadataMap.containsKey(refTable)) {
          inDegree.merge(metadata.tableName(), 1, Integer::sum);
        }
      }
    }

    return inDegree;
  }

  private Queue<String> initializeQueue(Map<String, Integer> inDegree) {
    Queue<String> queue = new LinkedList<>();
    for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
      if (entry.getValue() == 0) {
        queue.offer(entry.getKey());
      }
    }
    return queue;
  }

  private void processDependencies(
      Map<String, TableMetadata> tableMetadataMap,
      List<TableMetadata> result,
      Set<String> visited,
      Map<String, Integer> inDegree,
      Queue<String> queue) {

    while (!queue.isEmpty()) {
      String tableName = queue.poll();
      TableMetadata metadata = tableMetadataMap.get(tableName);

      if (metadata != null && !visited.contains(tableName)) {
        result.add(metadata);
        visited.add(tableName);
        updateDependentTables(tableMetadataMap, visited, inDegree, queue, tableName);
      }
    }
  }

  private void updateDependentTables(
      Map<String, TableMetadata> tableMetadataMap,
      Set<String> visited,
      Map<String, Integer> inDegree,
      Queue<String> queue,
      String currentTable) {

    for (TableMetadata dependent : tableMetadataMap.values()) {
      if (visited.contains(dependent.tableName())) {
        continue;
      }

      boolean dependsOnCurrent =
          dependent.foreignKeys().stream()
              .anyMatch(fk -> fk.referencedTable().equals(currentTable));

      if (dependsOnCurrent) {
        int currentInDegree = inDegree.get(dependent.tableName());
        inDegree.put(dependent.tableName(), currentInDegree - 1);

        if (currentInDegree - 1 == 0) {
          queue.offer(dependent.tableName());
        }
      }
    }
  }

  private void handleCircularDependencies(
      Map<String, TableMetadata> tableMetadataMap,
      List<TableMetadata> result,
      Set<String> visited) {

    for (TableMetadata metadata : tableMetadataMap.values()) {
      if (!visited.contains(metadata.tableName())) {
        logger.warning("Circular dependency detected for table: " + metadata.tableName());
        result.add(metadata);
        visited.add(metadata.tableName());
      }
    }
  }
}
