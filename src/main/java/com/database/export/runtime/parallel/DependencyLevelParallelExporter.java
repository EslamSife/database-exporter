package com.database.export.runtime.parallel;

import com.database.export.runtime.config.ExportConfig;
import com.database.export.runtime.connection.ConnectionPool;
import com.database.export.runtime.exporter.TableDataExporter;
import com.database.export.runtime.metadata.ForeignKeyInfo;
import com.database.export.runtime.metadata.TableMetadata;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Parallel table exporter that respects foreign key dependencies.
 * 
 * Exports tables in "levels" where:
 * - Level 0: Tables with no foreign keys (can export immediately)
 * - Level 1: Tables that depend only on Level 0 tables
 * - Level 2: Tables that depend on Level 0 or Level 1 tables
 * - etc.
 * 
 * All tables within a level can be exported in parallel safely.
 */
public class DependencyLevelParallelExporter {
    
    private static final Logger logger = Logger.getLogger(DependencyLevelParallelExporter.class.getName());
    
    private final ExecutorService exportPool;
    private final ConnectionPool connectionPool;
    private final ExportConfig config;
    private final TableDataExporter tableExporter;
    
    public DependencyLevelParallelExporter(ExportConfig config,
                                          ConnectionPool connectionPool,
                                          TableDataExporter tableExporter) {
        this.config = config;
        this.connectionPool = connectionPool;
        this.tableExporter = tableExporter;
        this.exportPool = Executors.newFixedThreadPool(config.parallelThreads());
    }
    
    /**
     * Exports tables in parallel while respecting dependencies.
     * Returns a map of table name to row count exported.
     */
    public Map<String, Long> exportInParallel(List<TableMetadata> sortedTables) throws Exception {
        logger.info("========================================");
        logger.info("Starting parallel export with " + config.parallelThreads() + " threads");
        logger.info("========================================");
        
        long startTime = System.currentTimeMillis();
        
        // Build dependency graph
        Map<String, Set<String>> dependencyGraph = buildDependencyGraph(sortedTables);
        
        // Assign each table to a level
        Map<String, Integer> tableLevels = computeDependencyLevels(sortedTables, dependencyGraph);
        
        // Group tables by level
        Map<Integer, List<TableMetadata>> levelGroups = sortedTables.stream()
            .collect(Collectors.groupingBy(t -> tableLevels.getOrDefault(t.tableName(), 0)));
        
        int maxLevel = levelGroups.keySet().stream().max(Integer::compareTo).orElse(0);
        
        logger.info(String.format("Dependency analysis complete:"));
        logger.info(String.format("  - Total tables: %d", sortedTables.size()));
        logger.info(String.format("  - Dependency levels: %d", maxLevel + 1));
        for (int level = 0; level <= maxLevel; level++) {
            int tablesInLevel = levelGroups.getOrDefault(level, Collections.emptyList()).size();
            logger.info(String.format("  - Level %d: %d tables", level, tablesInLevel));
        }
        logger.info("========================================");
        
        Map<String, Long> exportResults = new ConcurrentHashMap<>();
        
        // Export each level sequentially, but tables within a level in parallel
        for (int level = 0; level <= maxLevel; level++) {
            List<TableMetadata> tablesInLevel = levelGroups.get(level);
            if (tablesInLevel == null || tablesInLevel.isEmpty()) continue;
            
            logger.info("");
            logger.info(String.format("=== LEVEL %d: Exporting %d tables in parallel ===", 
                level, tablesInLevel.size()));
            
            long levelStartTime = System.currentTimeMillis();
            
            // Submit all tables in this level for parallel execution
            List<Future<TableExportResult>> futures = tablesInLevel.stream()
                .map(table -> exportPool.submit(() -> exportTableSafe(table)))
                .collect(Collectors.toList());
            
            // Wait for all tables in this level to complete
            int completed = 0;
            long totalRowsInLevel = 0;
            for (Future<TableExportResult> future : futures) {
                TableExportResult result = future.get(); // Blocks until done
                exportResults.put(result.tableName, result.rowCount);
                totalRowsInLevel += result.rowCount;
                
                completed++;
                logger.info(String.format("  [%d/%d] ✓ %-45s %,6d rows in %.2f sec", 
                    completed, futures.size(), 
                    result.tableName + ":", 
                    result.rowCount, 
                    result.durationSeconds));
            }
            
            long levelDuration = System.currentTimeMillis() - levelStartTime;
            logger.info(String.format("=== Level %d complete: %,d rows in %.2f seconds ===", 
                level, totalRowsInLevel, levelDuration / 1000.0));
        }
        
        long totalDuration = System.currentTimeMillis() - startTime;
        long totalRows = exportResults.values().stream().mapToLong(Long::longValue).sum();
        
        logger.info("");
        logger.info("========================================");
        logger.info("Parallel export complete!");
        logger.info(String.format("  - Duration: %.2f seconds", totalDuration / 1000.0));
        logger.info(String.format("  - Total rows: %,d", totalRows));
        logger.info(String.format("  - Throughput: %.0f rows/sec", totalRows / (totalDuration / 1000.0)));
        logger.info("========================================");
        
        return exportResults;
    }
    
    /**
     * Safely exports a single table with error handling.
     */
    private TableExportResult exportTableSafe(TableMetadata metadata) {
        Connection conn = null;
        long startTime = System.currentTimeMillis();
        
        try {
            conn = connectionPool.acquire();
            long rowCount = tableExporter.exportTable(metadata, conn);
            
            long duration = System.currentTimeMillis() - startTime;
            return new TableExportResult(metadata.tableName(), rowCount, duration / 1000.0);
            
        } catch (Exception e) {
            logger.severe(String.format("Failed to export table %s: %s", 
                metadata.tableName(), e.getMessage()));
            throw new RuntimeException(e);
        } finally {
            if (conn != null) {
                connectionPool.release(conn);
            }
        }
    }
    
    /**
     * Builds a dependency graph showing which tables each table depends on.
     */
    private Map<String, Set<String>> buildDependencyGraph(List<TableMetadata> tables) {
        Map<String, Set<String>> graph = new HashMap<>();
        Set<String> allTableNames = tables.stream()
            .map(TableMetadata::tableName)
            .collect(Collectors.toSet());
        
        for (TableMetadata table : tables) {
            Set<String> dependencies = table.foreignKeys().stream()
                .map(ForeignKeyInfo::referencedTable)
                .filter(allTableNames::contains) // Only include tables in this export
                .collect(Collectors.toSet());
            
            graph.put(table.tableName(), dependencies);
        }
        
        return graph;
    }
    
    /**
     * Computes dependency levels using BFS algorithm.
     * Level 0 = no dependencies, Level N = depends on tables in levels 0..N-1
     */
    private Map<String, Integer> computeDependencyLevels(List<TableMetadata> tables,
                                                         Map<String, Set<String>> dependencyGraph) {
        Map<String, Integer> levels = new HashMap<>();
        Set<String> processed = new HashSet<>();
        Queue<String> queue = new LinkedList<>();
        
        // Find root tables (no dependencies) - they are level 0
        for (TableMetadata table : tables) {
            Set<String> deps = dependencyGraph.get(table.tableName());
            if (deps == null || deps.isEmpty()) {
                queue.offer(table.tableName());
                levels.put(table.tableName(), 0);
                processed.add(table.tableName());
            }
        }
        
        // BFS to assign levels
        while (!queue.isEmpty()) {
            String current = queue.poll();
            
            // Find tables that depend on current table
            for (Map.Entry<String, Set<String>> entry : dependencyGraph.entrySet()) {
                String tableName = entry.getKey();
                Set<String> deps = entry.getValue();
                
                if (processed.contains(tableName)) continue;
                
                // Check if all dependencies are processed
                if (processed.containsAll(deps)) {
                    int maxDepLevel = deps.stream()
                        .mapToInt(dep -> levels.getOrDefault(dep, 0))
                        .max()
                        .orElse(-1);
                    
                    levels.put(tableName, maxDepLevel + 1);
                    queue.offer(tableName);
                    processed.add(tableName);
                }
            }
        }
        
        // Handle any remaining tables (circular dependencies)
        for (TableMetadata table : tables) {
            if (!processed.contains(table.tableName())) {
                levels.put(table.tableName(), Integer.MAX_VALUE); // Put at end
                logger.warning("Circular dependency detected for table: " + table.tableName());
            }
        }
        
        return levels;
    }

    public void shutdown() {
        exportPool.shutdown();
        try {
            if (!exportPool.awaitTermination(60, TimeUnit.SECONDS)) {
                exportPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            exportPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    record TableExportResult(String tableName, long rowCount, double durationSeconds) {}
}
