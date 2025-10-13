package com.database.export.runtime.filter;

import com.database.export.runtime.config.ExportConfig;
import com.database.export.runtime.config.ExporterProperties.FilterConfig;
import com.database.export.runtime.metadata.TableMetadata;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Filters tables intelligently based on various criteria.
 * - Exclusion patterns (backup, temp, audit tables)
 * - Empty tables
 * - System tables
 */
public class SmartTableFilter {
    
    private static final Logger logger = Logger.getLogger(SmartTableFilter.class.getName());
    
    private final ExportConfig config;
    private final ConfigurableTableExclusionFilter exclusionFilter;
    private final boolean excludeEmptyTables;
    
    public SmartTableFilter(ExportConfig config, FilterConfig filterConfig) {
        this.config = config;
        this.exclusionFilter = new ConfigurableTableExclusionFilter(filterConfig);
        this.excludeEmptyTables = filterConfig.isExcludeEmptyTables();
    }
    
    @Deprecated
    public SmartTableFilter(ExportConfig config, boolean excludeEmptyTables) {
        this.config = config;
        this.exclusionFilter = new ConfigurableTableExclusionFilter(new FilterConfig());
        this.excludeEmptyTables = excludeEmptyTables;
    }
    
    /**
     * Filters tables based on all criteria.
     */
    public List<TableMetadata> filterTables(Map<String, TableMetadata> allTables) {
        logger.info("========================================");
        logger.info("Filtering tables...");
        logger.info("========================================");
        
        int originalCount = allTables.size();
        
        List<TableMetadata> filteredTables = allTables.values().stream()
            .filter(this::shouldInclude)
            .collect(Collectors.toList());
        
        int excludedCount = originalCount - filteredTables.size();
        
        logger.info(String.format("Original tables: %d", originalCount));
        logger.info(String.format("Excluded tables: %d", excludedCount));
        logger.info(String.format("Tables to export: %d", filteredTables.size()));
        logger.info("========================================");
        
        return filteredTables;
    }
    
    /**
     * Determines if a table should be included in the export.
     */
    private boolean shouldInclude(TableMetadata table) {
        // Check exclusion patterns
        if (exclusionFilter.shouldExclude(table.tableName())) {
            logger.fine("Excluded by pattern: " + table.tableName());
            return false;
        }
        
        // Check if table is empty (if configured)
        if (excludeEmptyTables && table.estimatedRowCount() == 0) {
            logger.fine("Excluded (empty): " + table.tableName());
            return false;
        }
        
        return true;
    }
    
    /**
     * Gets statistics about filtered tables.
     */
    public FilterStatistics getStatistics(Map<String, TableMetadata> allTables, 
                                         List<TableMetadata> filteredTables) {
        int excludedByPattern = 0;
        int excludedByEmpty = 0;
        
        for (TableMetadata table : allTables.values()) {
            if (!filteredTables.contains(table)) {
                if (exclusionFilter.shouldExclude(table.tableName())) {
                    excludedByPattern++;
                } else if (excludeEmptyTables && table.estimatedRowCount() == 0) {
                    excludedByEmpty++;
                }
            }
        }
        
        return new FilterStatistics(
            allTables.size(),
            filteredTables.size(),
            excludedByPattern,
            excludedByEmpty
        );
    }
    
    /**
     * Statistics about table filtering.
     */
    public record FilterStatistics(
        int totalTables,
        int includedTables,
        int excludedByPattern,
        int excludedByEmpty
    ) {
        @Override
        public String toString() {
            return String.format(
                "Filter Stats: %d total, %d included, %d excluded (pattern: %d, empty: %d)",
                totalTables, includedTables, 
                excludedByPattern + excludedByEmpty,
                excludedByPattern, excludedByEmpty
            );
        }
    }
}
