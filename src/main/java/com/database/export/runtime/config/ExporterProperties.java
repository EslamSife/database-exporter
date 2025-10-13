package com.database.export.runtime.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;


@Setter
@Getter
@ConfigurationProperties(prefix = "exporter")
public class ExporterProperties {

    private DatabaseConfig database = new DatabaseConfig();
    private ExportConfig export = new ExportConfig();
    private ParallelConfig parallel = new ParallelConfig();
    private FilterConfig filter = new FilterConfig();

    /**
     * Database connection configuration
     */
    @Setter
    @Getter
    public static class DatabaseConfig {
        // Getters and Setters
        private String host;
        private int port;
        private String name;
        private String username;
        private String password;
        private String schema;
        private PoolConfig pool = new PoolConfig();

        /**
         * Builds JDBC URL with default SQL Server settings
         */
        public String getJdbcUrl() {
            return String.format("jdbc:sqlserver://%s:%d;databaseName=%s", host, port, name);
        }
    }

    /**
     * Connection pool configuration
     */
    @Setter
    @Getter
    public static class PoolConfig {
        private int size = 8;

    }

    /**
     * Export configuration
     */
    @Setter
    @Getter
    public static class ExportConfig {
        private int rowLimit = 200;
        private String outputDirectory = "./exports";
        private int batchSize = 2000;
        private boolean generateCreateStatements = false;
        private boolean generateDropStatements = false;

    }

    /**
     * Parallel processing configuration
     */
    @Setter
    @Getter
    public static class ParallelConfig {
        // Getters and Setters
        private int threads = 8;
        private boolean enabled = true;

    }

    /**
     * Table filtering configuration
     */
    @Setter
    @Getter
    public static class FilterConfig {
        private boolean excludeEmptyTables = true;
        private boolean excludeSystemTables = true;
        private List<String> exclusionPatterns = new ArrayList<>();
        private List<String> excludedTables = new ArrayList<>();
        private List<String> exclusionRegex = new ArrayList<>();
        private List<String> excludedPrefixes = new ArrayList<>();
    }
}
