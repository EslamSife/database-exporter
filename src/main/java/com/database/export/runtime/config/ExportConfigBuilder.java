package com.database.export.runtime.config;

public class ExportConfigBuilder {
    private String dbHost = "localhost";
    private String dbPort = "1433";
    private String dbName;
    private String dbUser;
    private String dbPassword;
    private int rowLimit = 200;
    private String outputDirectory = "./database_exports";
    private int parallelThreads = Runtime.getRuntime().availableProcessors();
    private boolean includeSystemTables = false;
    private String schemaName = "dbo";
    private boolean generateCreateStatements = false;
    private boolean generateDropStatements = false;
    private int batchSize = 1000;

    public ExportConfigBuilder dbHost(String dbHost) {
        this.dbHost = dbHost;
        return this;
    }

    public ExportConfigBuilder dbPort(String dbPort) {
        this.dbPort = dbPort;
        return this;
    }

    public ExportConfigBuilder dbName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public ExportConfigBuilder dbUser(String dbUser) {
        this.dbUser = dbUser;
        return this;
    }

    public ExportConfigBuilder dbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
        return this;
    }

    public ExportConfigBuilder rowLimit(int rowLimit) {
        this.rowLimit = rowLimit;
        return this;
    }

    public ExportConfigBuilder outputDirectory(String outputDirectory) {
        this.outputDirectory = outputDirectory;
        return this;
    }

    public ExportConfigBuilder parallelThreads(int parallelThreads) {
        this.parallelThreads = parallelThreads;
        return this;
    }

    public ExportConfigBuilder includeSystemTables(boolean includeSystemTables) {
        this.includeSystemTables = includeSystemTables;
        return this;
    }

    public ExportConfigBuilder schemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public ExportConfigBuilder generateCreateStatements(boolean generateCreateStatements) {
        this.generateCreateStatements = generateCreateStatements;
        return this;
    }

    public ExportConfigBuilder generateDropStatements(boolean generateDropStatements) {
        this.generateDropStatements = generateDropStatements;
        return this;
    }

    public ExportConfigBuilder batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public ExportConfig build() {
        return new ExportConfig(
                dbHost,
                dbPort,
                dbName,
                dbUser,
                dbPassword,
                rowLimit,
                outputDirectory,
                parallelThreads,
                includeSystemTables,
                schemaName,
                generateCreateStatements,
                generateDropStatements,
                batchSize
        );
    }
}
