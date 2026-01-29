# Database Exporter

A production-grade SQL Server database export tool that generates INSERT statements while preserving referential integrity through topological sorting of foreign key dependencies.

## Problem Statement

Migrating data between database environments (dev → staging → production) or creating consistent backups requires careful handling of foreign key constraints. Traditional export tools often fail when:

- Tables have circular dependencies
- Export order doesn't respect FK relationships
- Large datasets need to be chunked while maintaining transaction boundaries

This tool solves these problems by analyzing the database schema, building a dependency graph, and performing a topological sort to determine the correct export order.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DatabaseExporter                               │
│  (Orchestrates the export process)                                       │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────┐         ┌─────────────────┐         ┌─────────────────┐
│SchemaAnalyzer │         │DependencyResolver│        │TableDataExporter │
│               │         │                  │         │                 │
│• Table scan   │         │• FK graph build  │         │• Chunk export   │
│• Column meta  │         │• Topological sort│         │• SQL generation │
│• FK detection │         │• Cycle detection │         │• File writing   │
└───────────────┘         └─────────────────┘         └─────────────────┘
        │                           │                           │
        ▼                           ▼                           ▼
┌───────────────┐         ┌─────────────────┐         ┌─────────────────┐
│MetadataExtract│         │  TableMetadata  │         │SqlStatement     │
│               │         │  (Records)      │         │Generator        │
│• JDBC metadata│         │• ColumnInfo     │         │• Value escape   │
│• Type mapping │         │• ForeignKeyInfo │         │• Type handling  │
└───────────────┘         └─────────────────┘         └─────────────────┘
```

## Tech Stack

| Technology | Version | Justification |
|------------|---------|---------------|
| Java | 21 | Records, pattern matching, virtual threads support |
| Spring Boot | 3.5 | Production-ready framework with excellent JDBC support |
| MSSQL JDBC | Latest | Native SQL Server driver with bulk operations |
| Docker | 3.8 | Reproducible development environment |

## Key Design Decisions

### 1. Factory Pattern for Dependency Injection
The `DatabaseExporterFactory` assembles all components with their dependencies, making the system testable and allowing different configurations without modifying core logic.

```java
public static DatabaseExporter create(ExportConfig config) {
    // All dependencies explicitly wired
    DatabaseConnectionManager connectionManager = new DatabaseConnectionManager(config);
    SchemaAnalyzer schemaAnalyzer = createSchemaAnalyzer(config, connectionManager);
    // ...
}
```

### 2. Topological Sort with Cycle Detection
Foreign key relationships form a directed graph. Kahn's algorithm ensures tables are exported in dependency order, with explicit handling for circular dependencies.

```java
public List<TableMetadata> topologicalSort(Map<String, TableMetadata> tables) {
    // Calculate in-degree for each table
    // Process tables with zero dependencies first
    // Handle cycles by logging warnings and appending remaining tables
}
```

### 3. Immutable Data Transfer Objects
Java Records ensure thread safety and eliminate defensive copying:

```java
public record TableMetadata(
    String tableName,
    List<ColumnInfo> columns,
    List<ForeignKeyInfo> foreignKeys,
    SortStrategy sortStrategy
) {}
```

### 4. Separation of SQL Generation from I/O
`SqlStatementGenerator` handles value formatting and escaping, while `SqlWriter` manages file operations. This separation allows for different output strategies (single file, per-table, streaming).

## Trade-offs

| Decision | Benefit | Cost |
|----------|---------|------|
| In-memory graph processing | Simple implementation, fast for typical schemas | Memory bound for 1000+ tables |
| Single-threaded export | Predictable ordering, simple debugging | Slower for large datasets |
| Full table scans | Complete data capture | No incremental export support |

## What This Demonstrates

- **System design thinking**: Breaking a complex problem into focused, composable components
- **Algorithm application**: Graph theory (topological sort) solving a real infrastructure problem
- **Production concerns**: Logging, statistics, error handling, Docker support
- **Clean architecture**: Clear boundaries between I/O, business logic, and data structures
- **Modern Java**: Records, factory patterns, immutable design

## Quick Start

```bash
# Start SQL Server with sample data
docker-compose up -d

# Run the exporter
./mvnw spring-boot:run
```

## Configuration

```yaml
export:
  output-directory: ./output
  chunk-size: 1000
  include-schemas: dbo
  exclude-tables: audit_log,temp_*
```

## Output

The tool generates:
- `{table_name}.sql` - INSERT statements per table
- `export_report.txt` - Statistics and dependency order
- `export.log` - Detailed execution log

---

*Built for database migration scenarios where data integrity cannot be compromised.*
