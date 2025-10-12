package com.database.export.runtime.connection;

import com.database.export.runtime.config.ExportConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Manages database connections for export operations.
 */
public class DatabaseConnectionManager {

  private static final Logger logger = Logger.getLogger(DatabaseConnectionManager.class.getName());

  private final ExportConfig config;
  private Connection connection;

  public DatabaseConnectionManager(ExportConfig config) {
    this.config = config;
  }

  public Connection createConnection() throws SQLException {
    logger.info("Connecting to database...");

    Properties props = buildConnectionProperties();
    connection = DriverManager.getConnection(config.jdbcUrl(), props);
    configureConnection(connection);

    logger.info("✓ Connected to database: " + config.dbName());
    return connection;
  }

  /**
   * Creates a new connection for parallel operations.
   */
  public Connection createNewConnection() throws SQLException {
    Properties props = buildConnectionProperties();
    Connection conn = DriverManager.getConnection(config.jdbcUrl(), props);
    configureConnection(conn);
    return conn;
  }

  public Connection getConnection() {
    return connection;
  }

  public void closeConnection() {
    closeConnection(connection);
  }

  public void closeConnection(Connection conn) {
    if (conn != null) {
      try {
        if (!conn.isClosed()) {
          conn.close();
          if (conn == connection) {
            logger.info("✓ Database connection closed");
          }
        }
      } catch (SQLException e) {
        logger.warning("Error closing connection: " + e.getMessage());
      }
    }
  }

  private Properties buildConnectionProperties() {
    Properties props = new Properties();
    props.setProperty("user", config.dbUser());
    props.setProperty("password", config.dbPassword());
    props.setProperty("encrypt", "false");
    props.setProperty("trustServerCertificate", "true");
    props.setProperty("loginTimeout", "30");
    return props;
  }

  private void configureConnection(Connection conn) throws SQLException {
    conn.setAutoCommit(false);
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
  }
}
