package com.database.export.runtime.connection;

import com.database.export.runtime.config.ExportConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;


public class ConnectionPool {
    
    private static final Logger logger = Logger.getLogger(ConnectionPool.class.getName());
    
    private final BlockingQueue<Connection> availableConnections;
    private final Set<Connection> allConnections;
    private final ExportConfig config;
    private final AtomicInteger activeConnections = new AtomicInteger(0);
    private final int poolSize;
    
    public ConnectionPool(ExportConfig config, int poolSize) throws SQLException {
        this.config = config;
        this.poolSize = poolSize;
        this.availableConnections = new LinkedBlockingQueue<>(poolSize);
        this.allConnections = ConcurrentHashMap.newKeySet();
        
        logger.info(String.format("Initializing connection pool with %d connections...", poolSize));
        
        // Pre-create all connections
        for (int i = 0; i < poolSize; i++) {
            Connection conn = createConnection();
            availableConnections.offer(conn);
            allConnections.add(conn);
        }
        
        logger.info("✓ Connection pool initialized successfully");
    }
    
    /**
     * Creates a connection with default SQL Server settings.
     * No custom security properties - relies on JDBC driver defaults.
     */
    private Connection createConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", config.dbUser());
        props.setProperty("password", config.dbPassword());
        
        Connection conn = DriverManager.getConnection(config.jdbcUrl(), props);
        
        // Optimize for read-only export
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        conn.setReadOnly(true);
        
        return conn;
    }
    
    /**
     * Acquires a connection from the pool. Blocks if none available.
     */
    public Connection acquire() throws InterruptedException {
        activeConnections.incrementAndGet();
        Connection conn = availableConnections.take();
        logger.fine(String.format("Connection acquired (active: %d/%d)", 
            activeConnections.get(), poolSize));
        return conn;
    }
    
    /**
     * Returns a connection to the pool.
     */
    public void release(Connection conn) {
        if (conn != null) {
            activeConnections.decrementAndGet();
            availableConnections.offer(conn);
            logger.fine(String.format("Connection released (active: %d/%d)", 
                activeConnections.get(), poolSize));
        }
    }
    
    /**
     * Gets the number of currently active (acquired) connections.
     */
    public int getActiveConnectionCount() {
        return activeConnections.get();
    }
    
    /**
     * Gets the total pool size.
     */
    public int getPoolSize() {
        return poolSize;
    }
    
    /**
     * Shuts down the connection pool, closing all connections.
     */
    public void shutdown() {
        logger.info("Shutting down connection pool...");
        
        for (Connection conn : allConnections) {
            try {
                if (!conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.warning("Error closing connection: " + e.getMessage());
            }
        }
        
        availableConnections.clear();
        allConnections.clear();
        
        logger.info("✓ Connection pool shutdown complete");
    }
}
