package com.database.export.runtime.logging;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Configures logging for export operations.
 */
public class ExportLogger {

  /**
   * Configures the package root logger with console and file handlers.
   */
  public static Logger configure(String outputDirectory) throws IOException {
    Logger packageLogger = Logger.getLogger("com.database.export.runtime");

    packageLogger.setUseParentHandlers(false);
    for (Handler handler : packageLogger.getHandlers()) {
      packageLogger.removeHandler(handler);
    }

    packageLogger.addHandler(createConsoleHandler());
    packageLogger.addHandler(createFileHandler(outputDirectory));
    packageLogger.setLevel(Level.INFO);

    System.out.println("✓ Logging configured:");
    System.out.println("  - Console output: ENABLED");
    System.out.println("  - Log file: " + getLogFileName(outputDirectory));
    System.out.println();

    return packageLogger;
  }

  private static ConsoleHandler createConsoleHandler() {
    ConsoleHandler handler = new ConsoleHandler();
    handler.setLevel(Level.ALL);
    handler.setFormatter(
        new SimpleFormatter() {
          private static final String FORMAT = "[%1$tF %1$tT] [%2$s] %3$s %n";

          @Override
          public synchronized String format(LogRecord record) {
            return String.format(
                FORMAT, new Date(record.getMillis()), record.getLevel().getName(), record.getMessage());
          }
        });
    return handler;
  }

  private static FileHandler createFileHandler(String outputDirectory) throws IOException {
    Path logDir = Paths.get(outputDirectory, "logs");
    Files.createDirectories(logDir);

    String logFileName = getLogFileName(outputDirectory);
    FileHandler fileHandler = new FileHandler(logFileName);
    fileHandler.setFormatter(new SimpleFormatter());
    fileHandler.setLevel(Level.ALL);

    return fileHandler;
  }

  private static String getLogFileName(String outputDirectory) {
    Path logDir = Paths.get(outputDirectory, "logs");
    return logDir
        .resolve(
            "export_"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
                + ".log")
        .toString();
  }
}
