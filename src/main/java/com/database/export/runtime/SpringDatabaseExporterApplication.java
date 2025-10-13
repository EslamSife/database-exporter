package com.database.export.runtime;

import com.database.export.runtime.config.ExporterProperties;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;


@SpringBootApplication(exclude = {
        org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration.class,
        org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration.class,
        org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration.class
})
@EnableConfigurationProperties(ExporterProperties.class)
public class SpringDatabaseExporterApplication {

    public static void main(String[] args) {
        // Disable Spring Boot banner for cleaner output
        SpringApplication app = new SpringApplication(SpringDatabaseExporterApplication.class);
        app.setBannerMode(org.springframework.boot.Banner.Mode.OFF);
        app.run(args);
    }

    @Bean
    public CommandLineRunner exportRunner(DatabaseExportService exportService) {
        return args -> {
            printBanner();
            exportService.executeExport();
        };
    }

    private void printBanner() {
        System.out.println("╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║   DATABASE EXPORTER - SPRING BOOT VERSION                     ║");
        System.out.println("║                                                                ║");
        System.out.println("║   ✅ YAML Configuration                                        ║");
        System.out.println("║   ✅ Profile Support (dev/staging/prod)                        ║");
        System.out.println("║   ✅ Environment-Aware Security                                ║");
        System.out.println("║   ✅ Bulk Metadata Extraction                                  ║");
        System.out.println("║   ✅ JDBC Fetch Size Optimization                              ║");
        System.out.println("║   ✅ Connection Pooling                                        ║");
        System.out.println("║   ✅ Parallel Export (Dependency-Aware)                        ║");
        System.out.println("║   ✅ Smart Table Filtering                                     ║");
        System.out.println("║                                                                ║");
        System.out.println("╚════════════════════════════════════════════════════════════════╝");
        System.out.println();
    }
}
