package com.database.export.runtime.filter;

import com.database.export.runtime.config.ExporterProperties.FilterConfig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * YAML-configurable table exclusion filter with intelligent fallback to defaults.
 *
 * Supports:
 * - Wildcard patterns (* and ?)
 * - Exact table name matches
 * - Prefix-based exclusion
 * - Regular expressions
 *
 * Configuration priority:
 * 1. User-provided YAML config (if not empty)
 * 2. Default patterns (fallback)
 */
public class ConfigurableTableExclusionFilter {

    private static final Logger logger = Logger.getLogger(ConfigurableTableExclusionFilter.class.getName());

    private final List<String> wildcardPatterns;
    private final Set<String> exactMatches;
    private final List<String> prefixes;
    private final List<Pattern> regexPatterns;

    /**
     * Constructs filter from YAML configuration with intelligent defaults.
     */
    public ConfigurableTableExclusionFilter(FilterConfig config) {
        this.wildcardPatterns = loadWildcardPatterns(config);
        this.exactMatches = loadExactMatches(config);
        this.prefixes = loadPrefixes(config);
        this.regexPatterns = loadRegexPatterns(config);

        logConfiguration();
    }

    /**
     * Checks if a table should be excluded from export.
     *
     * Evaluation order (for performance):
     * 1. Exact matches (O(1) lookup)
     * 2. Prefix matches (O(n) but typically fast)
     * 3. Wildcard patterns (O(n))
     * 4. Regex patterns (O(n) but most expensive)
     */
    public boolean shouldExclude(String tableName) {
        if (tableName == null || tableName.isEmpty()) {
            return true;
        }

        // 1. Exact match (fastest)
        if (exactMatches.contains(tableName)) {
            logger.fine("Excluded (exact match): " + tableName);
            return true;
        }

        // 2. Prefix match (fast)
        for (String prefix : prefixes) {
            if (tableName.startsWith(prefix)) {
                logger.fine("Excluded (prefix '" + prefix + "'): " + tableName);
                return true;
            }
        }

        // 3. Wildcard match (moderate)
        for (String pattern : wildcardPatterns) {
            if (matchesWildcard(tableName, pattern)) {
                logger.fine("Excluded (wildcard '" + pattern + "'): " + tableName);
                return true;
            }
        }

        // 4. Regex match (most expensive)
        for (Pattern regex : regexPatterns) {
            if (regex.matcher(tableName).matches()) {
                logger.fine("Excluded (regex): " + tableName);
                return true;
            }
        }

        return false;
    }

    /**
     * Loads wildcard patterns from config or uses defaults.
     */
    private List<String> loadWildcardPatterns(FilterConfig config) {
        List<String> patterns = config.getExclusionPatterns();

        if (patterns != null && !patterns.isEmpty()) {
            logger.info("Using " + patterns.size() + " custom wildcard patterns from YAML");
            return new ArrayList<>(patterns);
        }

        logger.info("Using default wildcard patterns");
        return getDefaultWildcardPatterns();
    }

    /**
     * Loads exact table matches from config or uses defaults.
     */
    private Set<String> loadExactMatches(FilterConfig config) {
        List<String> tables = config.getExcludedTables();

        if (tables != null && !tables.isEmpty()) {
            logger.info("Using " + tables.size() + " custom excluded tables from YAML");
            return new HashSet<>(tables);
        }

        logger.info("Using default excluded tables");
        return new HashSet<>(getDefaultExactMatches());
    }

    /**
     * Loads prefix patterns from config or uses defaults.
     */
    private List<String> loadPrefixes(FilterConfig config) {
        List<String> prefixes = config.getExcludedPrefixes();

        if (prefixes != null && !prefixes.isEmpty()) {
            logger.info("Using " + prefixes.size() + " custom prefixes from YAML");
            return new ArrayList<>(prefixes);
        }

        logger.info("Using default prefixes");
        return getDefaultPrefixes();
    }

    /**
     * Loads regex patterns from config or uses defaults.
     */
    private List<Pattern> loadRegexPatterns(FilterConfig config) {
        List<String> regexStrings = config.getExclusionRegex();

        if (regexStrings != null && !regexStrings.isEmpty()) {
            logger.info("Using " + regexStrings.size() + " custom regex patterns from YAML");
            return regexStrings.stream()
                    .map(regex -> {
                        try {
                            return Pattern.compile(regex);
                        } catch (Exception e) {
                            logger.warning("Invalid regex pattern: " + regex + " - " + e.getMessage());
                            return null;
                        }
                    })
                    .filter(pattern -> pattern != null)
                    .collect(Collectors.toList());
        }

        logger.info("Using default regex patterns");
        return getDefaultRegexPatterns();
    }

    /**
     * Matches a table name against a wildcard pattern.
     * Supports * (any characters) and ? (single character).
     */
    private boolean matchesWildcard(String text, String pattern) {
        String regex = pattern
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return text.matches(regex);
    }

    /**
     * Default wildcard patterns for common backup/temp/audit tables.
     */
    private List<String> getDefaultWildcardPatterns() {
        return List.of(
                "*_backup",
                "*_bk",
                "*_history",
                "*_archive",
                "*_temp",
                "*_staging",
                "*_audit",
                "*_log",
                "tmp_*",
                "temp_*",
                "staging_*",
                "archive_*",
                "bak_*"
        );
    }

    /**
     * Default exact table name matches to exclude.
     */
    private List<String> getDefaultExactMatches() {
        return List.of(
                "sysdiagrams",
                "dtproperties",
                "sysconstraints",
                "syssegments"
        );
    }

    /**
     * Default prefixes to exclude.
     */
    private List<String> getDefaultPrefixes() {
        return List.of(
                "sys",
                "INFORMATION_SCHEMA",
                "__",
                "msreplication",
                "spt_"
        );
    }

    /**
     * Default regex patterns to exclude.
     */
    private List<Pattern> getDefaultRegexPatterns() {
        List<Pattern> patterns = new ArrayList<>();

        // Tables starting with $
        patterns.add(Pattern.compile("^\\$.*"));

        // Tables ending with date pattern (e.g., table_20241013)
        patterns.add(Pattern.compile(".*_\\d{8}$"));

        // Tables ending with date-time pattern (e.g., table_20241013_120000)
        patterns.add(Pattern.compile(".*_\\d{8}_\\d{6}$"));

        return patterns;
    }

    /**
     * Logs the current configuration for debugging.
     */
    private void logConfiguration() {
        logger.info("Table Exclusion Filter Configuration:");
        logger.info("  ├─ Wildcard Patterns: " + wildcardPatterns.size());
        logger.info("  ├─ Exact Matches: " + exactMatches.size());
        logger.info("  ├─ Prefixes: " + prefixes.size());
        logger.info("  └─ Regex Patterns: " + regexPatterns.size());

        if (logger.isLoggable(java.util.logging.Level.FINE)) {
            logger.fine("Wildcard Patterns: " + wildcardPatterns);
            logger.fine("Exact Matches: " + exactMatches);
            logger.fine("Prefixes: " + prefixes);
        }
    }
}