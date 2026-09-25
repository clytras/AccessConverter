package io.lytrax.accessconverter.target.mysql;

import java.util.Locale;

/**
 * The two server families a dump is written for (05, Dialects). They share one writer; a dump written for one isn't
 * guaranteed to load on the other, and MariaDB executes {@code /*!NNNNN … *}{@code /} comments too, so conditional
 * comments can't tell them apart.
 */
public enum MySqlDialect {
    /** MySQL 8.0.13 or later: the first with expression defaults, which the dump uses. */
    MYSQL("MySQL", "MySQL 8.0.13 or later", "utf8mb4_0900_as_ci"),
    /** MariaDB 10.11 or later. */
    MARIADB("MariaDB", "MariaDB 10.11 or later", "utf8mb4_uca1400_as_ci");

    private final String displayName;
    private final String baseline;
    private final String defaultCollation;

    MySqlDialect(String displayName, String baseline, String defaultCollation) {
        this.displayName = displayName;
        this.baseline = baseline;
        this.defaultCollation = defaultCollation;
    }

    public String displayName() {
        return displayName;
    }

    /** The oldest server the dump is written for, for the header. */
    public String baseline() {
        return baseline;
    }

    /**
     * Accent-sensitive and case-insensitive like Access, and never stricter than Access on the probes of 05: a
     * unique index that holds in Access holds here.
     */
    public String defaultCollation() {
        return defaultCollation;
    }

    /** The dialect a server's {@code VERSION()} belongs to. */
    public static MySqlDialect ofVersion(String version) {
        return version.toLowerCase(Locale.ROOT).contains("mariadb") ? MARIADB : MYSQL;
    }
}
