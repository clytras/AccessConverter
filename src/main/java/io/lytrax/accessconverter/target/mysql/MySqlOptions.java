package io.lytrax.accessconverter.target.mysql;

import java.util.Locale;
import java.util.Objects;

/**
 * The MySQL/MariaDB options (05). The dialect and the collation decide the plan, so {@code verify} must be given
 * the same ones; the rest only shape the dump.
 *
 * @param collation the table collation, or null for the dialect's default
 * @param dropExisting emit {@code DROP TABLE IF EXISTS} for every table before creating it
 * @param database emit {@code CREATE DATABASE IF NOT EXISTS} and {@code USE} for this database, or null
 * @param batchBytes the largest INSERT statement, in bytes, before a new one starts (a single larger row still gets
 *     its own statement)
 * @param stamp put the generation time in the header, which makes the dump differ on every run
 */
public record MySqlOptions(
        MySqlDialect dialect, String collation, boolean dropExisting, String database, int batchBytes, boolean stamp) {

    public static final int DEFAULT_BATCH_BYTES = 1 << 20;

    public MySqlOptions {
        Objects.requireNonNull(dialect, "dialect");
        if (batchBytes < 1) {
            throw new IllegalArgumentException("batchBytes must be at least 1");
        }
    }

    public static MySqlOptions of(MySqlDialect dialect) {
        return new MySqlOptions(dialect, null, false, null, DEFAULT_BATCH_BYTES, false);
    }

    /** The collation every table and text column uses. */
    public String effectiveCollation() {
        return collation != null ? collation : dialect.defaultCollation();
    }

    /**
     * Whether the collation compares letters case-insensitively as Access does. With {@code utf8mb4_bin} or a
     * {@code _cs} collation, text that Access treats as equal is distinct, which matters for foreign keys and
     * CHECKs.
     */
    public boolean caseInsensitive() {
        return effectiveCollation().toLowerCase(Locale.ROOT).endsWith("_ci");
    }
}
