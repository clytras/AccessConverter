package com.lytrax.accessconverter.target.sqlite;

/**
 * The SQLite-specific convert options (06).
 *
 * @param strict emit {@code STRICT} tables: declared types collapse to SQLite's storage classes
 * @param nocase add {@code COLLATE NOCASE} to every text column, making comparisons case-insensitive like Access
 * @param metadata add the opt-in {@code _access_columns}, {@code _access_relationships} and {@code _access_export} tables
 * @param analyze run a full {@code ANALYZE} before closing, not only {@code PRAGMA optimize}
 */
public record SqliteOptions(boolean strict, boolean nocase, boolean metadata, boolean analyze) {

    public static final SqliteOptions DEFAULT = new SqliteOptions(false, false, false, false);
}
