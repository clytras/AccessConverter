package io.lytrax.accessconverter.cli;

import io.lytrax.accessconverter.target.mysql.MySqlDialect;

/** The output formats of {@code convert --to}. */
enum Target {
    /** A {@code .sqlite3} database file (06). */
    sqlite(".sqlite3", null),
    /** A {@code .sql} dump for MySQL 8.0 or later (05). */
    mysql(".sql", MySqlDialect.MYSQL),
    /** A {@code .sql} dump for MariaDB 10.11 or later (05). */
    mariadb(".sql", MySqlDialect.MARIADB),
    /** A {@code .json} document, or with {@code --json-layout ndjson} a directory of {@code .ndjson} files (07). */
    json(".json", null);

    private final String extension;
    private final MySqlDialect dialect;

    Target(String extension, MySqlDialect dialect) {
        this.extension = extension;
        this.dialect = dialect;
    }

    String extension() {
        return extension;
    }

    boolean isMySql() {
        return dialect != null;
    }

    /** The MySQL dialect, or null for a target that isn't a MySQL dump. */
    MySqlDialect dialect() {
        return dialect;
    }

    static Target of(MySqlDialect dialect) {
        return dialect == MySqlDialect.MARIADB ? mariadb : mysql;
    }
}
