package com.lytrax.accessconverter.cli;

import com.lytrax.accessconverter.target.mysql.MySqlDialect;

/** The output formats of {@code convert --to}. */
enum Target {
    /** A {@code .sqlite3} database file (06). */
    sqlite(".sqlite3", null),
    /** A {@code .sql} dump for MySQL 8.0 or later (05). */
    mysql(".sql", MySqlDialect.MYSQL),
    /** A {@code .sql} dump for MariaDB 10.11 or later (05). */
    mariadb(".sql", MySqlDialect.MARIADB);

    private final String extension;
    private final MySqlDialect dialect;

    Target(String extension, MySqlDialect dialect) {
        this.extension = extension;
        this.dialect = dialect;
    }

    String extension() {
        return extension;
    }

    /** The MySQL dialect, or null for a target that isn't a MySQL dump. */
    MySqlDialect dialect() {
        return dialect;
    }

    static Target of(MySqlDialect dialect) {
        return dialect == MySqlDialect.MARIADB ? mariadb : mysql;
    }
}
