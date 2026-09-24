package com.lytrax.accessconverter.target.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * How {@code verify} reads a server's spelling of what the dump declared. Every reported form here is one a server
 * of the matrix returned (MySQL 8.0/8.4, MariaDB 10.11/11.4/11.8).
 */
class MySqlVerifierTest {

    @Test
    void typesAreComparedWithoutDisplayWidths() {
        assertThat(MySqlVerifier.expectedType("BOOLEAN")).isEqualTo("tinyint(1)");
        assertThat(MySqlVerifier.actualType("tinyint(1)")).isEqualTo("tinyint(1)");
        assertThat(MySqlVerifier.expectedType("TINYINT UNSIGNED"))
                .isEqualTo(MySqlVerifier.actualType("tinyint(3) unsigned"));
        assertThat(MySqlVerifier.expectedType("INT")).isEqualTo(MySqlVerifier.actualType("int(11)"));
        assertThat(MySqlVerifier.expectedType("INT")).isEqualTo(MySqlVerifier.actualType("int"));
        assertThat(MySqlVerifier.expectedType("VARCHAR(50)")).isEqualTo(MySqlVerifier.actualType("varchar(50)"));
        assertThat(MySqlVerifier.expectedType("DECIMAL(19,4)")).isEqualTo(MySqlVerifier.actualType("decimal(19,4)"));
    }

    @Test
    void literalDefaultsAreComparedByValue() {
        // MySQL reports the value, MariaDB the quoted literal
        assertThat(MySqlVerifier.sameDefault("'x\\'y'", "x'y")).isTrue();
        assertThat(MySqlVerifier.sameDefault("'x\\'y'", "'x''y'")).isTrue();
        assertThat(MySqlVerifier.sameDefault("'2000-01-31 00:00:00'", "2000-01-31 00:00:00"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault("'2000-01-31 00:00:00'", "'2000-01-31 00:00:00'"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault("0", "0")).isTrue();
        assertThat(MySqlVerifier.sameDefault("-1.5", "-1.50")).isTrue();
        assertThat(MySqlVerifier.sameDefault("'Greece'", "Greecf")).isFalse();
        // MariaDB reports a nullable column without a default as NULL
        assertThat(MySqlVerifier.sameDefault(null, "NULL")).isTrue();
        assertThat(MySqlVerifier.sameDefault(null, null)).isTrue();
        assertThat(MySqlVerifier.sameDefault("0", null)).isFalse();
    }

    @Test
    void expressionDefaultsAreComparedAfterTheServersSpellingsAreUndone() {
        String guid = MySqlExpressions.RANDOM_GUID;
        assertThat(MySqlVerifier.sameDefault(
                        guid, "concat(_utf8mb4\\'{\\',convert(upper(uuid()) using utf8mb4),_utf8mb4\\'}\\')"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault(guid, "concat(_latin1\\'{\\',upper(uuid()),_latin1\\'}\\')"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault(guid, "concat('{',ucase(uuid()),'}')"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault(MySqlExpressions.TODAY, "curdate()"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault(
                        MySqlExpressions.TIME_OF_DAY, "timestamp(_latin1\\'1899-12-30\\',curtime())"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault(MySqlExpressions.TIME_OF_DAY, "timestamp('1899-12-30',curtime())"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault("CURRENT_TIMESTAMP", "current_timestamp()"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault("CURRENT_TIMESTAMP(3)", "CURRENT_TIMESTAMP(3)"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault("('abc')", "_utf8mb4\\'abc\\'")).isTrue();
        assertThat(MySqlVerifier.sameDefault("('abc')", "'abc'")).isTrue();
        assertThat(MySqlVerifier.sameDefault("CURRENT_TIMESTAMP(3)", "curdate()"))
                .isFalse();
        // A Random autonumber's default, as MySQL 8 (fully parenthesized) and MariaDB (as written) report it
        String random = MySqlExpressions.RANDOM_INT;
        assertThat(MySqlVerifier.sameDefault(
                        random, "(((floor((rand() * 65536)) * 65536) + floor((rand() * 65536))) - 2147483648)"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault(
                        random, "floor(rand() * 65536) * 65536 + floor(rand() * 65536) - 2147483648"))
                .isTrue();
        assertThat(MySqlVerifier.sameDefault(random, "floor(rand() * 65536) * 65536 - 2147483648"))
                .isFalse();
    }
}
