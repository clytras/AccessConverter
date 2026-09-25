package io.lytrax.accessconverter.target.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.fixtures.CorpusCase;
import io.lytrax.accessconverter.report.Severity;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.mysql.MySqlFixture.Converted;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Every database of tiers A, B and D converts to both dialects without an error, and the same input gives the same
 * dump every time, byte for byte (03, Determinism). Loading and verifying the dumps needs the servers:
 * {@code MySqlCorpusIT}.
 */
class MySqlCorpusTest {

    @TempDir
    static Path dir;

    static Stream<Arguments> cases() {
        return Stream.of(MySqlDialect.values())
                .flatMap(dialect -> CorpusCase.databases().map(database -> Arguments.of(dialect, database)));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("cases")
    void convertsTheSameWayEveryTime(MySqlDialect dialect, CorpusCase database) throws IOException {
        Converted first = convert(dialect, database, "-1");
        Converted second = convert(dialect, database, "-2");

        assertThat(first.issues().list())
                .as("errors in the conversion report")
                .noneMatch(issue -> issue.severity() == Severity.ERROR);
        assertThat(Files.mismatch(first.file(), second.file())).isEqualTo(-1L);
    }

    private static Converted convert(MySqlDialect dialect, CorpusCase database, String suffix) {
        Path output = dir.resolve(dialect + "_" + database.id().replaceAll("[^A-Za-z0-9.]", "_") + suffix + ".sql");
        return MySqlFixture.convert(
                database.file(), output, database.options(), ConvertOptions.DEFAULT, MySqlOptions.of(dialect));
    }
}
