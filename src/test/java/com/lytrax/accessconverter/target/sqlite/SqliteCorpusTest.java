package com.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.CorpusCase;
import com.lytrax.accessconverter.report.Severity;
import com.lytrax.accessconverter.target.BinaryMode;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture.Converted;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Phase 2's exit criterion: every database Jackcess can read converts to SQLite, the file is consistent
 * ({@code integrity_check} and {@code foreign_key_check}), and {@code verify} finds the schema and every value
 * where the plan says they should be (06, Acceptance criteria).
 */
class SqliteCorpusTest {

    @TempDir
    static Path dir;

    static Stream<CorpusCase> databases() {
        return CorpusCase.databases();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("databases")
    void convertsAndVerifiesEveryDatabase(CorpusCase database) {
        Converted converted = convert(database, "");

        try (Sqlite sqlite = converted.open()) {
            sqlite.assertIsConsistent();
        }
        assertThat(converted.verified().differences()).isEmpty();
        assertThat(converted.verified().matches()).isTrue();
        assertThat(converted.issues().list())
                .as("errors in the conversion report")
                .noneMatch(issue -> issue.severity() == Severity.ERROR);
    }

    /**
     * 08's options on every database: OLE values decoded next to their raw bytes, every Binary, OLE and attachment
     * value in a file of its own, and version history as child tables; the file is still consistent and verifies,
     * files included.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("databases")
    void convertsAndVerifiesWithFilesExtractionAndVersionHistory(CorpusCase database) {
        Path output = dir.resolve(database.id().replace('/', '_') + "-files.sqlite3");
        Converted converted = SqliteFixture.convert(
                database.file(),
                output,
                database.options(),
                ConvertOptions.DEFAULT.withBinary(BinaryMode.FILES, true, true),
                SqliteOptions.DEFAULT);

        try (Sqlite sqlite = converted.open()) {
            sqlite.assertIsConsistent();
        }
        assertThat(converted.verified().differences()).isEmpty();
        assertThat(converted.issues().list())
                .as("errors in the conversion report")
                .noneMatch(issue -> issue.severity() == Severity.ERROR);
    }

    /**
     * The same input gives the same output, every time: the dumps of two conversions are identical. Together with
     * the golden files, which CI compares on Linux, Windows and macOS under two locales, that is 06's portability
     * criterion.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("databases")
    void theOutputIsTheSameOnEveryRun(CorpusCase database) {
        assertThat(Sqlite.dump(convert(database, "-1").file()))
                .isEqualTo(Sqlite.dump(convert(database, "-2").file()));
    }

    private static Converted convert(CorpusCase database, String suffix) {
        Path output = dir.resolve(database.id().replace('/', '_') + suffix + ".sqlite3");
        return SqliteFixture.convert(
                database.file(), output, database.options(), ConvertOptions.DEFAULT, SqliteOptions.DEFAULT);
    }
}
