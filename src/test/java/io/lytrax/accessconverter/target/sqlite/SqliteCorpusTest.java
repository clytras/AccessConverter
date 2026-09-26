package io.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.fixtures.CorpusCase;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Severity;
import io.lytrax.accessconverter.target.BinaryMode;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.IdentifierPolicy;
import io.lytrax.accessconverter.target.sqlite.SqliteFixture.Converted;
import java.nio.file.Path;
import java.util.List;
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
     * {@code --add-primary-key} on every database: each table Access keeps without a key gets an {@code id}, the file
     * is still consistent, verifies with the numbered rows, and every written table now has a primary key.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("databases")
    void convertsAndVerifiesWithAddedPrimaryKeys(CorpusCase database) {
        Path output = dir.resolve(database.id().replace('/', '_') + "-keys.sqlite3");
        Converted converted = SqliteFixture.convert(
                database.file(), output, database.options(), ConvertOptions.DEFAULT, SqliteOptions.DEFAULT, "id");

        try (Sqlite sqlite = converted.open()) {
            sqlite.assertIsConsistent();
            for (var added : converted.issues(IssueCode.PRIMARY_KEY_ADDED)) {
                String table = converted.table(added.table()).name();
                String column = converted.column(added.table(), added.object()).name();
                List<Object> numbers = sqlite.query("SELECT count(*), min(" + IdentifierPolicy.quote(column) + "), max("
                                + IdentifierPolicy.quote(column) + ") FROM " + IdentifierPolicy.quote(table))
                        .get(0);
                List<Long> longs = numbers.stream()
                        .map(n -> n == null ? null : ((Number) n).longValue())
                        .toList();
                long rows = longs.get(0);
                // Numbered 1, 2, 3, ... with no gap
                assertThat(longs).as(table).containsExactly(rows, rows == 0 ? null : 1L, rows == 0 ? null : rows);
            }
        }
        assertThat(converted.plan().tables())
                .allSatisfy(t -> assertThat(t.primaryKey()).as(t.name()).isNotNull());
        assertThat(converted.verified().differences()).isEmpty();
        assertThat(converted.issues(IssueCode.NO_PRIMARY_KEY)).isEmpty();
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

    /**
     * {@code --linked resolve}: the linked tables are written with their back-end's data, the back-end's enforced
     * relationship between them becomes a foreign key, the file is consistent, verifies, and is the same every run.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("io.lytrax.accessconverter.fixtures.CorpusCase#linkedResolved")
    void convertsAndVerifiesWithLinkedTablesResolved(CorpusCase database) {
        Converted converted = convert(database, "-resolved");

        try (Sqlite sqlite = converted.open()) {
            sqlite.assertIsConsistent();
        }
        assertThat(converted.model().tables()).noneMatch(t -> t.isLinked());
        assertThat(converted.model().tables()).anyMatch(t -> t.isResolvedLink());
        assertThat(converted.verified().differences()).isEmpty();
        assertThat(converted.issues().list())
                .as("errors in the conversion report")
                .noneMatch(issue -> issue.severity() == Severity.ERROR);
        assertThat(Sqlite.dump(converted.file()))
                .isEqualTo(Sqlite.dump(convert(database, "-resolved-2").file()));
    }

    private static Converted convert(CorpusCase database, String suffix) {
        Path output = dir.resolve(database.id().replaceAll("[/ ]", "_") + suffix + ".sqlite3");
        return SqliteFixture.convert(
                database.file(), output, database.options(), ConvertOptions.DEFAULT, SqliteOptions.DEFAULT);
    }
}
