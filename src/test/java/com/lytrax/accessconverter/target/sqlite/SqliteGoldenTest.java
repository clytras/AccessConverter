package com.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.Golden;
import com.lytrax.accessconverter.fixtures.Access97Fixture;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.target.ConvertOptions;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Golden files for the SQLite target (09, Test layers): the exact DDL and the exact literals, compared byte for
 * byte. CI runs this on Linux, Windows and macOS under two locales and time zones, which is 06's portability
 * criterion. Regenerate with {@code ./mvnw test -Dgolden.update=true} and review the diff.
 */
class SqliteGoldenTest {

    @TempDir
    static Path dir;

    /** Small databases whose whole content is pinned: keys, indexes, constraints, values and escaping. */
    static Stream<Case> dumps() {
        return Stream.of(
                generated(GeneratedFixture.HUNDRED_ROWS),
                generated(GeneratedFixture.TEXT_EDGE),
                generated(GeneratedFixture.UNIQUE_PROBE),
                access97(Access97Fixture.GR97),
                corpus("jackcess/V2010/calcFieldV2010.accdb"),
                corpus("jackcess/V2019/extDateV2019.accdb"),
                corpus("jackcess/V2010/indexPropertiesV2010.accdb"),
                corpus("jackcess/V2010/indexV2010.accdb"),
                corpus("jackcess/V2010/complexDataV2010.accdb"),
                corpus("jackcess/V1997/common2V1997.mdb"));
    }

    /** Databases whose schema is pinned but whose data is far too large for a golden file. */
    static Stream<Case> schemas() {
        return Stream.of(generated(GeneratedFixture.SCHEMA_FIDELITY), corpus("jackcess/V2007/blobV2007.accdb"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("dumps")
    void theWholeDatabaseMatchesItsGoldenFile(Case database) {
        Path output = convert(database, SqliteOptions.DEFAULT, "");
        Golden.assertMatches("sqlite/" + database.id() + ".sql", Sqlite.dump(output));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("schemas")
    void theSchemaMatchesItsGoldenFile(Case database) {
        Path output = convert(database, SqliteOptions.DEFAULT, "");
        Golden.assertMatches("sqlite/" + database.id() + ".schema.sql", Sqlite.schema(output));
    }

    @Test
    void theSqliteOptionsChangeTheSchemaInTheirOwnWays() {
        Case fixture = generated(GeneratedFixture.SCHEMA_FIDELITY);
        Golden.assertMatches(
                "sqlite/" + fixture.id() + ".strict.schema.sql",
                Sqlite.schema(convert(fixture, new SqliteOptions(true, false, false, false), "-strict")));
        Golden.assertMatches(
                "sqlite/" + fixture.id() + ".metadata.schema.sql",
                Sqlite.schema(convert(fixture, new SqliteOptions(false, true, true, false), "-metadata")));
    }

    /** The dump a real {@code sqlite3} writes must match too; only run when {@code -Dsqlite3.cli} names one. */
    @Test
    void theSqlite3CommandDumpsTheSameDatabaseTwice() {
        Case fixture = generated(GeneratedFixture.SCHEMA_FIDELITY);
        String first = Sqlite.dumpWithCli(convert(fixture, SqliteOptions.DEFAULT, "-cli1"));
        org.junit.jupiter.api.Assumptions.assumeTrue(first != null, "no -Dsqlite3.cli");
        String second = Sqlite.dumpWithCli(convert(fixture, SqliteOptions.DEFAULT, "-cli2"));
        assertThat(first).isEqualTo(second);
    }

    private static Path convert(Case database, SqliteOptions options, String suffix) {
        Path output = dir.resolve(database.id().replace('/', '_') + suffix + ".sqlite3");
        return SqliteFixture.convert(database.file(), output, database.options(), ConvertOptions.DEFAULT, options)
                .file();
    }

    /** @param id the golden file's path under {@code golden/sqlite/}, which is the fixture's corpus id */
    record Case(String id, Path file, OpenOptions options) {
        @Override
        public String toString() {
            return id;
        }
    }

    private static Case generated(GeneratedFixture fixture) {
        return new Case("generated/" + fixture.fileName(), fixture.path(), OpenOptions.DEFAULT);
    }

    private static Case access97(Access97Fixture fixture) {
        return new Case("access97/" + fixture.file().getFileName(), fixture.file(), fixture.openOptions());
    }

    private static Case corpus(String id) {
        CorpusFile file = CorpusFile.get(id);
        return new Case(file.id(), file.file(), file.openOptions());
    }
}
