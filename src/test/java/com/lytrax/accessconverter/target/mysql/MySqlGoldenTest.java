package com.lytrax.accessconverter.target.mysql;

import com.lytrax.accessconverter.Golden;
import com.lytrax.accessconverter.fixtures.Access97Fixture;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.target.ConvertOptions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Golden files for the MySQL and MariaDB dumps (09, Test layers): the exact DDL and the exact literals, compared byte
 * for byte in both locale runs on three operating systems. Regenerate with {@code ./mvnw test -Dgolden.update=true}
 * and review the diff.
 */
class MySqlGoldenTest {

    @TempDir
    static Path dir;

    /** Small databases whose whole dump is pinned: keys, indexes, constraints, values and escaping. */
    static Stream<Case> dumps() {
        return Stream.of(
                generated(GeneratedFixture.HUNDRED_ROWS),
                generated(GeneratedFixture.TEXT_EDGE),
                generated(GeneratedFixture.UNIQUE_PROBE),
                access97(Access97Fixture.GR97),
                corpus("jackcess/V2010/calcFieldV2010.accdb"),
                corpus("jackcess/V2019/extDateV2019.accdb"),
                corpus("jackcess/V2010/indexV2010.accdb"),
                corpus("jackcess/V2010/complexDataV2010.accdb"),
                corpus("jackcess/V1997/common2V1997.mdb"));
    }

    /** Databases whose schema is pinned but whose data is too large for a golden file. */
    static Stream<Case> schemas() {
        return Stream.of(
                generated(GeneratedFixture.SCHEMA_FIDELITY),
                corpus("jackcess/V2007/blobV2007.accdb"),
                corpus("jackcess/V2010/common1V2010.accdb"),
                corpus("jackcess-encrypt/money2002.mny"));
    }

    static Stream<Arguments> dumpCases() {
        return Stream.of(MySqlDialect.values()).flatMap(d -> dumps().map(c -> Arguments.of(d, c)));
    }

    static Stream<Arguments> schemaCases() {
        return Stream.of(MySqlDialect.values()).flatMap(d -> schemas().map(c -> Arguments.of(d, c)));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("dumpCases")
    void theWholeDumpMatchesItsGoldenFile(MySqlDialect dialect, Case database) {
        Golden.assertMatches(golden(dialect, database, ".sql"), read(convert(database, MySqlOptions.of(dialect), "")));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("schemaCases")
    void theSchemaMatchesItsGoldenFile(MySqlDialect dialect, Case database) {
        Golden.assertMatches(
                golden(dialect, database, ".schema.sql"),
                withoutData(read(convert(database, MySqlOptions.of(dialect), ""))));
    }

    @Test
    void theDumpOptionsChangeTheHeaderInTheirOwnWays() {
        Case fixture = generated(GeneratedFixture.HUNDRED_ROWS);
        MySqlOptions options = new MySqlOptions(MySqlDialect.MYSQL, null, true, "shop", 200, false);
        Golden.assertMatches("mysql/" + fixture.id() + ".options.sql", read(convert(fixture, options, "-options")));
    }

    private static String golden(MySqlDialect dialect, Case database, String suffix) {
        return dialect.name().toLowerCase(Locale.ROOT) + "/" + database.id() + suffix;
    }

    private static Path convert(Case database, MySqlOptions options, String suffix) {
        Path output =
                dir.resolve(options.dialect() + "_" + database.id().replaceAll("[^A-Za-z0-9.]", "_") + suffix + ".sql");
        return MySqlFixture.convert(database.file(), output, database.options(), ConvertOptions.DEFAULT, options)
                .file();
    }

    private static String read(Path dump) {
        try {
            return Files.readString(dump, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The dump without its INSERT statements. */
    private static String withoutData(String dump) {
        StringBuilder kept = new StringBuilder();
        boolean inInsert = false;
        for (String line : dump.split("\n", -1)) {
            if (line.startsWith("INSERT INTO ")) {
                inInsert = true;
            }
            if (!inInsert) {
                kept.append(line).append('\n');
            }
            if (inInsert && line.endsWith(");")) {
                inInsert = false;
            }
        }
        return kept.toString().lines().collect(Collectors.joining("\n", "", "\n"));
    }

    /** @param id the golden file's path under {@code golden/<dialect>/}, which is the fixture's corpus id */
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
