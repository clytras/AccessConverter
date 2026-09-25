package io.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.fixtures.Access97Fixture;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.verify.VerifyResult;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Every SQLite output says who wrote it, in the header fields SQLite reserves for that: {@code application_id}
 * ("ACCV") and {@code user_version} (the layout version). With {@code --sqlite-metadata}, {@code _access_export}
 * also names the producer and the source; without it, no table is added. Nothing records a time, so the output stays
 * deterministic.
 */
class SqliteAttributionTest {

    @TempDir
    Path dir;

    @Test
    void theHeaderMarksTheFileAndNoTableIsAddedForIt() {
        var converted = SqliteFixture.convert(Access97Fixture.GR97.file(), dir.resolve("gr97.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            assertThat(sqlite.value("PRAGMA application_id")).isEqualTo(0x41434356);
            assertThat(sqlite.value("PRAGMA user_version")).isEqualTo(1);
            assertThat(sqlite.strings("SELECT name FROM sqlite_schema WHERE name LIKE '\\_access%' ESCAPE '\\'"))
                    .isEmpty();
        }
        assertThat(converted.verified().differences()).isEmpty();
    }

    @Test
    void theMetadataOptionNamesTheProducerAndTheSource() {
        var converted = SqliteFixture.convert(
                Access97Fixture.GR97.file(), dir.resolve("meta.sqlite3"), new SqliteOptions(false, false, true, false));
        try (Sqlite sqlite = converted.open()) {
            assertThat(sqlite.query("SELECT producer, format_version, source_file, source_format, source_code_page,"
                            + " source_charset FROM _access_export"))
                    .containsExactly(List.of("AccessConverter", 1, "gr97.mdb", "V1997", 1253, "windows-1253"));
        }
        assertThat(converted.verified().differences()).isEmpty();
    }

    @Test
    void verifyNoticesAFileItDidNotWrite() throws IOException {
        Path output = dir.resolve("other.sqlite3");
        var converted = SqliteFixture.convert(Access97Fixture.GR97.file(), output);
        try (Sqlite sqlite = converted.open()) {
            sqlite.execute("PRAGMA application_id = 0");
        }
        VerifyResult result;
        try (AccessSource db = AccessSource.open(Access97Fixture.GR97.file())) {
            result = SqliteVerifier.verify(db, converted.plan(), output);
        }
        assertThat(result.differences())
                .anySatisfy(d -> assertThat(d.toString()).contains("PRAGMA application_id"));
    }
}
