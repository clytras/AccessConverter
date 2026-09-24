package com.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.complex.Attachment;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.lytrax.accessconverter.fixtures.LocalSample;
import com.lytrax.accessconverter.fixtures.LocalSamples;
import com.lytrax.accessconverter.report.Severity;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.target.BinaryMode;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture.Converted;
import com.lytrax.accessconverter.value.CanonicalText;
import com.lytrax.accessconverter.verify.VerifyResult;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tier C: the maintainer's real databases (gitignored, so only on their machine, with {@code -Plocal-samples}).
 * These are the files v2's SQLite export lost tables and rows in, and the ones the speed budget was measured on.
 */
@LocalSamples
class SqliteLocalSamplesTest {

    @TempDir
    static Path dir;

    @ParameterizedTest
    @EnumSource(LocalSample.class)
    void convertsAndVerifiesEverySample(LocalSample sample) {
        Converted converted = SqliteFixture.convert(sample.path(), dir.resolve(sample.name() + ".sqlite3"));

        try (Sqlite sqlite = converted.open()) {
            sqlite.assertIsConsistent();
        }
        assertThat(converted.verified().differences()).isEmpty();
        assertThat(converted.issues().list()).noneMatch(issue -> issue.severity() == Severity.ERROR);
    }

    @Test
    void northwindKeepsEveryTableAndItsComplexValues() {
        Converted converted =
                SqliteFixture.convert(LocalSample.NORTHWIND_2007.path(), dir.resolve("northwind.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            // v2 lost Orders and Products to the complex-column autonumber (F-15) and 308 FK violations with them.
            // 21 Access tables, and a child table for each of its five attachment columns and its multi-value column
            // (08); the attachment tables are empty because the sample holds no attachments (measured: its hidden
            // attachment tables have no rows).
            assertThat(sqlite.strings("SELECT name FROM sqlite_schema WHERE type = 'table'"
                            + " AND name NOT LIKE 'sqlite_%' ORDER BY name"))
                    .hasSize(27);
            assertThat(converted.plan().tables().stream().filter(t -> t.source().isComplexChild()))
                    .hasSize(6);
            long values = converted.plan().tables().stream()
                    .filter(t -> t.source().isComplexChild())
                    .mapToLong(t -> ((Number) sqlite.value("SELECT count(*) FROM \"" + t.name() + "\"")).longValue())
                    .sum();
            assertThat(values)
                    .as("the supplier ids of the products (multi-value)")
                    .isEqualTo(50);
            assertThat(sqlite.query("PRAGMA foreign_key_check")).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource(LocalSample.class)
    void convertsAndVerifiesEverySampleWithFilesAndExtraction(LocalSample sample) {
        Converted converted = SqliteFixture.convert(
                sample.path(),
                dir.resolve(sample.name() + "-files.sqlite3"),
                OpenOptions.DEFAULT,
                ConvertOptions.DEFAULT.withBinary(BinaryMode.FILES, true, true),
                SqliteOptions.DEFAULT);

        try (Sqlite sqlite = converted.open()) {
            sqlite.assertIsConsistent();
        }
        assertThat(converted.verified().differences()).isEmpty();
        assertThat(converted.issues().list()).noneMatch(issue -> issue.severity() == Severity.ERROR);
    }

    @Test
    void allTypesKeepsItsFourAttachmentsExactly() throws IOException {
        Converted converted = SqliteFixture.convert(LocalSample.ALL_TYPES.path(), dir.resolve("allTypes.sqlite3"));
        List<String> expected = new ArrayList<>();
        try (Database db = new DatabaseBuilder(LocalSample.ALL_TYPES.path())
                .setReadOnly(true)
                .open()) {
            for (Row row : db.getTable("testTable")) {
                for (Attachment a : ((ComplexValueForeignKey) row.get("attachment")).getAttachments()) {
                    expected.add(a.getFileName() + " " + CanonicalText.sha256(a.getFileData()));
                }
            }
        }
        assertThat(expected).hasSize(4);
        try (Sqlite sqlite = converted.open()) {
            List<String> actual = new ArrayList<>();
            for (List<Object> row : sqlite.query("SELECT file_name, file_data FROM testTable_attachment")) {
                actual.add(row.get(0) + " " + CanonicalText.sha256((byte[]) row.get(1)));
            }
            assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
        }
        assertThat(converted.verified().differences()).isEmpty();
    }

    @Test
    void marketBasketConvertsInsideTheBudget() {
        long started = System.nanoTime();
        Converted converted =
                SqliteFixture.convert(LocalSample.MARKET_BASKET.path(), dir.resolve("marketBasket.sqlite3"));
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        long rows = converted.verified().tables().stream()
                .mapToLong(VerifyResult.TableRows::actual)
                .sum();
        assertThat(rows).isEqualTo(262_704);
        // 06's budget is ten seconds for the conversion; this also verifies every value, a second full read
        assertThat(took).as("%d rows in %s", rows, took).isLessThan(Duration.ofSeconds(20));
    }

    @Test
    void theGreekAccess97NorthwindKeepsItsForeignKeys() {
        Converted converted = SqliteFixture.convert(LocalSample.NORTHWIND_97.path(), dir.resolve("nw97.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            // Jackcess's index seek misses rows in this file; the orphan check falls back to a scan, so the
            // relationships are still foreign keys instead of being dropped as orphaned
            assertThat(sqlite.value("SELECT count(*) FROM pragma_foreign_key_list('Orders')"))
                    .isEqualTo(3);
            assertThat(sqlite.value("SELECT count(*) FROM pragma_foreign_key_list('Order Details')"))
                    .isEqualTo(2);
            sqlite.assertIsConsistent();
        }
    }
}
