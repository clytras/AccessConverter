package com.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.LocalSample;
import com.lytrax.accessconverter.fixtures.LocalSamples;
import com.lytrax.accessconverter.report.Severity;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture.Converted;
import com.lytrax.accessconverter.verify.VerifyResult;
import java.nio.file.Path;
import java.time.Duration;
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
    void northwindKeepsEveryTableAndItsAttachmentsIds() {
        Converted converted =
                SqliteFixture.convert(LocalSample.NORTHWIND_2007.path(), dir.resolve("northwind.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            // v2 lost Orders and Products to the complex-column autonumber (F-15) and 308 FK violations with them
            assertThat(sqlite.strings("SELECT name FROM sqlite_schema WHERE type = 'table'"
                            + " AND name NOT LIKE 'sqlite_%' ORDER BY name"))
                    .hasSize(21);
            assertThat(sqlite.query("PRAGMA foreign_key_check")).isEmpty();
        }
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
