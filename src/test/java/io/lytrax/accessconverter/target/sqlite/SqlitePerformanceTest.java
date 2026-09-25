package io.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.cli.Main;
import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.fixtures.LargeFixture;
import io.lytrax.accessconverter.target.sqlite.SqliteFixture.Converted;
import io.lytrax.accessconverter.verify.VerifyResult;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The speed and memory budgets of 06 and 09. v2's SQLite export ran one transaction per row and managed about 140
 * rows a second (F-40); v3 writes one transaction per table through a reused prepared statement.
 *
 * <p>Tagged {@code perf}, so it runs with {@code -Pperf} rather than on every build.
 */
@Tag("perf")
class SqlitePerformanceTest {

    /** 06: {@code indexCodesV2010} and MarketBasket each in under ten seconds on a developer machine. */
    private static final Duration BUDGET = Duration.ofSeconds(10);

    /** 09: at least 50,000 rows a second end to end. */
    private static final long ROWS_PER_SECOND = 50_000;

    @TempDir
    static Path dir;

    @Test
    void theLargestCorpusDatabaseConvertsWellInsideTheBudget() {
        Path source = CorpusFile.get("jackcess/V2010/indexCodesV2010.accdb").file();
        long started = System.nanoTime();
        Converted converted = SqliteFixture.convert(source, dir.resolve("indexCodes.sqlite3"));
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        long rows = converted.verified().tables().stream()
                .mapToLong(VerifyResult.TableRows::actual)
                .sum();
        assertThat(rows).isEqualTo(133_792);
        // The conversion above includes the verification pass, which reads every value a second time
        assertThat(took).as("%d rows in %s", rows, took).isLessThan(BUDGET.multipliedBy(2));
    }

    @Test
    void aMillionRowsStreamThroughAQuarterOfAGigabyte() throws Exception {
        Path source = LargeFixture.path();
        Path output = dir.resolve("large.sqlite3");
        // Memory is what this measures, so it needs its own JVM: -Xmx256m, as 09 asks
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m",
                "-cp",
                System.getProperty("java.class.path"),
                Main.class.getName(),
                "convert",
                "--to",
                "sqlite",
                "--no-report",
                "-o",
                output.toString(),
                source.toString()));
        long started = System.nanoTime();
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String log = new String(process.getInputStream().readAllBytes());
        int exitCode = process.waitFor();
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        assertThat(exitCode).as(log).isIn(0, 1);
        try (Sqlite sqlite = Sqlite.open(output)) {
            assertThat(sqlite.value("SELECT count(*) FROM Large")).isEqualTo(LargeFixture.ROWS);
            sqlite.assertIsConsistent();
        }
        long rowsPerSecond = LargeFixture.ROWS * 1000L / Math.max(1, took.toMillis());
        assertThat(rowsPerSecond)
                .as("%d rows in %s (%d rows/s), log: %s", LargeFixture.ROWS, took, rowsPerSecond, log)
                .isGreaterThanOrEqualTo(ROWS_PER_SECOND);
    }
}
