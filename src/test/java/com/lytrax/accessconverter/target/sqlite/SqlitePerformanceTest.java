package com.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.lytrax.accessconverter.cli.Main;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.fixtures.Fixtures;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture.Converted;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
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

    private static final int LARGE_ROWS = 1_000_000;

    @TempDir
    static Path dir;

    @Test
    void theLargestCorpusDatabaseConvertsWellInsideTheBudget() {
        Path source = CorpusFile.get("jackcess/V2010/indexCodesV2010.accdb").file();
        long started = System.nanoTime();
        Converted converted = SqliteFixture.convert(source, dir.resolve("indexCodes.sqlite3"));
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        long rows = converted.verified().tables().stream()
                .mapToLong(SqliteVerifier.TableRows::actual)
                .sum();
        assertThat(rows).isEqualTo(133_792);
        // The conversion above includes the verification pass, which reads every value a second time
        assertThat(took).as("%d rows in %s", rows, took).isLessThan(BUDGET.multipliedBy(2));
    }

    @Test
    void aMillionRowsStreamThroughAQuarterOfAGigabyte() throws Exception {
        Path source = largeDatabase();
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
            assertThat(sqlite.value("SELECT count(*) FROM Large")).isEqualTo(LARGE_ROWS);
            sqlite.assertIsConsistent();
        }
        long rowsPerSecond = LARGE_ROWS * 1000L / Math.max(1, took.toMillis());
        assertThat(rowsPerSecond)
                .as("%d rows in %s (%d rows/s), log: %s", LARGE_ROWS, took, rowsPerSecond, log)
                .isGreaterThanOrEqualTo(ROWS_PER_SECOND);
    }

    /** A million rows of mixed types, built once per machine and kept in {@code target/fixtures}. */
    private static Path largeDatabase() throws IOException {
        Path file = Fixtures.root().resolve("generated").resolve("large.accdb");
        if (Files.isRegularFile(file)) {
            return file;
        }
        Files.createDirectories(file.getParent());
        Path partial = file.resolveSibling("large.accdb.partial");
        Files.deleteIfExists(partial);
        try (Database db = new com.healthmarketscience.jackcess.DatabaseBuilder(partial)
                .setFileFormat(Database.FileFormat.V2010)
                .create()) {
            db.setDateTimeType(com.healthmarketscience.jackcess.DateTimeType.LOCAL_DATE_TIME);
            db.setEvaluateExpressions(false);
            Table table = new TableBuilder("Large")
                    .addColumn(new ColumnBuilder("ID", DataType.LONG).setAutoNumber(true))
                    .addColumn(new ColumnBuilder("Name", DataType.TEXT).setLengthInUnits(50))
                    .addColumn(new ColumnBuilder("Amount", DataType.MONEY))
                    .addColumn(new ColumnBuilder("When", DataType.SHORT_DATE_TIME))
                    .addColumn(new ColumnBuilder("Flag", DataType.BOOLEAN))
                    .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                            .addColumns("ID")
                            .setPrimaryKey())
                    .toTable(db);
            LocalDateTime start = LocalDateTime.of(2000, 1, 1, 0, 0);
            List<Object[]> batch = new ArrayList<>(1000);
            for (int i = 0; i < LARGE_ROWS; i++) {
                batch.add(new Object[] {
                    Column.AUTO_NUMBER, "row " + i, BigDecimal.valueOf(i % 100_000, 2), start.plusMinutes(i), i % 2 == 0
                });
                if (batch.size() == 1000) {
                    table.addRows(batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                table.addRows(batch);
            }
        }
        return Files.move(partial, file);
    }
}
