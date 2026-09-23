package com.lytrax.accessconverter.target.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.cli.Main;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.fixtures.LargeFixture;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The speed and memory budgets of 03 and 09 for a dump: at least 50,000 rows a second end to end, and a million rows
 * in a quarter of a gigabyte of heap, because rows stream and no table is held in memory (F-20).
 *
 * <p>Tagged {@code perf}, so it runs with {@code -Pperf} rather than on every build.
 */
@Tag("perf")
class MySqlPerformanceTest {

    private static final Duration BUDGET = Duration.ofSeconds(10);

    private static final long ROWS_PER_SECOND = 50_000;

    @TempDir
    static Path dir;

    @Test
    void theLargestCorpusDatabaseConvertsWellInsideTheBudget() {
        Path source = CorpusFile.get("jackcess/V2010/indexCodesV2010.accdb").file();
        long started = System.nanoTime();
        MySqlFixture.convert(source, dir.resolve("indexCodes.sql"), MySqlDialect.MYSQL);
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        assertThat(took).isLessThan(BUDGET);
    }

    @Test
    void aMillionRowsStreamThroughAQuarterOfAGigabyte() throws Exception {
        Path source = LargeFixture.path();
        Path output = dir.resolve("large.sql");
        // Memory is what this measures, so it needs its own JVM: -Xmx256m, as 09 asks
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m",
                "-cp",
                System.getProperty("java.class.path"),
                Main.class.getName(),
                "convert",
                "--to",
                "mysql",
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
        long rows = 0;
        try (BufferedReader dump = Files.newBufferedReader(output, StandardCharsets.UTF_8)) {
            for (String line = dump.readLine(); line != null; line = dump.readLine()) {
                if (line.startsWith("(")) {
                    rows++;
                }
            }
        }
        assertThat(rows).isEqualTo(LargeFixture.ROWS);
        long rowsPerSecond = LargeFixture.ROWS * 1000L / Math.max(1, took.toMillis());
        assertThat(rowsPerSecond)
                .as("%d rows in %s (%d rows/s), log: %s", LargeFixture.ROWS, took, rowsPerSecond, log)
                .isGreaterThanOrEqualTo(ROWS_PER_SECOND);
    }
}
