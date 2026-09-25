package io.lytrax.accessconverter.target.json;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.cli.Main;
import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.fixtures.LargeFixture;
import io.lytrax.accessconverter.target.json.JsonFixture.Converted;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * 07's memory criterion and 09's budgets: a generated million-row table exports under {@code -Xmx128m} (v2 built the
 * whole document in memory: F-20), at 50,000 rows a second or better, and {@code --verify} reads it back under the same
 * limit, since the verifier streams too.
 *
 * <p>Tagged {@code perf}, so it runs with {@code -Pperf} rather than on every build.
 */
@Tag("perf")
class JsonPerformanceTest {

    private static final Duration BUDGET = Duration.ofSeconds(10);

    private static final long ROWS_PER_SECOND = 50_000;

    @TempDir
    static Path dir;

    @Test
    void theLargestCorpusDatabaseExportsWellInsideTheBudget() {
        Path source = CorpusFile.get("jackcess/V2010/indexCodesV2010.accdb").file();
        long started = System.nanoTime();
        Converted converted = JsonFixture.convert(source, dir.resolve("indexCodes.json"));
        Duration took = Duration.ofNanos(System.nanoTime() - started);

        assertThat(converted.outcome().rowsWritten()).isEqualTo(133_792);
        assertThat(took).as("%s", took).isLessThan(BUDGET);
    }

    @ParameterizedTest
    @ValueSource(strings = {"document", "ndjson"})
    void aMillionRowsStreamThroughAnEighthOfAGigabyte(String layout) throws Exception {
        Path source = LargeFixture.path();
        Path output = dir.resolve(layout.equals("document") ? "large.json" : "large-ndjson");
        // Memory is what this measures, so it needs its own JVM: -Xmx128m, as 07 asks
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx128m",
                "-cp",
                System.getProperty("java.class.path"),
                Main.class.getName(),
                "convert",
                "--to",
                "json",
                "--json-layout",
                layout,
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
        assertThat(rows(output)).isEqualTo(LargeFixture.ROWS);
        long rowsPerSecond = LargeFixture.ROWS * 1000L / Math.max(1, took.toMillis());
        assertThat(rowsPerSecond)
                .as("%d rows in %s (%d rows/s), log: %s", LargeFixture.ROWS, took, rowsPerSecond, log)
                .isGreaterThanOrEqualTo(ROWS_PER_SECOND);

        List<String> verify = new ArrayList<>(command.subList(0, 5));
        verify.addAll(List.of("verify", source.toString(), output.toString()));
        Process verifying = new ProcessBuilder(verify).redirectErrorStream(true).start();
        String verified = new String(verifying.getInputStream().readAllBytes());
        assertThat(verifying.waitFor()).as(verified).isIn(0, 1);
        assertThat(verified).contains("verify: the output matches the source");
    }

    /** The rows of {@code Large}: the lines of its ndjson file, or the document's row lines. */
    private static long rows(Path output) throws IOException {
        Path file = Files.isDirectory(output) ? output.resolve("Large.ndjson") : output;
        long rows = 0;
        try (BufferedReader lines = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            for (String line = lines.readLine(); line != null; line = lines.readLine()) {
                if (line.startsWith("{\"ID\":") || line.startsWith("      {\"ID\":")) {
                    rows++;
                }
            }
        }
        return rows;
    }
}
