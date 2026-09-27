package io.lytrax.accessconverter.it;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The packaged jar, run as users run it ({@code java -jar}), on the JDK the build runs on: a successful conversion to
 * every target, and a verification, leave standard error empty. Java 24 and later print warnings when a library loads
 * native code (sqlite-jdbc) or reaches for {@code sun.misc.Unsafe}, unless the jar's manifest allows it; scripts rely
 * on standard error holding only our own lines. The CI builds on Java 21 and 25.
 *
 * <p>Only {@code java -jar} applies the manifest, so this runs after {@code package}, in every build (the
 * {@code shaded-jar} failsafe execution), not only with {@code -Pintegration}.
 */
class ShadedJarIT {

    @TempDir
    static Path dir;

    static Path jar;

    static Path source;

    @BeforeAll
    static void thePackagedJar() {
        jar = Path.of(System.getProperty("shaded.jar"));
        assertThat(jar).isRegularFile();
        source = GeneratedFixture.SCHEMA_FIDELITY.path();
    }

    @Test
    void convertingToSqliteLeavesStandardErrorEmpty() throws Exception {
        Path output = dir.resolve("out.sqlite3");
        run("convert", "--to", "sqlite", "-o", output.toString(), source.toString());
        run("verify", source.toString(), output.toString());
    }

    @Test
    void convertingToMySqlAndMariaDbLeavesStandardErrorEmpty() throws Exception {
        run("convert", "--to", "mysql", "-o", dir.resolve("out-mysql.sql").toString(), source.toString());
        run("convert", "--to", "mariadb", "-o", dir.resolve("out-mariadb.sql").toString(), source.toString());
    }

    @Test
    void convertingToJsonLeavesStandardErrorEmpty() throws Exception {
        Path output = dir.resolve("out.json");
        run("convert", "--to", "json", "-o", output.toString(), source.toString());
        run("verify", source.toString(), output.toString());
        run(
                "convert",
                "--to",
                "json",
                "--json-layout",
                "ndjson",
                "-o",
                dir.resolve("out-ndjson").toString(),
                source.toString());
    }

    /** Runs the jar and checks it succeeded (exit 0, or 1 for warnings) with nothing on standard error. */
    private static void run(String... args) throws Exception {
        List<String> command = new ArrayList<>(
                List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-jar", jar.toString()));
        command.addAll(List.of(args));
        command.add("--no-progress");
        Path stdout = Files.createTempFile(dir, "stdout", ".txt");
        Path stderr = Files.createTempFile(dir, "stderr", ".txt");
        Process process = new ProcessBuilder(command)
                .redirectOutput(stdout.toFile())
                .redirectError(stderr.toFile())
                .start();
        assertThat(process.waitFor(2, TimeUnit.MINUTES)).isTrue();
        String err = new String(Files.readAllBytes(stderr), StandardCharsets.UTF_8);
        assertThat(process.exitValue())
                .as("%s%n%s", String.join(" ", args), err)
                .isIn(0, 1);
        assertThat(err)
                .as("standard error of %s on Java %s", String.join(" ", args), Runtime.version())
                .isEmpty();
    }
}
