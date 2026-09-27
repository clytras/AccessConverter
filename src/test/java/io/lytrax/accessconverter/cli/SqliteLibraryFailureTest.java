package io.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * When sqlite-jdbc can't load its native library, as in a container with a read-only root filesystem where it can't
 * unpack it into /tmp, the conversion fails with our one {@code error:} line (02, exit codes). sqlite-jdbc's own log
 * records, each a timestamped line and a stack trace, stay off unless {@code --verbose}.
 *
 * <p>The failure needs a JVM of its own: a library loaded once stays loaded, so this JVM can't be made to fail. The
 * directory sqlite-jdbc unpacks into ({@code org.sqlite.tmpdir}) is a regular file there, so nothing can be unpacked.
 */
class SqliteLibraryFailureTest {

    @TempDir
    Path dir;

    /**
     * The classpath as the jar has it: the test classpath has slf4j-api (from the schema validator), through which
     * sqlite-jdbc would log to nowhere, hiding the records the jar prints through java.util.logging.
     */
    private static String withoutSlf4j(String classpath) {
        return Arrays.stream(classpath.split(File.pathSeparator))
                .filter(entry -> !entry.contains("slf4j"))
                .collect(Collectors.joining(File.pathSeparator));
    }

    @Test
    void aLibraryThatCantBeLoadedIsOneErrorLine() throws Exception {
        Path notADirectory = Files.writeString(dir.resolve("tmp"), "a file, not a directory");
        Path output = dir.resolve("out.sqlite3");
        List<String> command = List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dorg.sqlite.tmpdir=" + notADirectory,
                // Java 24 and later warn when a native library is loaded, unless native access is enabled
                "--enable-native-access=ALL-UNNAMED",
                "-cp",
                withoutSlf4j(System.getProperty("java.class.path")),
                Main.class.getName(),
                "convert",
                "--to",
                "sqlite",
                "--no-progress",
                "-o",
                output.toString(),
                GeneratedFixture.HUNDRED_ROWS.path().toString());
        // Into files, not pipes: a child that logs stack traces would fill one pipe while the other is read
        Path stdout = dir.resolve("stdout.txt");
        Path stderr = dir.resolve("stderr.txt");
        Process process = new ProcessBuilder(command)
                .redirectOutput(stdout.toFile())
                .redirectError(stderr.toFile())
                .start();
        assertThat(process.waitFor(2, TimeUnit.MINUTES)).isTrue();
        int exitCode = process.exitValue();
        String out = new String(Files.readAllBytes(stdout), StandardCharsets.UTF_8);
        String err = new String(Files.readAllBytes(stderr), StandardCharsets.UTF_8);

        assertThat(exitCode).as(err).isEqualTo(ExitCodes.FAILED);
        assertThat(out).isEmpty();
        assertThat(err.lines())
                .singleElement()
                .satisfies(line ->
                        assertThat(line).startsWith("error: ").contains("the SQLite output could not be written"));
        assertThat(output).doesNotExist();
        assertThat(dir.resolve("out.sqlite3.partial")).doesNotExist();
    }
}
