package io.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.util.LibraryLoaderUtil;

/**
 * When sqlite-jdbc can't load its native library, as in a container with a read-only root filesystem where it can't
 * unpack it into /tmp, the conversion fails with our one {@code error:} line (02, exit codes): the output, then that
 * the library couldn't be loaded, from where, and what to do. sqlite-jdbc's own log records, each a timestamped line
 * and a stack trace, stay off unless {@code --verbose}.
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

    /** Converts to {@code output} in a JVM of its own with {@code properties}; returns its one error line. */
    private String failingConversion(Path output, String... properties) throws Exception {
        List<String> command = new ArrayList<>(
                List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString()));
        command.addAll(List.of(properties));
        command.addAll(List.of(
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
                GeneratedFixture.HUNDRED_ROWS.path().toString()));
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
        assertThat(output).doesNotExist();
        assertThat(output.resolveSibling(output.getFileName() + ".partial")).doesNotExist();
        assertThat(err.lines()).as(err).hasSize(1);
        return err.lines().findFirst().orElseThrow();
    }

    @Test
    void aLibraryThatCantBeUnpackedIsOneErrorLine() throws Exception {
        Path notADirectory = Files.writeString(dir.resolve("tmp"), "a file, not a directory");
        Path output = dir.resolve("out.sqlite3");
        assertThat(failingConversion(output, "-Dorg.sqlite.tmpdir=" + notADirectory))
                .isEqualTo("error: " + output + ": the SQLite library could not be unpacked into " + notADirectory
                        + " and loaded from there: make org.sqlite.tmpdir a writable folder that allows running"
                        + " programs (java -Dorg.sqlite.tmpdir=<folder>, or"
                        + " ACCESSCONVERTER_JAVA_OPTS=-Dorg.sqlite.tmpdir=<folder> with the runtime image)");
    }

    /** As the container, and runtime images that hold the library, run it: with the library missing there. */
    @Test
    void aLibraryThatIsntWhereItIsExpectedIsOneErrorLine() throws Exception {
        Path notADirectory = Files.writeString(dir.resolve("tmp"), "a file, not a directory");
        Path empty = Files.createDirectory(dir.resolve("native"));
        Path output = dir.resolve("out.sqlite3");
        assertThat(failingConversion(
                        output,
                        "-Dorg.sqlite.tmpdir=" + notADirectory,
                        "-Dorg.sqlite.lib.path=" + empty,
                        "-Dorg.sqlite.lib.name=" + LibraryLoaderUtil.getNativeLibName()))
                .isEqualTo("error: " + output + ": the SQLite library could not be loaded from "
                        + empty.resolve(LibraryLoaderUtil.getNativeLibName()) + ", nor unpacked into " + notADirectory
                        + ": the installation may be damaged or for another platform; reinstall it, or make"
                        + " org.sqlite.tmpdir a writable folder that allows running programs");
    }
}
