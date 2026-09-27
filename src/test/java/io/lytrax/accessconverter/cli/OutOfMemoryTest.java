package io.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Running out of memory is a failed conversion like any other (02, exit codes): exit 2 and one {@code error:} line
 * that says what to do, nothing on standard output, and no output, {@code .partial} or report left behind. It used to
 * exit 1, which reads as success with warnings, with the JVM's stack trace and the {@code .partial} file left.
 *
 * <p>The heap is what this is about, so the conversion runs in a JVM of its own, too small for the database's one 30 MB
 * value.
 */
class OutOfMemoryTest {

    private static final String HEAP = "-Xmx24m";

    @TempDir
    static Path dir;

    static Path source;

    @BeforeAll
    static void aDatabaseWithOneLargeValue() throws Exception {
        source = dir.resolve("large.accdb");
        try (Database db = new DatabaseBuilder(source.toFile())
                .setFileFormat(Database.FileFormat.V2010)
                .create()) {
            Table table = new TableBuilder("Large")
                    .addColumn(new ColumnBuilder("ID", DataType.LONG))
                    .addColumn(new ColumnBuilder("Payload", DataType.OLE))
                    .toTable(db);
            table.addRow(1, new byte[30 << 20]);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mysql", "sqlite", "json", "sqlite --binary files"})
    void runningOutOfMemoryFailsWithOneLineAndLeavesNothing(String options) throws Exception {
        List<String> words = List.of(options.split(" "));
        String target = words.get(0);
        Path work = Files.createDirectory(dir.resolve(options.replace(" ", "")));
        Path output = work.resolve("out." + target);
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                HEAP,
                // The test classpath has slf4j-api (from the schema validator) and no provider, which sqlite-jdbc
                // would warn about; the jar has neither
                "-Dslf4j.internal.verbosity=ERROR",
                "-cp",
                System.getProperty("java.class.path"),
                Main.class.getName(),
                "convert",
                "--to",
                target,
                "--no-progress",
                "--format-result",
                "json",
                "-o",
                output.toString(),
                source.toString()));
        // --binary files: the directory of value files goes too
        command.addAll(words.subList(1, words.size()));
        Process process = new ProcessBuilder(command).start();
        String out = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String err = new String(process.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode = process.waitFor();

        assertThat(exitCode).as(err).isEqualTo(ExitCodes.FAILED);
        assertThat(out).isEmpty();
        assertThat(err.lines())
                .singleElement()
                .satisfies(line -> assertThat(line)
                        .startsWith("error: large.accdb: memory ran out (Java heap space): give Java a larger heap")
                        .contains("ACCESSCONVERTER_JAVA_OPTS=-Xmx", "--binary files"));
        try (Stream<Path> left = Files.list(work)) {
            assertThat(left).isEmpty();
        }
    }
}
