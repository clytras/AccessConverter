package io.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * An output that can't be written fails with exit 2 and one line, {@code error: <output>: <reason>}: the output as the
 * user gave it, never its {@code .partial}, and the reason in words, not a Java class name (02, exit codes).
 */
class OutputErrorTest {

    @TempDir
    Path dir;

    private static Cli convert(String target, Path output, String... options) {
        List<String> args =
                new ArrayList<>(List.of("convert", "--to", target, "--no-progress", "-o", output.toString()));
        args.addAll(List.of(options));
        args.add(GeneratedFixture.HUNDRED_ROWS.path().toString());
        return Cli.run(args.toArray(String[]::new));
    }

    private static void assertOneLine(Cli cli, Path output, String reason) {
        assertThat(cli.exitCode()).as(cli.err()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.out()).isEmpty();
        assertThat(cli.err().lines()).singleElement().isEqualTo("error: " + output + ": " + reason);
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "json", "mysql", "mariadb"})
    void aMissingFolderIsNamed(String target) {
        Path output = dir.resolve("nodir").resolve("out." + target);
        assertOneLine(convert(target, output), output, "the folder " + output.getParent() + " doesn't exist");
        assertThat(dir.resolve("nodir")).doesNotExist();
    }

    @Test
    void aMissingFolderIsNamedForNdjson() {
        Path output = dir.resolve("nodir").resolve("out-ndjson");
        assertOneLine(
                convert("json", output, "--json-layout", "ndjson"),
                output,
                "the folder " + output.getParent() + " doesn't exist");
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "json", "mysql"})
    void aFileWhereTheFolderShouldBeIsNamed(String target) throws IOException {
        Path notAFolder = Files.writeString(dir.resolve("notafolder"), "a file");
        Path output = notAFolder.resolve("out." + target);
        assertOneLine(convert(target, output), output, notAFolder + " is not a folder");
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "json", "mysql"})
    void aFolderWithoutWritePermissionIsNamed(String target) throws IOException {
        assumeTrue(Files.getFileStore(dir).supportsFileAttributeView("posix"), "POSIX permissions");
        Path readOnly = Files.createDirectory(dir.resolve("readonly"));
        Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("r-xr-xr-x"));
        try {
            assumeTrue(!Files.isWritable(readOnly), "not running as root");
            Path output = readOnly.resolve("out." + target);
            assertOneLine(convert(target, output), output, "no permission to write in the folder " + readOnly);
        } finally {
            Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }
}
