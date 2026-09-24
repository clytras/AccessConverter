package com.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.target.sqlite.Sqlite;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** {@code --binary}, {@code --ole-extract} and {@code --include-version-history} from the command line (02, 08). */
class BinaryOptionsTest {

    @TempDir
    Path dir;

    @Test
    void filesModeWritesTheFilesNextToTheOutputAndVerifies() throws IOException {
        Path input = dir.resolve("fidelity.accdb");
        Files.copy(GeneratedFixture.SCHEMA_FIDELITY.path(), input);

        Cli cli = Cli.run(
                "convert", "--to", "sqlite", "--binary", "files", "--ole-extract", "--verify", input.toString());

        assertThat(cli.exitCode()).as(cli.out() + cli.err()).isEqualTo(ExitCodes.WARNINGS);
        Path files = dir.resolve("fidelity.sqlite3-files");
        assertThat(files.resolve("Files/Doc__content/1-hello.txt")).hasContent("hello ole");
        assertThat(files.resolve("Files/Img/1.png")).exists();
        try (Sqlite sqlite = Sqlite.open(dir.resolve("fidelity.sqlite3"))) {
            assertThat(sqlite.value("SELECT Doc__content FROM Files WHERE FileID = 1"))
                    .isEqualTo("fidelity.sqlite3-files/Files/Doc__content/1-hello.txt");
        }
        String report = Files.readString(dir.resolve("fidelity.sqlite3.report.json"), StandardCharsets.UTF_8);
        assertThat(report)
                .contains("\"binary\": \"files\"", "\"oleExtract\": \"true\"", "\"includeVersionHistory\": \"false\"")
                .doesNotContain("VERIFY_DIFFERENCE");

        // verify needs the same options, and finds the files through the paths
        Cli verify = Cli.run(
                "verify",
                input.toString(),
                dir.resolve("fidelity.sqlite3").toString(),
                "--binary",
                "files",
                "--ole-extract");
        assertThat(verify.exitCode()).as(verify.out() + verify.err()).isIn(ExitCodes.OK, ExitCodes.WARNINGS);
        Files.writeString(files.resolve("Files/Doc__content/1-hello.txt"), "changed");
        Cli changed = Cli.run(
                "verify",
                input.toString(),
                dir.resolve("fidelity.sqlite3").toString(),
                "--binary",
                "files",
                "--ole-extract");
        assertThat(changed.exitCode()).isEqualTo(ExitCodes.FAILED);
    }

    @Test
    void anExistingFilesDirectoryIsNeverReplacedWithoutBeingAskedTo() throws IOException {
        Path input = dir.resolve("fidelity.accdb");
        Files.copy(GeneratedFixture.SCHEMA_FIDELITY.path(), input);
        Path files = Files.createDirectories(dir.resolve("fidelity.json-files"));
        Files.writeString(files.resolve("mine.txt"), "keep");

        Cli refused = Cli.run("convert", "--to", "json", "--binary", "files", input.toString());
        assertThat(refused.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(refused.err()).contains("fidelity.json-files exists; pass --overwrite");
        assertThat(files.resolve("mine.txt")).hasContent("keep");
        assertThat(dir.resolve("fidelity.json")).doesNotExist();

        Cli replaced = Cli.run("convert", "--to", "json", "--binary", "files", "--overwrite", input.toString());
        assertThat(replaced.exitCode()).as(replaced.err()).isEqualTo(ExitCodes.OK);
        assertThat(files.resolve("mine.txt")).doesNotExist();
        assertThat(files.resolve("Files/Doc/1.bin")).exists();
    }

    @Test
    void omitModeAndVersionHistoryOnMySql() throws IOException {
        Path input = dir.resolve("complex.accdb");
        Files.copy(CorpusFile.get("jackcess/V2010/complexDataV2010.accdb").file(), input);

        Cli cli =
                Cli.run("convert", "--to", "mysql", "--binary", "omit", "--include-version-history", input.toString());

        assertThat(cli.exitCode()).as(cli.out() + cli.err()).isEqualTo(ExitCodes.WARNINGS);
        String dump = Files.readString(dir.resolve("complex.sql"), StandardCharsets.UTF_8);
        assertThat(dump)
                .contains("CREATE TABLE `Table1_VersionHistory_F5F8918F-0A3F-4DA9-AE71-184EE5012880`")
                .contains("`file_data` BIGINT")
                .contains("(1, 2, 'test_data.txt', 'txt', 38, 38, NULL, NULL, NULL)");
        String report = Files.readString(dir.resolve("complex.sql.report.json"), StandardCharsets.UTF_8);
        assertThat(report).contains("\"code\": \"BINARY_OMITTED\"").doesNotContain("VERSION_HISTORY_SKIPPED");
    }

    @Test
    void anUnknownModeIsAUsageError() {
        Cli cli = Cli.run(
                "convert",
                "--to",
                "sqlite",
                "--binary",
                "somewhere",
                GeneratedFixture.HUNDRED_ROWS.path().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.USAGE);
    }
}
