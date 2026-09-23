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

/** The {@code convert} command as a user meets it (02, CLI): output naming, the report, exit codes and refusals. */
class ConvertCommandTest {

    @TempDir
    Path dir;

    @Test
    void convertsToASqliteFileNextToTheInputAndWritesAReport() throws IOException {
        Path input = dir.resolve("copy.accdb");
        Files.copy(GeneratedFixture.SCHEMA_FIDELITY.path(), input);

        Cli cli = Cli.run("convert", "--to", "sqlite", input.toString());

        // exit 1: the fixture's autonumber that isn't the primary key is a warning
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.WARNINGS);
        assertThat(cli.err()).isEmpty();
        Path output = dir.resolve("copy.sqlite3");
        assertThat(output).exists();
        assertThat(cli.out()).contains("copy.accdb (V2010) -> ", "11 tables, 18 rows", "1 warning");
        assertThat(cli.out()).contains("PRAGMA foreign_keys = ON");
        Path report = dir.resolve("copy.sqlite3.report.json");
        assertThat(report).exists();
        String json = Files.readString(report, StandardCharsets.UTF_8);
        assertThat(json)
                .contains("\"format\": \"accessconverter-report\"")
                .contains("\"command\": \"convert\"")
                .contains("\"code\": \"AUTOINCREMENT_NOT_PRESERVED\"")
                // Only a consumer can switch foreign keys on, so the report says so too
                .contains("\"code\": \"FOREIGN_KEYS_NEED_PRAGMA\"")
                .contains("\"rowsWritten\": 3");
        // No leftovers: the partial file is renamed, not copied
        assertThat(dir.resolve("copy.sqlite3.partial")).doesNotExist();
    }

    @Test
    void anExistingOutputIsNeverReplacedWithoutBeingAskedTo() throws IOException {
        Path output = dir.resolve("out.sqlite3");
        Files.writeString(output, "not a database");

        Cli refused = Cli.run(
                "convert",
                "--to",
                "sqlite",
                "-o",
                output.toString(),
                GeneratedFixture.HUNDRED_ROWS.path().toString());

        assertThat(refused.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(refused.err()).contains("exists; pass --overwrite");
        assertThat(Files.readString(output)).isEqualTo("not a database");

        Cli replaced = Cli.run(
                "convert",
                "--to",
                "sqlite",
                "--overwrite",
                "-o",
                output.toString(),
                GeneratedFixture.HUNDRED_ROWS.path().toString());
        assertThat(replaced.exitCode()).isEqualTo(ExitCodes.OK);
        try (Sqlite sqlite = Sqlite.open(output)) {
            assertThat(sqlite.value("SELECT count(*) FROM hundred")).isEqualTo(100);
        }
    }

    @Test
    void theResultCanBePrintedAsJsonForScripts() {
        Path output = dir.resolve("json.sqlite3");
        Cli cli = Cli.run(
                "convert",
                "--to",
                "sqlite",
                "--format-result",
                "json",
                "--no-report",
                "-o",
                output.toString(),
                GeneratedFixture.HUNDRED_ROWS.path().toString());

        assertThat(cli.exitCode()).isEqualTo(ExitCodes.OK);
        assertThat(cli.out())
                .startsWith("{\n  \"format\": \"accessconverter-report\"")
                .contains("\"outcome\"");
        assertThat(dir.resolve("json.sqlite3.report.json")).doesNotExist();
    }

    @Test
    void aTableFilterLeavesTheOtherTablesAndTheirRelationshipsOut() {
        Path output = dir.resolve("filtered.sqlite3");
        Cli cli = Cli.run(
                "convert",
                "--to",
                "sqlite",
                "--tables",
                "Customers,Order*",
                "-o",
                output.toString(),
                "--no-report",
                GeneratedFixture.SCHEMA_FIDELITY.path().toString());

        assertThat(cli.exitCode()).isIn(ExitCodes.OK, ExitCodes.WARNINGS);
        try (Sqlite sqlite = Sqlite.open(output)) {
            assertThat(sqlite.strings("SELECT name FROM sqlite_schema WHERE type = 'table'"
                            + " AND name NOT LIKE 'sqlite_stat%' ORDER BY name"))
                    .containsExactly("Customers", "Order Details", "Orders", "sqlite_sequence");
            // The relationship to Shippers can't be a foreign key with Shippers left out
            assertThat(sqlite.sql("Orders")).doesNotContain("Shippers");
        }
    }

    @Test
    void verifyComparesTheOutputWithItsSource() {
        Path output = dir.resolve("verified.sqlite3");
        assertThat(Cli.run(
                                "convert",
                                "--to",
                                "sqlite",
                                "--verify",
                                "--no-report",
                                "-o",
                                output.toString(),
                                GeneratedFixture.SCHEMA_FIDELITY.path().toString())
                        .exitCode())
                .isEqualTo(ExitCodes.WARNINGS);

        Cli verify = Cli.run("verify", GeneratedFixture.SCHEMA_FIDELITY.path().toString(), output.toString());

        assertThat(verify.exitCode()).isIn(ExitCodes.OK, ExitCodes.WARNINGS);
        assertThat(verify.out())
                .contains("verify: the output matches the source")
                .contains("Customers: 3 rows");
    }

    @Test
    void verifyFailsWhenTheOutputWasWrittenWithOtherOptions() {
        Path output = dir.resolve("strict.sqlite3");
        Cli.run(
                "convert",
                "--to",
                "sqlite",
                "--sqlite-strict",
                "--no-report",
                "-o",
                output.toString(),
                GeneratedFixture.HUNDRED_ROWS.path().toString());

        Cli verify = Cli.run("verify", GeneratedFixture.HUNDRED_ROWS.path().toString(), output.toString());

        assertThat(verify.exitCode()).isEqualTo(ExitCodes.FAILED);
        // Comparing the stored statement catches everything, including a STRICT table the options didn't ask for
        assertThat(verify.out())
                .contains("differences")
                .contains("the table's SQL")
                .contains("STRICT");
    }

    @Test
    void verifyOnlyUnderstandsTheOutputsItCanRead() throws IOException {
        Path output = dir.resolve("mystery.bin");
        Files.writeString(output, "not a database");
        Cli verify = Cli.run("verify", GeneratedFixture.HUNDRED_ROWS.path().toString(), output.toString());
        assertThat(verify.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(verify.err()).contains("is not a SQLite file");
    }

    @Test
    void aSourceThatCannotBeReadFailsWithOneLineAndWritesNothing() {
        Path output = dir.resolve("never.sqlite3");
        CorpusFile notADatabase = CorpusFile.all().stream()
                .filter(f -> !f.isDatabase())
                .findFirst()
                .orElseThrow();

        Cli cli = Cli.run(
                "convert",
                "--to",
                "sqlite",
                "-o",
                output.toString(),
                notADatabase.file().toString());

        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.out()).isEmpty();
        assertThat(cli.err().lines())
                .hasSize(1)
                .allSatisfy(line ->
                        assertThat(line).startsWith("error: " + notADatabase.fileName() + ": not an Access database"));
        assertThat(output).doesNotExist();
    }

    @Test
    void theTargetsThatArentBuiltYetAreAUsageError() {
        Cli cli = Cli.run(
                "convert", "--to", "mysql", GeneratedFixture.HUNDRED_ROWS.path().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(cli.err()).contains("--to").contains("sqlite");
    }
}
