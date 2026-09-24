package com.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.Access97Fixture;
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
        assertThat(verify.err()).contains("is neither a SQLite file nor a JSON export");

        Path directory = Files.createDirectories(dir.resolve("some-directory"));
        Cli notNdjson = Cli.run("verify", GeneratedFixture.HUNDRED_ROWS.path().toString(), directory.toString());
        assertThat(notNdjson.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(notNdjson.err()).contains("without the schema.json of an ndjson export");
    }

    @Test
    void convertsToJsonAndVerifiesIt() throws IOException {
        Path input = dir.resolve("copy.accdb");
        Files.copy(GeneratedFixture.SCHEMA_FIDELITY.path(), input);

        Cli convert = Cli.run("convert", "--to", "json", "--verify", input.toString());
        assertThat(convert.exitCode()).as(convert.err()).isEqualTo(ExitCodes.OK);
        Path document = dir.resolve("copy.json");
        assertThat(Files.readString(document, StandardCharsets.UTF_8))
                .startsWith("{\n  \"format\": \"accessconverter\",\n  \"formatVersion\": 1,\n")
                .doesNotContain("\"exported\"");
        assertThat(Files.readString(dir.resolve("copy.json.report.json"), StandardCharsets.UTF_8))
                .contains("\"to\": \"json\"", "\"jsonLayout\": \"document\"", "\"verify\": \"true\"")
                .doesNotContain("batchRows");
        assertThat(convert.out()).doesNotContain("import it with");

        Cli verify = Cli.run("verify", input.toString(), document.toString());
        assertThat(verify.exitCode()).isEqualTo(ExitCodes.OK);
        assertThat(verify.out()).contains("(JSON)", "verify: the output matches the source");

        Cli ndjson = Cli.run(
                "convert",
                "--to",
                "json",
                "--json-layout",
                "ndjson",
                "--json-rows",
                "array",
                "--stamp",
                input.toString());
        assertThat(ndjson.exitCode()).isEqualTo(ExitCodes.OK);
        Path directory = dir.resolve("copy-ndjson");
        assertThat(directory.resolve("schema.json")).content().contains("\"exported\": \"");
        assertThat(directory.resolve("Order Details.ndjson")).content().startsWith("[1,1,3,9.9900]\n");
        assertThat(dir.resolve("copy-ndjson.report.json")).exists();

        Cli verifyNdjson = Cli.run("verify", input.toString(), directory.toString());
        assertThat(verifyNdjson.exitCode()).isEqualTo(ExitCodes.OK);

        // An edited value is a difference, and verify fails
        Files.writeString(
                directory.resolve("Order Details.ndjson"),
                Files.readString(directory.resolve("Order Details.ndjson")).replace("9.9900", "9.9901"));
        Cli edited = Cli.run("verify", input.toString(), directory.toString());
        assertThat(edited.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(edited.out()).contains("Order Details.UnitPrice: row 1 is 9.9901, expected 9.9900");
    }

    @Test
    void jsonOptionsBelongToJsonAndSqlOptionsDoNot() {
        String input = GeneratedFixture.HUNDRED_ROWS.path().toString();
        Cli jsonOption = Cli.run("convert", "--to", "sqlite", "--json-rows", "array", input);
        assertThat(jsonOption.exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(jsonOption.err()).contains("apply to --to json");

        for (String[] option : new String[][] {
            {"--batch-rows", "10"},
            {"--analyze"},
            {"--drop-existing"},
            {"--collation", "utf8mb4_bin"},
            {"--sqlite-strict"}
        }) {
            String[] args = new String[option.length + 4];
            args[0] = "convert";
            args[1] = "--to";
            args[2] = "json";
            System.arraycopy(option, 0, args, 3, option.length);
            args[args.length - 1] = input;
            Cli cli = Cli.run(args);
            assertThat(cli.exitCode()).as(String.join(" ", option)).isEqualTo(ExitCodes.USAGE);
        }

        Cli stamp = Cli.run("convert", "--to", "sqlite", "--stamp", input);
        assertThat(stamp.exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(stamp.err()).contains("--stamp applies to --to mysql, --to mariadb and --to json");
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
    void convertsToAMySqlOrMariaDbDumpNextToTheInput() throws IOException {
        Path input = dir.resolve("copy.accdb");
        Files.copy(GeneratedFixture.HUNDRED_ROWS.path(), input);

        Cli mysql = Cli.run("convert", "--to", "mysql", input.toString());
        assertThat(mysql.exitCode()).isEqualTo(ExitCodes.OK);
        Path dump = dir.resolve("copy.sql");
        String sql = Files.readString(dump, StandardCharsets.UTF_8);
        assertThat(sql).startsWith("-- AccessConverter ").contains("COLLATE=utf8mb4_0900_as_ci");
        assertThat(mysql.out()).contains("import it with: mysql <database> < copy.sql");
        assertThat(Files.readString(dir.resolve("copy.sql.report.json"), StandardCharsets.UTF_8))
                .contains("\"to\": \"mysql\"")
                .contains("\"collation\": \"utf8mb4_0900_as_ci\"");

        Cli mariadb = Cli.run(
                "convert", "--to", "mariadb", "--overwrite", "--database", "shop", "--drop-existing", input.toString());
        assertThat(mariadb.exitCode()).isEqualTo(ExitCodes.OK);
        assertThat(Files.readString(dump, StandardCharsets.UTF_8))
                .contains("COLLATE=utf8mb4_uca1400_as_ci")
                .contains("CREATE DATABASE IF NOT EXISTS `shop`")
                .contains("DROP TABLE IF EXISTS `Hundred`;");
        assertThat(mariadb.out()).contains("import it with: mariadb < copy.sql");
    }

    @Test
    void optionsForAnotherTargetAreAUsageErrorNeverIgnored() {
        String input = GeneratedFixture.HUNDRED_ROWS.path().toString();
        Cli sqliteOption = Cli.run("convert", "--to", "mysql", "--sqlite-strict", input);
        assertThat(sqliteOption.exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(sqliteOption.err()).contains("apply to --to sqlite");

        Cli mysqlOption = Cli.run("convert", "--to", "sqlite", "--drop-existing", input);
        assertThat(mysqlOption.exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(mysqlOption.err()).contains("apply to --to mysql and --to mariadb");

        Cli verify = Cli.run("convert", "--to", "mariadb", "--verify", input);
        assertThat(verify.exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(verify.err()).contains("verify --jdbc-url");

        Cli collation = Cli.run("convert", "--to", "mysql", "--collation", "latin1_swedish_ci", input);
        assertThat(collation.exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(collation.err()).contains("utf8mb4");
    }

    @Test
    void verifyNamesTheMissingDriverWhenThereIsNone() {
        Cli cli = Cli.run(
                "verify",
                GeneratedFixture.HUNDRED_ROWS.path().toString(),
                "--jdbc-url",
                "jdbc:nosuchdb://localhost/test");
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.err()).contains("--jdbc-driver");
    }

    @Test
    void verifySaysWhyAMatchingOutputStillExitsWithWarnings() {
        // gr97 warns while it is read (CATALOG_INDEX_UNUSABLE), in the conversion and again in verify
        String input = Access97Fixture.GR97.file().toString();
        Path output = dir.resolve("gr97.sqlite3");
        assertThat(Cli.run("convert", "--to", "sqlite", "-o", output.toString(), input)
                        .exitCode())
                .isEqualTo(ExitCodes.WARNINGS);

        Cli verify = Cli.run("verify", input, output.toString());

        // 02: exit 1 is success with warnings, so the line says both
        assertThat(verify.exitCode()).isEqualTo(ExitCodes.WARNINGS);
        assertThat(verify.out())
                .contains("verify: the output matches the source, with 1 warning carried over from reading the"
                        + " source, as in the conversion:")
                .contains("warning CATALOG_INDEX_UNUSABLE");
    }
}
