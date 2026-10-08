package io.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.json.JsonFactory;

/** 02, CLI: commands, exit codes and output encodings. */
class MainTest {

    @Test
    void inspectPrintsTheModel() {
        Cli cli = Cli.run("inspect", GeneratedFixture.SCHEMA_FIDELITY.path().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.OK);
        assertThat(cli.out())
                .startsWith("Source: schemaFidelity.accdb (V2010)\n")
                .contains("Table Πελάτες");
        assertThat(cli.err()).isEmpty();
    }

    @Test
    void warningsGiveExitCodeOne() {
        Cli cli = Cli.run(
                "inspect",
                CorpusFile.get("jackcess/V2007/linkedV2007.accdb").file().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.WARNINGS);
        assertThat(cli.out()).contains("warning LINKED_TABLE_SKIPPED");
    }

    @Test
    void usageErrorsGiveSixtyFour() {
        assertThat(Cli.run().exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(Cli.run("frobnicate").exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(Cli.run("inspect").exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(Cli.run("inspect", "--format", "yaml", "x.accdb").exitCode()).isEqualTo(ExitCodes.USAGE);
    }

    @Test
    void aMissingInputFails(@TempDir Path dir) {
        Cli cli = Cli.run("inspect", dir.resolve("missing.accdb").toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.err().lines()).containsExactly("error: " + dir.resolve("missing.accdb") + ": no such file");
    }

    @Test
    void aDatabaseThatCantBeConnectedToIsNamed() {
        String url = "jdbc:nothing://localhost/db";
        Cli cli = Cli.run("verify", GeneratedFixture.HUNDRED_ROWS.path().toString(), "--jdbc-url", url);
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.err().lines())
                .singleElement()
                .satisfies(line -> assertThat(line)
                        .startsWith("error: " + url + ": can't connect: ")
                        .endsWith("; pass --jdbc-driver with MariaDB Connector/J or MySQL Connector/J"));
    }

    @Test
    void aMissingJdbcDriverIsNamed(@TempDir Path dir) {
        Path driver = dir.resolve("driver.jar");
        Cli cli = Cli.run(
                "verify",
                GeneratedFixture.HUNDRED_ROWS.path().toString(),
                "--jdbc-url",
                "jdbc:mariadb://localhost/db",
                "--jdbc-driver",
                driver.toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.err().lines()).containsExactly("error: " + driver + ": no such JDBC driver jar");
    }

    @Test
    void errorsNameWhatFailedNotAJavaClass() {
        assertThat(Main.message(new IOException("x.jar: no JDBC driver in it accepts jdbc:y")))
                .isEqualTo("x.jar: no JDBC driver in it accepts jdbc:y");
        assertThat(Main.message(new NoSuchFileException("a.accdb"))).isEqualTo("a.accdb: no such file");
        assertThat(Main.message(new AccessDeniedException("a.accdb"))).isEqualTo("a.accdb: no permission");
        assertThat(Main.message(new AccessDeniedException("a.accdb", null, "locked")))
                .isEqualTo("a.accdb: locked");
        // An I/O error the tool didn't word, and a bug, still say what they are
        assertThat(Main.message(new EOFException("end of stream"))).isEqualTo("EOFException: end of stream");
        assertThat(Main.message(new IllegalStateException("oops")))
                .isEqualTo("IllegalStateException: oops (unexpected; run with --verbose for details)");
    }

    @Test
    void aFileThatIsNotAnAccessDatabaseFails(@TempDir Path dir) throws Exception {
        Path junk = Files.writeString(dir.resolve("junk.accdb"), "not a database");
        Cli cli = Cli.run("inspect", junk.toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.err()).startsWith("error: ");
    }

    @Test
    void versionAndHelp() {
        assertThat(Cli.run("--version").out()).startsWith("accessconverter ");
        Cli help = Cli.run("inspect", "--help");
        assertThat(help.exitCode()).isEqualTo(ExitCodes.OK);
        assertThat(help.out()).contains("--profile").contains("--format");
    }

    @Test
    void jsonOnStandardOutputIsUtf8WhateverTheConsoleEncoding() throws Exception {
        // As on a Greek Windows console: text would go out in windows-1253, JSON must still be UTF-8
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        int code = Main.run(
                stdout,
                Charset.forName("windows-1253"),
                new PrintWriter(new StringWriter()),
                "inspect",
                "--format",
                "json",
                GeneratedFixture.SCHEMA_FIDELITY.path().toString());
        assertThat(code).isEqualTo(ExitCodes.OK);
        String json = stdout.toString(StandardCharsets.UTF_8);
        assertThat(json).contains("\"Πελάτες\"").doesNotContain("�").doesNotContain("\r");
        try (JsonParser parser = new JsonFactory().createParser(ObjectReadContext.empty(), json)) {
            int depth = 0;
            do {
                JsonToken token = parser.nextToken();
                if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                    depth++;
                } else if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
                    depth--;
                }
            } while (depth > 0);
        }
    }

    @Test
    void outputFilesAreUtf8(@TempDir Path dir) throws Exception {
        Path out = dir.resolve("model.txt");
        Cli cli = Cli.run(
                "inspect",
                "-o",
                out.toString(),
                GeneratedFixture.SCHEMA_FIDELITY.path().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.OK);
        assertThat(cli.out()).isEmpty();
        assertThat(Files.readString(out, StandardCharsets.UTF_8)).contains("Κωδικός");
    }

    @Test
    void verifyPrintsTheSourceSide() {
        Cli cli = Cli.run("verify", GeneratedFixture.HUNDRED_ROWS.path().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.OK);
        assertThat(cli.out()).containsPattern("Hundred: 100 rows, sha256 [0-9a-f]{64}\n");
    }

    @Test
    void verifyNeedsAnOutputThatIsThere(@TempDir Path dir) {
        Cli cli = Cli.run(
                "verify",
                GeneratedFixture.HUNDRED_ROWS.path().toString(),
                dir.resolve("out.sqlite3").toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.err()).contains("no such file");
    }
}
