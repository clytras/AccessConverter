package com.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.fixtures.JackcessCorpus;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
        Cli cli = Cli.run("inspect", JackcessCorpus.LINKED_V2007.path().toString());
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
        assertThat(cli.err()).startsWith("error: NoSuchFileException").contains("missing.accdb");
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
    void verifyAgainstAnOutputIsNotAvailableYet(@TempDir Path dir) {
        Cli cli = Cli.run(
                "verify",
                GeneratedFixture.HUNDRED_ROWS.path().toString(),
                dir.resolve("out.sqlite3").toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(cli.err()).contains("isn't implemented yet");
    }
}
