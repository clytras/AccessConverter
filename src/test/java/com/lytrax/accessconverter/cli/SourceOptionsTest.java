package com.lytrax.accessconverter.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.CorpusFile;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** 02, CLI: --password and --charset, and how a source that can't be read is reported. */
class SourceOptionsTest {
    private static final CorpusFile ENCRYPTED = CorpusFile.get("jackcess-encrypt/db2007-enc.accdb");
    private static final CorpusFile ACCESS_97 = CorpusFile.get("jackcess/V1997/common2V1997.mdb");

    @Test
    void anEncryptedDatabaseOpensWithItsPassword() {
        Cli cli = Cli.run(
                "inspect", "--password", ENCRYPTED.password(), ENCRYPTED.file().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.OK);
        assertThat(cli.out()).startsWith("Source: db2007-enc.accdb (V2010)");
        assertThat(cli.out()).doesNotContain(ENCRYPTED.password());
        assertThat(cli.err()).isEmpty();
    }

    @Test
    void aBarePasswordIsAskedFor() {
        Cli cli = Cli.answering(
                ENCRYPTED.password(),
                "inspect",
                "--format",
                "json",
                ENCRYPTED.file().toString(),
                "--password");
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.OK);
        assertThat(cli.out()).startsWith("{");
        assertThat(cli.err()).isEmpty();
    }

    @Test
    void anEmptyAnswerIsNoPassword() {
        Cli cli = Cli.answering("", "inspect", ENCRYPTED.file().toString(), "--password");
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.err().lines())
                .containsExactly("error: db2007-enc.accdb: the database is encrypted: pass --password");
    }

    @Test
    void withoutThePasswordItFailsOnOneLine() {
        Cli cli = Cli.run("inspect", ENCRYPTED.file().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.out()).isEmpty();
        assertThat(cli.err().lines())
                .containsExactly("error: db2007-enc.accdb: the database is encrypted: pass --password");
    }

    @Test
    void aWrongPasswordFailsOnOneLineWithoutEchoingIt() {
        Cli cli = Cli.run("inspect", "--password=Secret99", ENCRYPTED.file().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.err().lines()).containsExactly("error: db2007-enc.accdb: the password is wrong");
    }

    @Test
    void verboseAddsTheStackTrace() {
        Cli cli = Cli.run(
                "--verbose", "inspect", "--password=Secret99", ENCRYPTED.file().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.FAILED);
        assertThat(cli.err())
                .startsWith("error: db2007-enc.accdb: the password is wrong")
                .contains("SourceException")
                .contains("InvalidCredentialsException")
                .contains("\tat ")
                .doesNotContain("Secret99");
    }

    @Test
    void unreadableInputsFailOnOneLine(@TempDir Path dir) throws IOException {
        Path empty = Files.write(dir.resolve("empty.mdb"), new byte[0]);
        Path project = Files.copy(CorpusFile.get("jackcess/adox_jet4.mdb").file(), dir.resolve("app.adp"));
        byte[] bytes = Files.readAllBytes(
                CorpusFile.get("jackcess/V2010/common2V2010.accdb").file());
        Path truncated = Files.write(dir.resolve("cut.accdb"), java.util.Arrays.copyOf(bytes, 4196));
        for (Path file : new Path[] {empty, project, truncated}) {
            Cli cli = Cli.run("inspect", file.toString());
            assertThat(cli.exitCode()).as(file.toString()).isEqualTo(ExitCodes.FAILED);
            assertThat(cli.out()).as(file.toString()).isEmpty();
            assertThat(cli.err().lines())
                    .as(file.toString())
                    .singleElement()
                    .asString()
                    .startsWith("error: " + file.getFileName() + ": ")
                    .doesNotContain("unexpected");
        }
    }

    @Test
    void inspectShowsTheCodePageAndCharset() {
        assertThat(Cli.run("inspect", ACCESS_97.file().toString()).out())
                .contains("\nText: code page 1252, charset windows-1252\n");
        assertThat(Cli.run(
                                "inspect",
                                CorpusFile.get("jackcess/V2000/common2V2000.mdb")
                                        .file()
                                        .toString())
                        .out())
                .contains("\nText: charset UTF-16LE\n");
    }

    @Test
    void aCharsetOverrideIsUsedAndReported() {
        Cli cli =
                Cli.run("inspect", "--charset", "windows-1251", ACCESS_97.file().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.OK);
        assertThat(cli.out())
                .contains("\nText: code page 1252, charset windows-1251\n")
                .contains("info CHARSET_OVERRIDDEN");
    }

    @Test
    void anUnknownCharsetIsAUsageError() {
        Cli cli = Cli.run("inspect", "--charset", "nosuch", ACCESS_97.file().toString());
        assertThat(cli.exitCode()).isEqualTo(ExitCodes.USAGE);
        assertThat(cli.err()).contains("unknown charset: nosuch");
    }
}
