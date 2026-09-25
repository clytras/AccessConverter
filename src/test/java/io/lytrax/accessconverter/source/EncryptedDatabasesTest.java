package io.lytrax.accessconverter.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.report.Issue;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.SourceException.Kind;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Encrypted and password-protected databases (jackcess-encrypt): Office 2007+ encryption (RC4 CryptoAPI and Agile),
 * Jet 3/4 encoding, and Microsoft Money passwords need the password; a Jet 3/4 "database password" doesn't encrypt
 * anything and isn't needed.
 */
class EncryptedDatabasesTest {

    static Stream<CorpusFile> encrypted() {
        return CorpusFile.databases().stream().filter(f -> f.password() != null);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("encrypted")
    void opensWithItsPassword(CorpusFile file) throws IOException {
        try (AccessSource source = AccessSource.open(file.file(), file.openOptions(), new Issues())) {
            assertThat(source.localTables()).isNotEmpty();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("encrypted")
    void withoutAPasswordItAsksForOne(CorpusFile file) {
        assertThatThrownBy(() -> AccessSource.open(file.file(), OpenOptions.DEFAULT, new Issues()))
                .isInstanceOfSatisfying(
                        SourceException.class, e -> assertThat(e.kind()).isEqualTo(Kind.PASSWORD_REQUIRED))
                .hasMessage(file.fileName() + ": the database is encrypted: pass --password");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("encrypted")
    void aWrongPasswordIsRejected(CorpusFile file) {
        assertThatThrownBy(() -> AccessSource.open(file.file(), new OpenOptions("wrong", null), new Issues()))
                .isInstanceOfSatisfying(
                        SourceException.class, e -> assertThat(e.kind()).isEqualTo(Kind.WRONG_PASSWORD))
                .hasMessage(file.fileName() + ": the password is wrong");
    }

    /** Jet 3/4 encoding (a fixed RC4 key) needs no password. */
    @ParameterizedTest
    @ValueSource(strings = {"jackcess-encrypt/db97-enc.mdb", "jackcess-encrypt/db-enc.mdb"})
    void encodedDatabasesOpenWithoutAPassword(String id) throws IOException {
        Issues issues = new Issues();
        try (AccessSource source = AccessSource.open(CorpusFile.get(id).file(), OpenOptions.DEFAULT, issues)) {
            assertThat(source.localTables()).isNotEmpty();
        }
        assertThat(issues.list()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "jackcess-encrypt/pwd-set.mdb",
                "jackcess-encrypt/pwd2-set.mdb",
                "mdb-reader/test/password/data/V2000_with-password.mdb",
                "mdb-reader/test/password/data/V2003_with-password.mdb"
            })
    void aJetDatabasePasswordIsNotNeededAndSaysSo(String id) throws IOException {
        Issues issues = new Issues();
        try (AccessSource source = AccessSource.open(CorpusFile.get(id).file(), OpenOptions.DEFAULT, issues)) {
            assertThat(source.localTables()).isNotEmpty();
        }
        assertThat(issues.list()).extracting(Issue::code).containsExactly(IssueCode.PASSWORD_NOT_REQUIRED);
    }

    @ParameterizedTest
    @ValueSource(strings = {"jackcess-encrypt/pwd-none.mdb", "jackcess-encrypt/pwd-removed.mdb"})
    void withoutADatabasePasswordNothingIsReported(String id) throws IOException {
        Issues issues = new Issues();
        try (AccessSource source = AccessSource.open(CorpusFile.get(id).file(), OpenOptions.DEFAULT, issues)) {
            assertThat(source.localTables()).isNotEmpty();
        }
        assertThat(issues.list()).isEmpty();
    }
}
