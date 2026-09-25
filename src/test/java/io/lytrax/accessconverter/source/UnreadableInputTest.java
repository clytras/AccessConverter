package io.lytrax.accessconverter.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lytrax.accessconverter.Extraction;
import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.source.SourceException.Kind;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Inputs that aren't readable Access databases fail with a typed {@link SourceException} whose message says why
 * (the CLI prints it and exits with 2). Compiled and runtime-only files (.mde, .accde, .accdr) are ordinary
 * databases with their code removed, and read like any other.
 */
class UnreadableInputTest {
    private static final CorpusFile JET4 = CorpusFile.get("jackcess/adox_jet4.mdb");
    private static final CorpusFile ACE = CorpusFile.get("jackcess/V2010/common2V2010.accdb");

    @TempDir
    Path dir;

    @Test
    void anEmptyFile() throws IOException {
        Path file = Files.write(dir.resolve("empty.accdb"), new byte[0]);
        assertFails(file, Kind.NOT_AN_ACCESS_DATABASE, "empty.accdb: the file is empty");
    }

    @Test
    void aTextFile() throws IOException {
        Path file = Files.writeString(dir.resolve("notes.mdb"), "id,name\n1,Αθήνα\n", StandardCharsets.UTF_8);
        assertFails(
                file,
                Kind.NOT_AN_ACCESS_DATABASE,
                "notes.mdb: not an Access database (no Jet or ACE header); Access 97 through Microsoft 365 files are"
                        + " supported");
    }

    @Test
    void anAccessProject() throws IOException {
        Path file = copy(JET4, "frontEnd.adp");
        assertFails(file, Kind.ACCESS_PROJECT, "frontEnd.adp: an Access project (.adp) is a front end");
    }

    @Test
    void aWorkgroupInformationFile() throws IOException {
        // The header of a real System.mdw: the same page 0 layout with another identifier
        Path file = copy(JET4, "System.mdw");
        patch(file, 0x04, "Jet System DB  ".getBytes(StandardCharsets.US_ASCII));
        assertFails(file, Kind.WORKGROUP_FILE, "System.mdw: a workgroup information file (.mdw)");
    }

    /** What an Access 1.x/2.0 file, or one from a future Access, looks like to Jackcess. */
    @Test
    void anUnknownFormatVersion() throws IOException {
        Path file = copy(JET4, "future.mdb");
        patch(file, 0x14, new byte[] {9});
        assertFails(
                file,
                Kind.UNSUPPORTED_VERSION,
                "future.mdb: unsupported Access file version (format code 9); Access 97 through Microsoft 365 files"
                        + " are supported");
    }

    @Test
    void aTruncatedFile() throws IOException {
        byte[] bytes = Files.readAllBytes(ACE.file());
        assertFailsAsDamaged(Files.write(dir.resolve("truncated.accdb"), Arrays.copyOf(bytes, 4096 + 100)));
    }

    @Test
    void aFileWhosePagesAreNoise() throws IOException {
        byte[] bytes = Files.readAllBytes(ACE.file());
        byte[] noise = new byte[bytes.length - 4096];
        new Random(42).nextBytes(noise);
        System.arraycopy(noise, 0, bytes, 4096, noise.length);
        assertFailsAsDamaged(Files.write(dir.resolve("noise.accdb"), bytes));
    }

    @ParameterizedTest
    @CsvSource({
        "jackcess/V2003/common2V2003.mdb, compiled.mde",
        "jackcess/V2010/common2V2010.accdb, compiled.accde",
        "jackcess/V2010/common2V2010.accdb, runtime.accdr"
    })
    void compiledAndRuntimeFilesReadLikeTheirDatabase(String id, String name) throws IOException {
        CorpusFile original = CorpusFile.get(id);
        Path file = copy(original, name);
        assertThat(Extraction.of(file, OpenOptions.DEFAULT).model().tables())
                .isEqualTo(Extraction.of(original).model().tables());
    }

    private Path copy(CorpusFile file, String name) throws IOException {
        return Files.copy(file.file(), dir.resolve(name), StandardCopyOption.REPLACE_EXISTING);
    }

    private static void patch(Path file, int offset, byte[] with) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        System.arraycopy(with, 0, bytes, offset, with.length);
        Files.write(file, bytes);
    }

    private static void assertFails(Path file, Kind kind, String messageStart) {
        assertThatThrownBy(() -> AccessSource.open(file).close())
                .isInstanceOfSatisfying(
                        SourceException.class, e -> assertThat(e.kind()).isEqualTo(kind))
                .message()
                .startsWith(messageStart);
    }

    /** Damage shows either when opening (CORRUPT) or when a table is read (READ_FAILED); both name the file. */
    private static void assertFailsAsDamaged(Path file) {
        assertThatThrownBy(() -> Extraction.of(file, OpenOptions.DEFAULT)).satisfies(e -> {
            SourceException source = sourceException(e);
            assertThat(source).as("a SourceException in %s", e).isNotNull();
            assertThat(source.kind()).isIn(Kind.CORRUPT, Kind.READ_FAILED);
            assertThat(source.getMessage())
                    .startsWith(file.getFileName() + ": ")
                    .contains("Compact and Repair");
        });
    }

    private static SourceException sourceException(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SourceException source) {
                return source;
            }
        }
        return null;
    }
}
