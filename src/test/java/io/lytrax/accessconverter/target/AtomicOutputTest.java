package io.lytrax.accessconverter.target;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A failure writing an output names the output, not its {@code .partial}, with the reason in words; a failure that
 * isn't about the output passes through unchanged. The cases no test can bring about for real, such as a full disk.
 */
class AtomicOutputTest {

    @TempDir
    Path dir;

    private Path output() {
        return dir.resolve("out.json");
    }

    private void failing(IOException e) throws IOException {
        AtomicOutput.write(output(), partial -> {
            throw e;
        });
    }

    @Test
    void aFullDiskIsSaidInWords() {
        assertThatThrownBy(() -> failing(new IOException("No space left on device")))
                .isInstanceOf(OutputException.class)
                .hasMessage(output() + ": the disk is full");
        assertThatThrownBy(() -> failing(new IOException("There is not enough space on the disk")))
                .hasMessage(output() + ": the disk is full");
        // SQLite's own, as a table that failed to be written wraps it
        IOException table = new IOException("table T failed", new SQLException("[SQLITE_FULL]", null, 13));
        assertThatThrownBy(() -> failing(table)).hasMessage(output() + ": the disk is full");
    }

    @Test
    void noPermissionOnThePartialNamesTheOutput() {
        Path partial = dir.resolve("out.json.partial");
        assertThatThrownBy(() -> failing(new AccessDeniedException(partial.toString())))
                .isInstanceOf(OutputException.class)
                .hasMessage(output() + ": no permission to write it");
    }

    @Test
    void aFailureInWordsIsPutUnderTheOutputsName() {
        assertThatThrownBy(() -> failing(new IOException("PRAGMA integrity_check failed: x")))
                .isInstanceOf(OutputException.class)
                .hasMessage(output() + ": PRAGMA integrity_check failed: x");
    }

    @Test
    void aFileErrorElsewhereIsNotTheOutputs() {
        NoSuchFileException elsewhere =
                new NoSuchFileException(dir.resolve("input.accdb").toString());
        assertThatThrownBy(() -> failing(elsewhere)).isSameAs(elsewhere);
    }

    @Test
    void aFolderWhereTheFileShouldBeIsSaid() throws IOException {
        Path output = Files.createDirectory(dir.resolve("out.sqlite3"));
        assertThat(OutputException.folderProblem(output, true)).isEqualTo("it is a folder");
        assertThat(OutputException.folderProblem(output, false)).isNull();
    }
}
