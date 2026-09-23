package com.lytrax.accessconverter.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Database;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The one Jackcess-internals call (DatabaseImpl#getDefaultCodePage): it must keep reading the header code page of
 * every Access 97 file in the corpus, encoded ones included. A Jackcess upgrade that breaks it fails here.
 */
class Jet3CodePageTest {

    static Stream<CorpusFile> access97() {
        return CorpusFile.databases().stream().filter(f -> {
            try (Database db = f.open()) {
                return db.getFileFormat() == Database.FileFormat.V1997;
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("access97")
    void readsTheHeaderCodePage(CorpusFile file) throws IOException {
        try (Database db = file.open()) {
            assertThat(Jet3CodePage.read(db)).isEqualTo(1252);
        }
    }
}
