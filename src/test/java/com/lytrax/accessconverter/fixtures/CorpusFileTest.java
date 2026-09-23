package com.lytrax.accessconverter.fixtures;

import static com.lytrax.accessconverter.fixtures.GeneratedFixtureTest.localTableNames;
import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Database;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Tier B smoke test: every corpus file downloads and matches its checksum; every database opens in Jackcess. */
class CorpusFileTest {

    static Stream<CorpusFile> databases() {
        return CorpusFile.databases().stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("databases")
    void opensInJackcess(CorpusFile file) throws IOException {
        try (Database db = file.open()) {
            assertThat(db.getFileFormat()).isNotNull();
            assertThat(localTableNames(db)).isNotNull();
        }
    }

    @Test
    void theManifestCoversEveryFormatAndSource() {
        assertThat(CorpusFile.all()).hasSize(138);
        assertThat(CorpusFile.databases()).hasSize(135);
        assertThat(CorpusFile.all())
                .extracting(f -> f.source().id())
                .contains("jackcess", "jackcess-encrypt", "mdb-reader");
        assertThat(CorpusFile.all())
                .extracting(CorpusFile::fileName)
                .anyMatch(n -> n.endsWith(".mdb"))
                .anyMatch(n -> n.endsWith(".accdb"))
                .anyMatch(n -> n.endsWith(".mny"));
    }

    /** 01, "Tables": {@code getTableNames()} includes linked tables, {@code withLocalUserTablesOnly()} doesn't (F-14). */
    @Test
    void linkedTableIsNotLocal() throws IOException {
        try (Database db = CorpusFile.get("jackcess/V2007/linkedV2007.accdb").open()) {
            assertThat(db.getTableNames()).containsExactlyInAnyOrder("Table1", "Table2");
            assertThat(localTableNames(db)).containsExactly("Table1");
        }
    }
}
