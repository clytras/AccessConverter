package com.lytrax.accessconverter.fixtures;

import static com.lytrax.accessconverter.fixtures.GeneratedFixtureTest.localTableNames;
import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Database;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tier B smoke test: every corpus file downloads, matches its checksum and opens. */
class JackcessCorpusTest {

    @ParameterizedTest
    @EnumSource(JackcessCorpus.class)
    void opensWithLocalTables(JackcessCorpus file) throws IOException {
        try (Database db = file.open()) {
            assertThat(localTableNames(db)).isNotEmpty();
        }
    }

    /** 01, "Tables": {@code getTableNames()} includes linked tables, {@code withLocalUserTablesOnly()} doesn't (F-14). */
    @Test
    void linkedTableIsNotLocal() throws IOException {
        try (Database db = JackcessCorpus.LINKED_V2007.open()) {
            assertThat(db.getTableNames()).containsExactlyInAnyOrder("Table1", "Table2");
            assertThat(localTableNames(db)).containsExactly("Table1");
        }
    }
}
