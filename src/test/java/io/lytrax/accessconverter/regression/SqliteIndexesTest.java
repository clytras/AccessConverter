package io.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.target.sqlite.Sqlite;
import io.lytrax.accessconverter.target.sqlite.SqliteFixture;
import io.lytrax.accessconverter.target.sqlite.SqliteFixture.Converted;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What the indexes look like in the output: direction is kept (F-37), Access's IgnoreNulls becomes a partial index,
 * the hidden index on a complex column is left out (F-16), and a GUID or complex autonumber never becomes an
 * identity column (F-15) — v2's SQLite export lost whole tables over that.
 */
class SqliteIndexesTest {

    @TempDir
    static Path dir;

    @Test
    void descendingIndexColumnsStayDescending() {
        Converted converted =
                SqliteFixture.convert(GeneratedFixture.SCHEMA_FIDELITY.path(), dir.resolve("schemaFidelity.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            assertThat(sqlite.sql("Customers_IX_Name"))
                    .isEqualTo("CREATE INDEX \"Customers_IX_Name\" ON \"Customers\" (\"Name\" DESC)");
            assertThat(sqlite.strings("SELECT desc FROM pragma_index_xinfo('Customers_IX_Name') WHERE key = 1"))
                    .containsExactly("1");
        }
    }

    @Test
    void ignoreNullsBecomesAPartialIndex() {
        Converted converted =
                SqliteFixture.convert(GeneratedFixture.SCHEMA_FIDELITY.path(), dir.resolve("partial.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            // Access leaves rows whose key is all NULL out of the index; a partial index is exactly that
            assertThat(sqlite.sql("Customers_UX_Email"))
                    .isEqualTo("CREATE UNIQUE INDEX \"Customers_UX_Email\" ON \"Customers\" (\"Email\")"
                            + " WHERE \"Email\" IS NOT NULL");
            assertThat(sqlite.value("SELECT partial FROM pragma_index_list('Customers') WHERE name ="
                            + " 'Customers_UX_Email'"))
                    .isEqualTo(1);
            // Two rows have no e-mail, which the index therefore doesn't hold, and Access allows
            assertThat(sqlite.value("SELECT count(*) FROM Customers WHERE Email IS NULL"))
                    .isEqualTo(2);
        }
    }

    @Test
    void theHiddenIndexOnAComplexColumnIsARealKeyOfItsComplexId() {
        Converted converted = SqliteFixture.convert(
                CorpusFile.get("jackcess/V2010/complexDataV2010.accdb").file(), dir.resolve("complex.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            // v2 made this a UNIQUE index over serialized attachment JSON, and the first duplicate lost the rows
            // (F-16). Now it's unique on Access's complex id, which the child tables' foreign keys refer to (08).
            assertThat(sqlite.strings("SELECT sql FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'Table1'"
                            + " AND sql IS NOT NULL ORDER BY name"))
                    .containsExactly(
                            "CREATE UNIQUE INDEX \"Table1_attach-data_071D71EDD53D45A1A9089929F06857D9\" ON \"Table1\""
                                    + " (\"attach-data\")",
                            "CREATE UNIQUE INDEX \"Table1_multi-value-data_F4C67B0F60124C1989D5583C00CF76E2\" ON"
                                    + " \"Table1\" (\"multi-value-data\")");
            assertThat(sqlite.value("SELECT count(*) FROM Table1")).isEqualTo(4);
            sqlite.assertIsConsistent();
        }
        assertThat(converted.issues(IssueCode.INDEX_SKIPPED_COMPLEX_COLUMN)).isEmpty();
        assertThat(converted.verified().matches()).isTrue();
    }

    @Test
    void aGuidAutonumberIsNeverAnIdentityColumn() {
        Converted converted = SqliteFixture.convert(
                CorpusFile.get("jackcess/V2010/common1V2010.accdb").file(), dir.resolve("common1.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            // Table4's key is a Replication ID: v2 wrote INTEGER PRIMARY KEY AUTOINCREMENT and lost the whole table
            assertThat(sqlite.sql("Table4"))
                    .contains("\"data\" CHAR(38)")
                    .contains("PRIMARY KEY (\"data\")")
                    .doesNotContain("AUTOINCREMENT");
            // Access generates a Replication ID itself, and so can SQLite: a new row gets one in Access's form
            assertThat(sqlite.sql("Table4")).contains("randomblob(4)");
            sqlite.execute("INSERT INTO Table4 (name) VALUES ('new row')");
            List<String> guids = sqlite.strings("SELECT data FROM Table4");
            assertThat(guids)
                    .singleElement()
                    .satisfies(guid -> assertThat(guid)
                            .matches("\\{[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}}"));
        }
    }

    @Test
    void anIndexThatOnlyRepeatsAnotherIsNotWrittenTwice() {
        Converted converted = SqliteFixture.convert(
                CorpusFile.get("jackcess/V2010/indexV2010.accdb").file(), dir.resolve("indexes.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            // Table1 has a plain index and the primary key on the same column, and hidden relationship indexes on
            // two more: only the two that add a lookup path are written (F-30)
            assertThat(sqlite.strings("SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'Table1'"
                            + " ORDER BY name"))
                    .containsExactly("Table1_Table2Table1", "Table1_Table3Table1");
            assertThat(sqlite.strings("SELECT name FROM pragma_index_list('Table1') WHERE origin = 'pk'"))
                    .isEmpty();
        }
        assertThat(converted.issues(IssueCode.INDEX_MERGED_DUPLICATE)).isNotEmpty();
    }
}
