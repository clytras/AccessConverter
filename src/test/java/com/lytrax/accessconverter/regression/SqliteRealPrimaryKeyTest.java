package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.target.sqlite.Sqlite;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture.Converted;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * F-32 and F-35: v2's SQLite export ignored the real primary key and made the autonumber one instead, so composite
 * keys disappeared and {@code Shippers} was keyed by an unrelated id. v3 takes the key from Access, and only from
 * Access.
 */
class SqliteRealPrimaryKeyTest {

    @TempDir
    static Path dir;

    static Converted converted;

    @BeforeAll
    static void convert() {
        converted =
                SqliteFixture.convert(GeneratedFixture.SCHEMA_FIDELITY.path(), dir.resolve("schemaFidelity.sqlite3"));
    }

    @Test
    void aTextPrimaryKeyStaysTheKeyAndTheAutonumberIsAnOrdinaryColumn() {
        try (Sqlite sqlite = converted.open()) {
            assertThat(keyColumns(sqlite, "Shippers")).containsExactly("Code");
            assertThat(sqlite.sql("Shippers"))
                    .contains("\"ShipperID\" INTEGER NOT NULL")
                    .doesNotContain("AUTOINCREMENT");
        }
    }

    @Test
    void aCompositePrimaryKeyIsKept() {
        try (Sqlite sqlite = converted.open()) {
            assertThat(keyColumns(sqlite, "Order Details")).containsExactly("OrderID", "ProductID");
            // SQLite allows NULLs in a primary key that isn't the rowid, so the columns say so themselves
            assertThat(sqlite.sql("Order Details"))
                    .contains("\"OrderID\" INTEGER NOT NULL")
                    .contains("\"ProductID\" INTEGER NOT NULL");
        }
    }

    @Test
    void anAutonumberPrimaryKeyBecomesTheRowidAndKeepsNegativeValues() {
        try (Sqlite sqlite = converted.open()) {
            assertThat(sqlite.sql("Customers")).contains("\"CustomerID\" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL");
            assertThat(sqlite.value("SELECT min(CustomerID) FROM Customers")).isEqualTo(-5);
            // Access never reuses an autonumber, and neither does AUTOINCREMENT: the sequence starts above the max
            assertThat(sqlite.value("SELECT seq FROM sqlite_sequence WHERE name = 'Customers'"))
                    .isEqualTo(2);
        }
    }

    private static List<String> keyColumns(Sqlite sqlite, String table) {
        return sqlite.strings("SELECT name FROM pragma_table_info('" + table + "') WHERE pk > 0 ORDER BY pk");
    }
}
