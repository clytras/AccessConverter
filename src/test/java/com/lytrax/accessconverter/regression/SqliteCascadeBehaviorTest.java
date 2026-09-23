package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.target.sqlite.Sqlite;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture.Converted;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * 06's behavior test: with {@code PRAGMA foreign_keys = ON}, the foreign keys do what Access did. Deleting a
 * customer cascades to its orders and their details, deleting a shipper sets the order's shipper to NULL, an update
 * of a key follows into the child rows, and a detail row for a product that isn't there is rejected.
 *
 * <p>v2 wrote no actions at all (F-33), so every one of these did nothing or failed.
 */
class SqliteCascadeBehaviorTest {

    @TempDir
    Path dir;

    private Converted converted;

    @BeforeEach
    void convert() {
        converted =
                SqliteFixture.convert(GeneratedFixture.SCHEMA_FIDELITY.path(), dir.resolve("schemaFidelity.sqlite3"));
    }

    @Test
    void deletingACustomerCascadesToItsOrdersAndTheirDetails() {
        try (Sqlite sqlite = enforcing()) {
            assertThat(sqlite.value("SELECT count(*) FROM Orders")).isEqualTo(2);
            assertThat(sqlite.value("SELECT count(*) FROM \"Order Details\"")).isEqualTo(3);

            sqlite.execute("DELETE FROM Customers WHERE CustomerID = -5");

            assertThat(sqlite.strings("SELECT OrderID FROM Orders")).containsExactly("2");
            assertThat(sqlite.value("SELECT count(*) FROM \"Order Details\"")).isEqualTo(1);
        }
    }

    @Test
    void updatingACustomerIdFollowsIntoItsOrders() {
        try (Sqlite sqlite = enforcing()) {
            sqlite.execute("UPDATE Customers SET CustomerID = 42 WHERE CustomerID = -5");
            assertThat(sqlite.strings("SELECT CustomerID FROM Orders ORDER BY OrderID"))
                    .containsExactly("42", "1");
        }
    }

    @Test
    void deletingAShipperSetsTheOrdersShipperToNull() {
        try (Sqlite sqlite = enforcing()) {
            assertThat(sqlite.value("SELECT ShipperCode FROM Orders WHERE OrderID = 1"))
                    .isEqualTo("DHL");
            sqlite.execute("DELETE FROM Shippers WHERE Code = 'DHL'");
            assertThat(sqlite.value("SELECT ShipperCode FROM Orders WHERE OrderID = 1"))
                    .isNull();
            // The order itself stays: Access's cascade-null only clears the reference
            assertThat(sqlite.value("SELECT count(*) FROM Orders WHERE OrderID = 1"))
                    .isEqualTo(1);
        }
    }

    @Test
    void aDetailRowForAProductThatIsNotThereIsRejected() {
        try (Sqlite sqlite = enforcing()) {
            assertThatThrownBy(() -> sqlite.execute("INSERT INTO \"Order Details\" VALUES (2, 999, 1, 1.0)"))
                    .rootCause()
                    .hasMessageContaining("FOREIGN KEY constraint failed");
            assertThat(sqlite.value("SELECT count(*) FROM \"Order Details\"")).isEqualTo(3);
        }
    }

    @Test
    void aRelationshipAccessDoesNotEnforceIsAnIndexAndAcceptsOrphans() {
        try (Sqlite sqlite = enforcing()) {
            // Suppliers/Products has no referential integrity in Access, and the fixture holds supplier 99 (F-31)
            assertThat(sqlite.value("SELECT SupplierID FROM Products WHERE Name = 'Widget Mini'"))
                    .isEqualTo(99);
            sqlite.execute("INSERT INTO Products (Name, SupplierID) VALUES ('Orphan', 12345)");
            assertThat(sqlite.query("PRAGMA foreign_key_check")).isEmpty();
            List<String> indexes = sqlite.strings(
                    "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'Products' ORDER BY name");
            assertThat(indexes).contains("Products_SuppliersProducts");
        }
    }

    private Sqlite enforcing() {
        Sqlite sqlite = converted.open();
        // A SQLite file can't turn this on for its readers; every consumer has to (06)
        sqlite.execute("PRAGMA foreign_keys = ON");
        return sqlite;
    }
}
