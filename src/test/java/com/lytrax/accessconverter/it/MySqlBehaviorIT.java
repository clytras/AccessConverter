package com.lytrax.accessconverter.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.target.mysql.MySqlFixture;
import com.lytrax.accessconverter.target.mysql.MySqlFixture.Converted;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 09's behavior test on every server: the foreign keys of {@code schemaFidelity} do what Access did. Deleting a
 * customer cascades to its orders and their details, changing its id follows into the orders, deleting a shipper
 * clears the orders' reference, an orphan detail is rejected, and the relationship Access doesn't enforce still
 * accepts orphans (F-31, F-33).
 */
class MySqlBehaviorIT {

    @TempDir
    static Path dir;

    static Stream<String> images() {
        return DatabaseServer.images().stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("images")
    void foreignKeysBehaveAsInAccess(String image) throws Exception {
        DatabaseServer server = DatabaseServer.of(image);
        Path dump = dir.resolve(image.replace(':', '-') + "-schemaFidelity.sql");
        Converted converted = MySqlFixture.convert(GeneratedFixture.SCHEMA_FIDELITY.path(), dump, server.dialect());
        server.recreate("behavior");
        assertThat(server.importDump(dump, "behavior")).isEmpty();

        try (Connection db = server.connect("behavior");
                Statement s = db.createStatement()) {
            assertThat(strings(s, "SELECT ShipperCode FROM Orders WHERE OrderID = 1"))
                    .containsExactly("DHL");
            s.execute("DELETE FROM Shippers WHERE Code = 'DHL'");
            assertThat(strings(s, "SELECT ShipperCode FROM Orders WHERE OrderID = 1"))
                    .containsExactly((String) null);

            s.execute("UPDATE Customers SET CustomerID = 42 WHERE CustomerID = -5");
            assertThat(strings(s, "SELECT CustomerID FROM Orders ORDER BY OrderID"))
                    .containsExactly("42", "1");

            s.execute("DELETE FROM Customers WHERE CustomerID = 42");
            assertThat(strings(s, "SELECT OrderID FROM Orders")).containsExactly("2");
            assertThat(strings(s, "SELECT count(*) FROM `Order Details`")).containsExactly("1");

            assertThatThrownBy(() -> s.execute("INSERT INTO `Order Details` VALUES (2, 999, 1, 1.0)"))
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("foreign key constraint fails");

            assertThat(strings(s, "SELECT SupplierID FROM Products WHERE Name = 'Widget Mini'"))
                    .containsExactly("99");
            s.execute("INSERT INTO Products (Name, SupplierID) VALUES ('Orphan', 12345)");
        }
        assertThat(converted.table("Products").foreignKeys()).isEmpty();
    }

    private static List<String> strings(Statement s, String sql) throws SQLException {
        List<String> values = new ArrayList<>();
        try (ResultSet rows = s.executeQuery(sql)) {
            while (rows.next()) {
                values.add(rows.getString(1));
            }
        }
        return values;
    }
}
