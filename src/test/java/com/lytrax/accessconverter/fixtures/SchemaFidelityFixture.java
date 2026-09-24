package com.lytrax.accessconverter.fixtures;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.RelationshipBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.healthmarketscience.jackcess.util.OleBlob;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDateTime;

/**
 * Builds {@code schemaFidelity.accdb} (audit tool {@code MakeFixture}): keys, negative autonumbers, an autonumber
 * that isn't the PK, a composite PK, enforced, cascading and non-enforced relationships, awkward values, Binary and
 * OLE, a 70-column table and Greek names. The values are the evidence cited in 01, so keep them stable.
 */
final class SchemaFidelityFixture {
    private static final String REQUIRED = PropertyMap.REQUIRED_PROP;
    private static final String DEFAULT = PropertyMap.DEFAULT_VALUE_PROP;

    private SchemaFidelityFixture() {}

    static void build(Database db) throws IOException {
        customers(db);
        orders(db);
        files(db);
        wideTable(db);
        greekNames(db);
        longRelationshipName(db);
    }

    private static void customers(Database db) throws IOException {
        Table customers = new TableBuilder("Customers")
                .addColumn(new ColumnBuilder("CustomerID", DataType.LONG).setAutoNumber(true))
                .addColumn(new ColumnBuilder("Code", DataType.TEXT)
                        .setLengthInUnits(10)
                        .putProperty(REQUIRED, true))
                .addColumn(new ColumnBuilder("Name", DataType.TEXT)
                        .setLengthInUnits(100)
                        .putProperty(REQUIRED, true)
                        .putProperty(PropertyMap.DESCRIPTION_PROP, "Customer display name"))
                .addColumn(new ColumnBuilder("Email", DataType.TEXT).setLengthInUnits(120))
                .addColumn(new ColumnBuilder("Notes", DataType.MEMO))
                .addColumn(new ColumnBuilder("Rating", DataType.BYTE))
                .addColumn(new ColumnBuilder("Balance", DataType.MONEY))
                .addColumn(new ColumnBuilder("Discount", DataType.NUMERIC)
                        .setPrecision(10)
                        .setScale(4))
                .addColumn(new ColumnBuilder("Created", DataType.SHORT_DATE_TIME).putProperty(DEFAULT, "Now()"))
                .addColumn(new ColumnBuilder("Active", DataType.BOOLEAN).putProperty(DEFAULT, "Yes"))
                .addColumn(new ColumnBuilder("RowGuid", DataType.GUID))
                .addColumn(new ColumnBuilder("Visits", DataType.LONG))
                .addColumn(new ColumnBuilder("Country", DataType.TEXT)
                        .setLengthInUnits(50)
                        .putProperty(DEFAULT, "\"Greece\""))
                .addIndex(primaryKey("CustomerID"))
                .addIndex(new IndexBuilder("UX_Code").addColumns("Code").setUnique())
                .addIndex(new IndexBuilder("UX_Email")
                        .addColumns("Email")
                        .setUnique()
                        .setIgnoreNulls())
                .addIndex(new IndexBuilder("IX_Name").addColumns(false, "Name"))
                .toTable(db);

        // 70,000 chars: over MySQL TEXT's 64 KB (F-03c)
        String bigMemo = "0123456789".repeat(7_000);

        // Negative autonumber (F-03d), Byte > 127 (F-01), Currency needing DECIMAL(19,4) (F-03a), scale 4 (F-03b)
        customers.setAllowAutoNumberInsert(true);
        customers.addRow(
                -5,
                "C1",
                "Alice",
                "a@example.com",
                bigMemo,
                200,
                new BigDecimal("123456789012345.1234"),
                new BigDecimal("12.3456"),
                LocalDateTime.of(2024, 1, 2, 3, 4, 5),
                true,
                "{6F1C2A3B-1111-2222-3333-444455556666}",
                null,
                "Greece");
        customers.setAllowAutoNumberInsert(false);
        customers.addRow(
                Column.AUTO_NUMBER, "resume", "Bob", null, null, 255, null, null, null, false, null, null, "Greece");
        // Accent variant of "resume": distinct in Access, so UX_Code must accept it in every target (F-36)
        customers.addRow(
                Column.AUTO_NUMBER,
                "résumé",
                "Élodie",
                null,
                "short",
                0,
                new BigDecimal("-5.5"),
                new BigDecimal("0.0001"),
                LocalDateTime.of(201, 5, 5, 0, 0),
                true,
                null,
                7,
                "France");
    }

    private static void orders(Database db) throws IOException {
        // Autonumber that is NOT the primary key; the PK is a text column (F-32, F-35)
        Table shippers = new TableBuilder("Shippers")
                .addColumn(new ColumnBuilder("ShipperID", DataType.LONG).setAutoNumber(true))
                .addColumn(new ColumnBuilder("Code", DataType.TEXT).setLengthInUnits(10))
                .addColumn(new ColumnBuilder("Name", DataType.TEXT).setLengthInUnits(50))
                .addIndex(primaryKey("Code"))
                .toTable(db);
        shippers.addRow(Column.AUTO_NUMBER, "DHL", "DHL Express");
        shippers.addRow(Column.AUTO_NUMBER, "UPS", "United Parcel");

        Table orders = new TableBuilder("Orders")
                .addColumn(new ColumnBuilder("OrderID", DataType.LONG).setAutoNumber(true))
                .addColumn(new ColumnBuilder("CustomerID", DataType.LONG).putProperty(REQUIRED, true))
                .addColumn(new ColumnBuilder("ShipperCode", DataType.TEXT).setLengthInUnits(10))
                .addColumn(new ColumnBuilder("OrderDate", DataType.SHORT_DATE_TIME))
                .addColumn(new ColumnBuilder("Order", DataType.TEXT).setLengthInUnits(20))
                .addIndex(primaryKey("OrderID"))
                .addIndex(new IndexBuilder("CustomerID").addColumns("CustomerID"))
                .toTable(db);

        Table suppliers = new TableBuilder("Suppliers")
                .addColumn(new ColumnBuilder("SupplierID", DataType.LONG).setAutoNumber(true))
                .addColumn(new ColumnBuilder("Name", DataType.TEXT).setLengthInUnits(50))
                .addIndex(primaryKey("SupplierID"))
                .toTable(db);
        suppliers.addRow(Column.AUTO_NUMBER, "Acme");

        Table products = new TableBuilder("Products")
                .addColumn(new ColumnBuilder("ProductID", DataType.LONG).setAutoNumber(true))
                .addColumn(new ColumnBuilder("Name", DataType.TEXT).setLengthInUnits(50))
                .addColumn(new ColumnBuilder("ParentProductID", DataType.LONG))
                .addColumn(new ColumnBuilder("SupplierID", DataType.LONG))
                .addColumn(new ColumnBuilder("Price", DataType.MONEY))
                .addIndex(primaryKey("ProductID"))
                .addIndex(new IndexBuilder("Name").addColumns("Name"))
                .toTable(db);

        // Composite PK (F-32) and a column validation rule (F-38)
        Table details = new TableBuilder("Order Details")
                .addColumn(new ColumnBuilder("OrderID", DataType.LONG).putProperty(REQUIRED, true))
                .addColumn(new ColumnBuilder("ProductID", DataType.LONG).putProperty(REQUIRED, true))
                .addColumn(new ColumnBuilder("Qty", DataType.INT).putProperty(PropertyMap.VALIDATION_RULE_PROP, ">0"))
                .addColumn(new ColumnBuilder("UnitPrice", DataType.MONEY))
                .addIndex(primaryKey("OrderID", "ProductID"))
                .toTable(db);

        new RelationshipBuilder("Customers", "Orders")
                .addColumns("CustomerID", "CustomerID")
                .setReferentialIntegrity()
                .setCascadeUpdates()
                .setCascadeDeletes()
                .toRelationship(db);
        new RelationshipBuilder("Shippers", "Orders")
                .addColumns("Code", "ShipperCode")
                .setReferentialIntegrity()
                .setCascadeNullOnDelete()
                .toRelationship(db);
        new RelationshipBuilder("Orders", "Order Details")
                .addColumns("OrderID", "OrderID")
                .setReferentialIntegrity()
                .setCascadeDeletes()
                .toRelationship(db);
        new RelationshipBuilder("Products", "Order Details")
                .addColumns("ProductID", "ProductID")
                .setReferentialIntegrity()
                .toRelationship(db);
        // Join line only, no referential integrity: Access allows orphans (F-31)
        new RelationshipBuilder("Suppliers", "Products")
                .addColumns("SupplierID", "SupplierID")
                .toRelationship(db);

        // FK to the negative customer ID; a time-only date (Access day zero)
        orders.addRow(Column.AUTO_NUMBER, -5, "DHL", LocalDateTime.of(1899, 12, 30, 10, 30), "first");
        orders.addRow(Column.AUTO_NUMBER, 1, null, null, null);
        products.addRow(Column.AUTO_NUMBER, "Widget", null, 1, new BigDecimal("9.99"));
        // Supplier 99 doesn't exist: an orphan under the non-enforced relationship
        products.addRow(Column.AUTO_NUMBER, "Widget Mini", 1, 99, new BigDecimal("4.50"));
        details.addRow(1, 1, 3, new BigDecimal("9.99"));
        details.addRow(1, 2, 1, new BigDecimal("4.50"));
        details.addRow(2, 1, 2, null);
    }

    private static void files(Database db) throws IOException {
        Table files = new TableBuilder("Files")
                .addColumn(new ColumnBuilder("FileID", DataType.LONG).setAutoNumber(true))
                .addColumn(new ColumnBuilder("Raw", DataType.BINARY).setLength(16))
                .addColumn(new ColumnBuilder("Doc", DataType.OLE))
                .addColumn(new ColumnBuilder("Img", DataType.OLE))
                .addIndex(primaryKey("FileID"))
                .toTable(db);
        byte[] raw = new byte[16];
        for (int i = 0; i < raw.length; i++) {
            raw[i] = (byte) (i * 17);
        }
        byte[] packaged =
                simplePackage("hello.txt", "C:/tmp/hello.txt", "hello ole".getBytes(StandardCharsets.US_ASCII));
        // Raw PNG header written by code, no OLE wrapper (F-11)
        byte[] png = {(byte) 0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0x0D, 'I', 'H', 'D', 'R'};
        files.addRow(Column.AUTO_NUMBER, raw, packaged, png);
        files.addRow(Column.AUTO_NUMBER, null, null, null);
        // Empty values, which are not NULL: Binary round-trips as X'' (08), and an empty OLE value (F-13b)
        files.addRow(Column.AUTO_NUMBER, new byte[0], new byte[0], null);
    }

    private static void wideTable(Database db) throws IOException {
        // 70 Text(255) columns: over MySQL's row-size limit as utf8mb4 VARCHAR(255) (F-18)
        TableBuilder wide =
                new TableBuilder("WideTable").addColumn(new ColumnBuilder("ID", DataType.LONG).setAutoNumber(true));
        for (int i = 1; i <= 70; i++) {
            wide.addColumn(new ColumnBuilder("T" + i, DataType.TEXT).setLengthInUnits(255));
        }
        Table wideTable = wide.addIndex(primaryKey("ID")).toTable(db);
        Object[] row = new Object[71];
        row[0] = Column.AUTO_NUMBER;
        for (int i = 1; i <= 70; i++) {
            row[i] = "v" + i;
        }
        wideTable.addRow(row);
    }

    private static void greekNames(Database db) throws IOException {
        Table greek = new TableBuilder("Πελάτες")
                .addColumn(new ColumnBuilder("Κωδικός", DataType.LONG).setAutoNumber(true))
                .addColumn(new ColumnBuilder("Όνομα", DataType.TEXT).setLengthInUnits(50))
                .addIndex(primaryKey("Κωδικός"))
                .toTable(db);
        greek.addRow(Column.AUTO_NUMBER, "Γιώργος");
    }

    private static void longRelationshipName(Database db) throws IOException {
        // Jackcess truncates the generated relationship name to 64 characters
        Table tiers = new TableBuilder("CustomerLoyaltyProgramMembershipTiers")
                .addColumn(new ColumnBuilder("TierID", DataType.LONG).setAutoNumber(true))
                .addIndex(primaryKey("TierID"))
                .toTable(db);
        new TableBuilder("CustomerLoyaltyProgramMembershipHistory")
                .addColumn(new ColumnBuilder("HistID", DataType.LONG).setAutoNumber(true))
                .addColumn(new ColumnBuilder("TierID", DataType.LONG))
                .addIndex(primaryKey("HistID"))
                .toTable(db);
        tiers.addRow(Column.AUTO_NUMBER);
        new RelationshipBuilder("CustomerLoyaltyProgramMembershipTiers", "CustomerLoyaltyProgramMembershipHistory")
                .addColumns("TierID", "TierID")
                .setReferentialIntegrity()
                .toRelationship(db);
    }

    private static IndexBuilder primaryKey(String... columns) {
        return new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                .addColumns(columns)
                .setPrimaryKey();
    }

    private static byte[] simplePackage(String fileName, String filePath, byte[] content) throws IOException {
        try (OleBlob blob = new OleBlob.Builder()
                .setSimplePackageBytes(content)
                .setSimplePackageFileName(fileName)
                .setSimplePackageFilePath(filePath)
                .toBlob()) {
            return blob.getBytes(1, (int) blob.length());
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}
