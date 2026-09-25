CREATE TABLE "CustomerLoyaltyProgramMembershipHistory" (
  "HistID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "TierID" INTEGER,
  FOREIGN KEY ("TierID") REFERENCES "CustomerLoyaltyProgramMembershipTiers" ("TierID") -- Access relationship: CustomerLoyaltyProgramMembersCustomerLoyaltyProgramMembership
);
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE "CustomerLoyaltyProgramMembershipTiers" (
  "TierID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
);
CREATE TABLE "Customers" (
  "CustomerID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Code" VARCHAR(10) NOT NULL COLLATE NOCASE,
  "Name" VARCHAR(100) NOT NULL COLLATE NOCASE, -- Customer display name
  "Email" VARCHAR(120) COLLATE NOCASE,
  "Notes" TEXT COLLATE NOCASE,
  "Rating" TINYINT,
  "Balance" TEXT,
  "Discount" DECIMAL(10,4),
  "Created" DATETIME DEFAULT (datetime('now','localtime')),
  "Active" BOOLEAN NOT NULL DEFAULT 1,
  "RowGuid" CHAR(38),
  "Visits" INTEGER,
  "Country" VARCHAR(50) DEFAULT 'Greece' COLLATE NOCASE
);
CREATE TABLE "Files" (
  "FileID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Raw" BLOB,
  "Doc" BLOB,
  "Img" BLOB
);
CREATE TABLE "Order Details" (
  "OrderID" INTEGER NOT NULL,
  "ProductID" INTEGER NOT NULL,
  "Qty" SMALLINT,
  "UnitPrice" DECIMAL(19,4),
  PRIMARY KEY ("OrderID", "ProductID"),
  CONSTRAINT "ck_Qty" CHECK ("Qty" > 0),
  FOREIGN KEY ("OrderID") REFERENCES "Orders" ("OrderID") ON DELETE CASCADE, -- Access relationship: OrdersOrder Details
  FOREIGN KEY ("ProductID") REFERENCES "Products" ("ProductID") -- Access relationship: ProductsOrder Details
);
CREATE TABLE "Orders" (
  "OrderID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "CustomerID" INTEGER NOT NULL,
  "ShipperCode" VARCHAR(10) COLLATE NOCASE,
  "OrderDate" DATETIME,
  "Order" VARCHAR(20) COLLATE NOCASE,
  FOREIGN KEY ("CustomerID") REFERENCES "Customers" ("CustomerID") ON UPDATE CASCADE ON DELETE CASCADE, -- Access relationship: CustomersOrders
  FOREIGN KEY ("ShipperCode") REFERENCES "Shippers" ("Code") ON DELETE SET NULL -- Access relationship: ShippersOrders
);
CREATE TABLE "Products" (
  "ProductID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Name" VARCHAR(50) COLLATE NOCASE,
  "ParentProductID" INTEGER,
  "SupplierID" INTEGER,
  "Price" DECIMAL(19,4)
);
CREATE TABLE "Shippers" (
  "ShipperID" INTEGER NOT NULL,
  "Code" VARCHAR(10) NOT NULL COLLATE NOCASE,
  "Name" VARCHAR(50) COLLATE NOCASE,
  PRIMARY KEY ("Code")
);
CREATE TABLE "Suppliers" (
  "SupplierID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Name" VARCHAR(50) COLLATE NOCASE
);
CREATE TABLE "WideTable" (
  "ID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "T1" VARCHAR(255) COLLATE NOCASE,
  "T2" VARCHAR(255) COLLATE NOCASE,
  "T3" VARCHAR(255) COLLATE NOCASE,
  "T4" VARCHAR(255) COLLATE NOCASE,
  "T5" VARCHAR(255) COLLATE NOCASE,
  "T6" VARCHAR(255) COLLATE NOCASE,
  "T7" VARCHAR(255) COLLATE NOCASE,
  "T8" VARCHAR(255) COLLATE NOCASE,
  "T9" VARCHAR(255) COLLATE NOCASE,
  "T10" VARCHAR(255) COLLATE NOCASE,
  "T11" VARCHAR(255) COLLATE NOCASE,
  "T12" VARCHAR(255) COLLATE NOCASE,
  "T13" VARCHAR(255) COLLATE NOCASE,
  "T14" VARCHAR(255) COLLATE NOCASE,
  "T15" VARCHAR(255) COLLATE NOCASE,
  "T16" VARCHAR(255) COLLATE NOCASE,
  "T17" VARCHAR(255) COLLATE NOCASE,
  "T18" VARCHAR(255) COLLATE NOCASE,
  "T19" VARCHAR(255) COLLATE NOCASE,
  "T20" VARCHAR(255) COLLATE NOCASE,
  "T21" VARCHAR(255) COLLATE NOCASE,
  "T22" VARCHAR(255) COLLATE NOCASE,
  "T23" VARCHAR(255) COLLATE NOCASE,
  "T24" VARCHAR(255) COLLATE NOCASE,
  "T25" VARCHAR(255) COLLATE NOCASE,
  "T26" VARCHAR(255) COLLATE NOCASE,
  "T27" VARCHAR(255) COLLATE NOCASE,
  "T28" VARCHAR(255) COLLATE NOCASE,
  "T29" VARCHAR(255) COLLATE NOCASE,
  "T30" VARCHAR(255) COLLATE NOCASE,
  "T31" VARCHAR(255) COLLATE NOCASE,
  "T32" VARCHAR(255) COLLATE NOCASE,
  "T33" VARCHAR(255) COLLATE NOCASE,
  "T34" VARCHAR(255) COLLATE NOCASE,
  "T35" VARCHAR(255) COLLATE NOCASE,
  "T36" VARCHAR(255) COLLATE NOCASE,
  "T37" VARCHAR(255) COLLATE NOCASE,
  "T38" VARCHAR(255) COLLATE NOCASE,
  "T39" VARCHAR(255) COLLATE NOCASE,
  "T40" VARCHAR(255) COLLATE NOCASE,
  "T41" VARCHAR(255) COLLATE NOCASE,
  "T42" VARCHAR(255) COLLATE NOCASE,
  "T43" VARCHAR(255) COLLATE NOCASE,
  "T44" VARCHAR(255) COLLATE NOCASE,
  "T45" VARCHAR(255) COLLATE NOCASE,
  "T46" VARCHAR(255) COLLATE NOCASE,
  "T47" VARCHAR(255) COLLATE NOCASE,
  "T48" VARCHAR(255) COLLATE NOCASE,
  "T49" VARCHAR(255) COLLATE NOCASE,
  "T50" VARCHAR(255) COLLATE NOCASE,
  "T51" VARCHAR(255) COLLATE NOCASE,
  "T52" VARCHAR(255) COLLATE NOCASE,
  "T53" VARCHAR(255) COLLATE NOCASE,
  "T54" VARCHAR(255) COLLATE NOCASE,
  "T55" VARCHAR(255) COLLATE NOCASE,
  "T56" VARCHAR(255) COLLATE NOCASE,
  "T57" VARCHAR(255) COLLATE NOCASE,
  "T58" VARCHAR(255) COLLATE NOCASE,
  "T59" VARCHAR(255) COLLATE NOCASE,
  "T60" VARCHAR(255) COLLATE NOCASE,
  "T61" VARCHAR(255) COLLATE NOCASE,
  "T62" VARCHAR(255) COLLATE NOCASE,
  "T63" VARCHAR(255) COLLATE NOCASE,
  "T64" VARCHAR(255) COLLATE NOCASE,
  "T65" VARCHAR(255) COLLATE NOCASE,
  "T66" VARCHAR(255) COLLATE NOCASE,
  "T67" VARCHAR(255) COLLATE NOCASE,
  "T68" VARCHAR(255) COLLATE NOCASE,
  "T69" VARCHAR(255) COLLATE NOCASE,
  "T70" VARCHAR(255) COLLATE NOCASE
);
CREATE TABLE "Πελάτες" (
  "Κωδικός" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Όνομα" VARCHAR(50) COLLATE NOCASE
);
CREATE TABLE "_access_columns" (
  -- Access metadata, added by --sqlite-metadata; it describes the source database
  "table_name",
  "column_name",
  "ordinal",
  "access_type",
  "length",
  "precision",
  "scale",
  "required",
  "allow_zero_length",
  "description",
  "format",
  "decimal_places",
  "default_expr",
  "validation_rule",
  "validation_text",
  "calculated_expr",
  "append_only",
  "hidden",
  "written_as"
);
CREATE TABLE "_access_relationships" (
  -- Access metadata, added by --sqlite-metadata; it describes the source database
  "name",
  "parent_table",
  "parent_columns",
  "child_table",
  "child_columns",
  "enforced",
  "on_update",
  "on_delete",
  "one_to_one",
  "join_type",
  "status",
  "written_as_foreign_key"
);
CREATE TABLE "_access_export" (
  -- Access metadata, added by --sqlite-metadata; it describes the source database
  "producer",
  "format_version",
  "source_file",
  "source_format",
  "source_code_page",
  "source_charset"
);
CREATE INDEX "CustomerLoyaltyProgramMembershipHistory_CustomerLoyaltyProgramMembersCustomerLoyaltyProgramMembership" ON "CustomerLoyaltyProgramMembershipHistory" ("TierID");
CREATE INDEX "Customers_IX_Name" ON "Customers" ("Name" DESC);
CREATE UNIQUE INDEX "Customers_UX_Code" ON "Customers" ("Code");
CREATE UNIQUE INDEX "Customers_UX_Email" ON "Customers" ("Email") WHERE "Email" IS NOT NULL;
CREATE INDEX "Order Details_ProductsOrder Details" ON "Order Details" ("ProductID");
CREATE INDEX "Orders_CustomerID" ON "Orders" ("CustomerID");
CREATE INDEX "Orders_ShippersOrders" ON "Orders" ("ShipperCode");
CREATE INDEX "Products_Name" ON "Products" ("Name");
CREATE INDEX "Products_SuppliersProducts" ON "Products" ("SupplierID");
