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
  "Code" VARCHAR(10) NOT NULL,
  "Name" VARCHAR(100) NOT NULL, -- Customer display name
  "Email" VARCHAR(120),
  "Notes" TEXT,
  "Rating" TINYINT,
  "Balance" TEXT,
  "Discount" DECIMAL(10,4),
  "Created" DATETIME DEFAULT (datetime('now','localtime')),
  "Active" BOOLEAN NOT NULL DEFAULT 1,
  "RowGuid" CHAR(38),
  "Visits" INTEGER,
  "Country" VARCHAR(50) DEFAULT 'Greece'
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
  "ShipperCode" VARCHAR(10),
  "OrderDate" DATETIME,
  "Order" VARCHAR(20),
  FOREIGN KEY ("CustomerID") REFERENCES "Customers" ("CustomerID") ON UPDATE CASCADE ON DELETE CASCADE, -- Access relationship: CustomersOrders
  FOREIGN KEY ("ShipperCode") REFERENCES "Shippers" ("Code") ON DELETE SET NULL -- Access relationship: ShippersOrders
);
CREATE TABLE "Products" (
  "ProductID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Name" VARCHAR(50),
  "ParentProductID" INTEGER,
  "SupplierID" INTEGER,
  "Price" DECIMAL(19,4)
);
CREATE TABLE "Shippers" (
  "ShipperID" INTEGER NOT NULL,
  "Code" VARCHAR(10) NOT NULL,
  "Name" VARCHAR(50),
  PRIMARY KEY ("Code")
);
CREATE TABLE "Suppliers" (
  "SupplierID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Name" VARCHAR(50)
);
CREATE TABLE "WideTable" (
  "ID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "T1" VARCHAR(255),
  "T2" VARCHAR(255),
  "T3" VARCHAR(255),
  "T4" VARCHAR(255),
  "T5" VARCHAR(255),
  "T6" VARCHAR(255),
  "T7" VARCHAR(255),
  "T8" VARCHAR(255),
  "T9" VARCHAR(255),
  "T10" VARCHAR(255),
  "T11" VARCHAR(255),
  "T12" VARCHAR(255),
  "T13" VARCHAR(255),
  "T14" VARCHAR(255),
  "T15" VARCHAR(255),
  "T16" VARCHAR(255),
  "T17" VARCHAR(255),
  "T18" VARCHAR(255),
  "T19" VARCHAR(255),
  "T20" VARCHAR(255),
  "T21" VARCHAR(255),
  "T22" VARCHAR(255),
  "T23" VARCHAR(255),
  "T24" VARCHAR(255),
  "T25" VARCHAR(255),
  "T26" VARCHAR(255),
  "T27" VARCHAR(255),
  "T28" VARCHAR(255),
  "T29" VARCHAR(255),
  "T30" VARCHAR(255),
  "T31" VARCHAR(255),
  "T32" VARCHAR(255),
  "T33" VARCHAR(255),
  "T34" VARCHAR(255),
  "T35" VARCHAR(255),
  "T36" VARCHAR(255),
  "T37" VARCHAR(255),
  "T38" VARCHAR(255),
  "T39" VARCHAR(255),
  "T40" VARCHAR(255),
  "T41" VARCHAR(255),
  "T42" VARCHAR(255),
  "T43" VARCHAR(255),
  "T44" VARCHAR(255),
  "T45" VARCHAR(255),
  "T46" VARCHAR(255),
  "T47" VARCHAR(255),
  "T48" VARCHAR(255),
  "T49" VARCHAR(255),
  "T50" VARCHAR(255),
  "T51" VARCHAR(255),
  "T52" VARCHAR(255),
  "T53" VARCHAR(255),
  "T54" VARCHAR(255),
  "T55" VARCHAR(255),
  "T56" VARCHAR(255),
  "T57" VARCHAR(255),
  "T58" VARCHAR(255),
  "T59" VARCHAR(255),
  "T60" VARCHAR(255),
  "T61" VARCHAR(255),
  "T62" VARCHAR(255),
  "T63" VARCHAR(255),
  "T64" VARCHAR(255),
  "T65" VARCHAR(255),
  "T66" VARCHAR(255),
  "T67" VARCHAR(255),
  "T68" VARCHAR(255),
  "T69" VARCHAR(255),
  "T70" VARCHAR(255)
);
CREATE TABLE "Πελάτες" (
  "Κωδικός" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Όνομα" VARCHAR(50)
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
