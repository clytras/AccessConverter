CREATE TABLE "CustomerLoyaltyProgramMembershipHistory" (
  "HistID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "TierID" INTEGER,
  FOREIGN KEY ("TierID") REFERENCES "CustomerLoyaltyProgramMembershipTiers" ("TierID") -- Access relationship: CustomerLoyaltyProgramMembersCustomerLoyaltyProgramMembership
) STRICT;
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE "CustomerLoyaltyProgramMembershipTiers" (
  "TierID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL
) STRICT;
CREATE TABLE "Customers" (
  "CustomerID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Code" TEXT NOT NULL,
  "Name" TEXT NOT NULL, -- Customer display name
  "Email" TEXT,
  "Notes" TEXT,
  "Rating" INTEGER,
  "Balance" TEXT,
  "Discount" TEXT,
  "Created" TEXT DEFAULT (datetime('now','localtime')),
  "Active" INTEGER NOT NULL DEFAULT 1,
  "RowGuid" TEXT,
  "Visits" INTEGER,
  "Country" TEXT DEFAULT 'Greece'
) STRICT;
CREATE TABLE "Files" (
  "FileID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Raw" BLOB,
  "Doc" BLOB,
  "Img" BLOB
) STRICT;
CREATE TABLE "Order Details" (
  "OrderID" INTEGER NOT NULL,
  "ProductID" INTEGER NOT NULL,
  "Qty" INTEGER,
  "UnitPrice" TEXT,
  PRIMARY KEY ("OrderID", "ProductID"),
  CONSTRAINT "ck_Qty" CHECK ("Qty" > 0),
  FOREIGN KEY ("OrderID") REFERENCES "Orders" ("OrderID") ON DELETE CASCADE, -- Access relationship: OrdersOrder Details
  FOREIGN KEY ("ProductID") REFERENCES "Products" ("ProductID") -- Access relationship: ProductsOrder Details
) STRICT;
CREATE TABLE "Orders" (
  "OrderID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "CustomerID" INTEGER NOT NULL,
  "ShipperCode" TEXT,
  "OrderDate" TEXT,
  "Order" TEXT,
  FOREIGN KEY ("CustomerID") REFERENCES "Customers" ("CustomerID") ON UPDATE CASCADE ON DELETE CASCADE, -- Access relationship: CustomersOrders
  FOREIGN KEY ("ShipperCode") REFERENCES "Shippers" ("Code") ON DELETE SET NULL -- Access relationship: ShippersOrders
) STRICT;
CREATE TABLE "Products" (
  "ProductID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Name" TEXT,
  "ParentProductID" INTEGER,
  "SupplierID" INTEGER,
  "Price" TEXT
) STRICT;
CREATE TABLE "Shippers" (
  "ShipperID" INTEGER NOT NULL,
  "Code" TEXT NOT NULL,
  "Name" TEXT,
  PRIMARY KEY ("Code")
) STRICT;
CREATE TABLE "Suppliers" (
  "SupplierID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Name" TEXT
) STRICT;
CREATE TABLE "WideTable" (
  "ID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "T1" TEXT,
  "T2" TEXT,
  "T3" TEXT,
  "T4" TEXT,
  "T5" TEXT,
  "T6" TEXT,
  "T7" TEXT,
  "T8" TEXT,
  "T9" TEXT,
  "T10" TEXT,
  "T11" TEXT,
  "T12" TEXT,
  "T13" TEXT,
  "T14" TEXT,
  "T15" TEXT,
  "T16" TEXT,
  "T17" TEXT,
  "T18" TEXT,
  "T19" TEXT,
  "T20" TEXT,
  "T21" TEXT,
  "T22" TEXT,
  "T23" TEXT,
  "T24" TEXT,
  "T25" TEXT,
  "T26" TEXT,
  "T27" TEXT,
  "T28" TEXT,
  "T29" TEXT,
  "T30" TEXT,
  "T31" TEXT,
  "T32" TEXT,
  "T33" TEXT,
  "T34" TEXT,
  "T35" TEXT,
  "T36" TEXT,
  "T37" TEXT,
  "T38" TEXT,
  "T39" TEXT,
  "T40" TEXT,
  "T41" TEXT,
  "T42" TEXT,
  "T43" TEXT,
  "T44" TEXT,
  "T45" TEXT,
  "T46" TEXT,
  "T47" TEXT,
  "T48" TEXT,
  "T49" TEXT,
  "T50" TEXT,
  "T51" TEXT,
  "T52" TEXT,
  "T53" TEXT,
  "T54" TEXT,
  "T55" TEXT,
  "T56" TEXT,
  "T57" TEXT,
  "T58" TEXT,
  "T59" TEXT,
  "T60" TEXT,
  "T61" TEXT,
  "T62" TEXT,
  "T63" TEXT,
  "T64" TEXT,
  "T65" TEXT,
  "T66" TEXT,
  "T67" TEXT,
  "T68" TEXT,
  "T69" TEXT,
  "T70" TEXT
) STRICT;
CREATE TABLE "Πελάτες" (
  "Κωδικός" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Όνομα" TEXT
) STRICT;
CREATE INDEX "CustomerLoyaltyProgramMembershipHistory_CustomerLoyaltyProgramMembersCustomerLoyaltyProgramMembership" ON "CustomerLoyaltyProgramMembershipHistory" ("TierID");
CREATE INDEX "Customers_IX_Name" ON "Customers" ("Name" DESC);
CREATE UNIQUE INDEX "Customers_UX_Code" ON "Customers" ("Code");
CREATE UNIQUE INDEX "Customers_UX_Email" ON "Customers" ("Email") WHERE "Email" IS NOT NULL;
CREATE INDEX "Order Details_ProductsOrder Details" ON "Order Details" ("ProductID");
CREATE INDEX "Orders_CustomerID" ON "Orders" ("CustomerID");
CREATE INDEX "Orders_ShippersOrders" ON "Orders" ("ShipperCode");
CREATE INDEX "Products_Name" ON "Products" ("Name");
CREATE INDEX "Products_SuppliersProducts" ON "Products" ("SupplierID");
