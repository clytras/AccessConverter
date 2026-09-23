-- AccessConverter - source: schemaFidelity.accdb (V2010)
-- Target: MariaDB 10.11 or later, collation utf8mb4_uca1400_as_ci

SET NAMES utf8mb4;
SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks, @ac_ac = @@SESSION.autocommit;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION foreign_key_checks = 0, autocommit = 0;

-- 1. Tables

CREATE TABLE `CustomerLoyaltyProgramMembershipHistory` (
  `HistID` INT NOT NULL AUTO_INCREMENT,
  `TierID` INT,
  PRIMARY KEY (`HistID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `CustomerLoyaltyProgramMembershipTiers` (
  `TierID` INT NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`TierID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci AUTO_INCREMENT=2;
CREATE TABLE `Customers` (
  `CustomerID` INT NOT NULL AUTO_INCREMENT,
  `Code` VARCHAR(10) NOT NULL,
  `Name` VARCHAR(100) NOT NULL COMMENT 'Customer display name',
  `Email` VARCHAR(120),
  `Notes` LONGTEXT,
  `Rating` TINYINT UNSIGNED,
  `Balance` DECIMAL(19,4),
  `Discount` DECIMAL(10,4),
  `Created` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `Active` BOOLEAN NOT NULL DEFAULT 1,
  `RowGuid` CHAR(38),
  `Visits` INT,
  `Country` VARCHAR(50) DEFAULT 'Greece',
  PRIMARY KEY (`CustomerID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci AUTO_INCREMENT=3;
CREATE TABLE `Files` (
  `FileID` INT NOT NULL AUTO_INCREMENT,
  `Raw` VARBINARY(16),
  `Doc` LONGBLOB,
  `Img` LONGBLOB,
  PRIMARY KEY (`FileID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci AUTO_INCREMENT=3;
CREATE TABLE `Order Details` (
  `OrderID` INT NOT NULL,
  `ProductID` INT NOT NULL,
  `Qty` SMALLINT,
  `UnitPrice` DECIMAL(19,4),
  PRIMARY KEY (`OrderID`, `ProductID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Orders` (
  `OrderID` INT NOT NULL AUTO_INCREMENT,
  `CustomerID` INT NOT NULL,
  `ShipperCode` VARCHAR(10),
  `OrderDate` DATETIME,
  `Order` VARCHAR(20),
  PRIMARY KEY (`OrderID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci AUTO_INCREMENT=3;
CREATE TABLE `Products` (
  `ProductID` INT NOT NULL AUTO_INCREMENT,
  `Name` VARCHAR(50),
  `ParentProductID` INT,
  `SupplierID` INT,
  `Price` DECIMAL(19,4),
  PRIMARY KEY (`ProductID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci AUTO_INCREMENT=3;
CREATE TABLE `Shippers` (
  `ShipperID` INT NOT NULL AUTO_INCREMENT,
  `Code` VARCHAR(10) NOT NULL,
  `Name` VARCHAR(50),
  PRIMARY KEY (`Code`),
  KEY `ShipperID` (`ShipperID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci AUTO_INCREMENT=3;
CREATE TABLE `Suppliers` (
  `SupplierID` INT NOT NULL AUTO_INCREMENT,
  `Name` VARCHAR(50),
  PRIMARY KEY (`SupplierID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci AUTO_INCREMENT=2;
CREATE TABLE `WideTable` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `T1` TEXT,
  `T2` TEXT,
  `T3` TEXT,
  `T4` TEXT,
  `T5` TEXT,
  `T6` TEXT,
  `T7` VARCHAR(255),
  `T8` VARCHAR(255),
  `T9` VARCHAR(255),
  `T10` VARCHAR(255),
  `T11` VARCHAR(255),
  `T12` VARCHAR(255),
  `T13` VARCHAR(255),
  `T14` VARCHAR(255),
  `T15` VARCHAR(255),
  `T16` VARCHAR(255),
  `T17` VARCHAR(255),
  `T18` VARCHAR(255),
  `T19` VARCHAR(255),
  `T20` VARCHAR(255),
  `T21` VARCHAR(255),
  `T22` VARCHAR(255),
  `T23` VARCHAR(255),
  `T24` VARCHAR(255),
  `T25` VARCHAR(255),
  `T26` VARCHAR(255),
  `T27` VARCHAR(255),
  `T28` VARCHAR(255),
  `T29` VARCHAR(255),
  `T30` VARCHAR(255),
  `T31` VARCHAR(255),
  `T32` VARCHAR(255),
  `T33` VARCHAR(255),
  `T34` VARCHAR(255),
  `T35` VARCHAR(255),
  `T36` VARCHAR(255),
  `T37` VARCHAR(255),
  `T38` VARCHAR(255),
  `T39` VARCHAR(255),
  `T40` VARCHAR(255),
  `T41` VARCHAR(255),
  `T42` VARCHAR(255),
  `T43` VARCHAR(255),
  `T44` VARCHAR(255),
  `T45` VARCHAR(255),
  `T46` VARCHAR(255),
  `T47` VARCHAR(255),
  `T48` VARCHAR(255),
  `T49` VARCHAR(255),
  `T50` VARCHAR(255),
  `T51` VARCHAR(255),
  `T52` VARCHAR(255),
  `T53` VARCHAR(255),
  `T54` VARCHAR(255),
  `T55` VARCHAR(255),
  `T56` VARCHAR(255),
  `T57` VARCHAR(255),
  `T58` VARCHAR(255),
  `T59` VARCHAR(255),
  `T60` VARCHAR(255),
  `T61` VARCHAR(255),
  `T62` VARCHAR(255),
  `T63` VARCHAR(255),
  `T64` VARCHAR(255),
  `T65` VARCHAR(255),
  `T66` VARCHAR(255),
  `T67` VARCHAR(255),
  `T68` VARCHAR(255),
  `T69` VARCHAR(255),
  `T70` VARCHAR(255),
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci AUTO_INCREMENT=2;
CREATE TABLE `Πελάτες` (
  `Κωδικός` INT NOT NULL AUTO_INCREMENT,
  `Όνομα` VARCHAR(50),
  PRIMARY KEY (`Κωδικός`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci AUTO_INCREMENT=2;

-- 2. Data: one transaction per table

COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;
COMMIT;

-- 3. Secondary indexes

ALTER TABLE `CustomerLoyaltyProgramMembershipHistory`
  ADD KEY `CustomerLoyaltyProgramMembersCustomerLoyaltyProgramMembership` (`TierID`);
ALTER TABLE `Customers`
  ADD KEY `IX_Name` (`Name` DESC),
  ADD UNIQUE KEY `UX_Code` (`Code`),
  ADD UNIQUE KEY `UX_Email` (`Email`);
ALTER TABLE `Order Details`
  ADD KEY `ProductsOrder Details` (`ProductID`);
ALTER TABLE `Orders`
  ADD KEY `CustomerID` (`CustomerID`),
  ADD KEY `ShippersOrders` (`ShipperCode`);
ALTER TABLE `Products`
  ADD KEY `Name` (`Name`),
  ADD KEY `SuppliersProducts` (`SupplierID`);

-- 4. Checks

ALTER TABLE `Order Details`
  ADD CONSTRAINT `chk_Order Details_Qty` CHECK (`Qty` > 0);

-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;
ALTER TABLE `CustomerLoyaltyProgramMembershipHistory`
  ADD CONSTRAINT `CustomerLoyaltyProgramMembersCustomerLoyaltyProgramMembership` FOREIGN KEY (`TierID`) REFERENCES `CustomerLoyaltyProgramMembershipTiers` (`TierID`);
ALTER TABLE `Order Details`
  ADD CONSTRAINT `OrdersOrder Details` FOREIGN KEY (`OrderID`) REFERENCES `Orders` (`OrderID`) ON DELETE CASCADE,
  ADD CONSTRAINT `ProductsOrder Details` FOREIGN KEY (`ProductID`) REFERENCES `Products` (`ProductID`);
ALTER TABLE `Orders`
  ADD CONSTRAINT `CustomersOrders` FOREIGN KEY (`CustomerID`) REFERENCES `Customers` (`CustomerID`) ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT `ShippersOrders` FOREIGN KEY (`ShipperCode`) REFERENCES `Shippers` (`Code`) ON DELETE SET NULL;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;

