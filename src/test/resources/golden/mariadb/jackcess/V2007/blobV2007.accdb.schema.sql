-- AccessConverter - source: blobV2007.accdb (V2007)
-- Target: MariaDB 10.11 or later, collation utf8mb4_uca1400_as_ci

SET NAMES utf8mb4;
SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks, @ac_ac = @@SESSION.autocommit;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION foreign_key_checks = 0, autocommit = 0;

-- 1. Tables

CREATE TABLE `Table1` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(255),
  `ole_data` LONGBLOB,
  `attach_data` INT NOT NULL,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci AUTO_INCREMENT=13;

-- 2. Data: one transaction per table

COMMIT;

-- 3. Secondary indexes


-- 4. Checks


-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;

