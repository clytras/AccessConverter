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
CREATE TABLE `Table1_attach_data` (
  `id` INT NOT NULL,
  `attach_data_ref` INT NOT NULL,
  `file_name` VARCHAR(255),
  `file_type` VARCHAR(255),
  `file_data` LONGBLOB,
  `file_size` BIGINT,
  `file_url` LONGTEXT,
  `file_timestamp` DATETIME,
  `file_flags` INT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci COMMENT='The attachments of Table1.attach_data';

-- 2. Data: one transaction per table

COMMIT;
COMMIT;

-- 3. Secondary indexes

ALTER TABLE `Table1`
  ADD UNIQUE KEY `attach_data_5C3A6A554ECB44BC86D7CDF1DD11F568` (`attach_data`);
ALTER TABLE `Table1_attach_data`
  ADD KEY `attach_data_ref` (`attach_data_ref`);

-- 4. Checks


-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;
ALTER TABLE `Table1_attach_data`
  ADD CONSTRAINT `Table1_attach_data` FOREIGN KEY (`attach_data_ref`) REFERENCES `Table1` (`attach_data`) ON DELETE CASCADE ON UPDATE CASCADE;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;

