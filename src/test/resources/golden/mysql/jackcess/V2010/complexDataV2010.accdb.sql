-- AccessConverter - source: complexDataV2010.accdb (V2010)
-- Target: MySQL 8.0 or later, collation utf8mb4_0900_as_ci

SET NAMES utf8mb4;
SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks, @ac_ac = @@SESSION.autocommit;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION foreign_key_checks = 0, autocommit = 0;

-- 1. Tables

CREATE TABLE `Table1` (
  `id` VARCHAR(255) NOT NULL,
  `memo-data` LONGTEXT,
  `append-memo-data` LONGTEXT,
  `multi-value-data` INT NOT NULL,
  `attach-data` INT NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci;
CREATE TABLE `Table1_attach-data` (
  `id` INT NOT NULL,
  `attach-data_ref` INT NOT NULL,
  `file_name` VARCHAR(255),
  `file_type` VARCHAR(255),
  `file_data` LONGBLOB,
  `file_size` BIGINT,
  `file_url` LONGTEXT,
  `file_timestamp` DATETIME,
  `file_flags` INT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci COMMENT='The attachments of Table1.attach-data';
CREATE TABLE `Table1_multi-value-data` (
  `id` INT NOT NULL,
  `multi-value-data_ref` INT NOT NULL,
  `value` VARCHAR(255),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci COMMENT='The values of Table1.multi-value-data';

-- 2. Data: one transaction per table

INSERT INTO `Table1` (`id`, `memo-data`, `append-memo-data`, `multi-value-data`, `attach-data`) VALUES
('row1', NULL, NULL, 1, 1),
('row2', 'row2-memo', 'row2-memo', 2, 2),
('row3', 'row3-memo', 'row3-memo-again', 3, 3),
('row4', 'row4-memo', 'row4-memo', 4, 4);
COMMIT;
INSERT INTO `Table1_attach-data` (`id`, `attach-data_ref`, `file_name`, `file_type`, `file_data`, `file_size`, `file_url`, `file_timestamp`, `file_flags`) VALUES
(1, 2, 'test_data.txt', 'txt', X'7468697320697320736F6D652074657374206461746120666F72206174746163686D656E742E', 38, NULL, NULL, NULL),
(2, 2, 'test_data2.txt', 'txt', X'7468697320697320736F6D65206D6F72652074657374206461746120666F72206174746163686D656E742E', 43, NULL, NULL, NULL),
(3, 4, 'test_data2.txt', 'txt', X'7468697320697320736F6D65206D6F72652074657374206461746120666F72206174746163686D656E742E', 43, NULL, NULL, NULL);
COMMIT;
INSERT INTO `Table1_multi-value-data` (`id`, `multi-value-data_ref`, `value`) VALUES
(1, 2, 'value1'),
(2, 2, 'value4'),
(3, 3, 'value1'),
(4, 3, 'value2'),
(5, 3, 'value3'),
(6, 3, 'value4');
COMMIT;

-- 3. Secondary indexes

ALTER TABLE `Table1`
  ADD UNIQUE KEY `attach-data_071D71EDD53D45A1A9089929F06857D9` (`attach-data`),
  ADD UNIQUE KEY `multi-value-data_F4C67B0F60124C1989D5583C00CF76E2` (`multi-value-data`);
ALTER TABLE `Table1_attach-data`
  ADD KEY `attach-data_ref` (`attach-data_ref`);
ALTER TABLE `Table1_multi-value-data`
  ADD KEY `multi-value-data_ref` (`multi-value-data_ref`);

-- 4. Checks


-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;
ALTER TABLE `Table1_attach-data`
  ADD CONSTRAINT `Table1_attach-data` FOREIGN KEY (`attach-data_ref`) REFERENCES `Table1` (`attach-data`) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE `Table1_multi-value-data`
  ADD CONSTRAINT `Table1_multi-value-data` FOREIGN KEY (`multi-value-data_ref`) REFERENCES `Table1` (`multi-value-data`) ON DELETE CASCADE ON UPDATE CASCADE;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;
