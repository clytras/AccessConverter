-- AccessConverter - source: complexDataV2010.accdb (V2010)
-- Target: MariaDB 10.11 or later, collation utf8mb4_uca1400_as_ci

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;

-- 2. Data: one transaction per table

INSERT INTO `Table1` (`id`, `memo-data`, `append-memo-data`, `multi-value-data`, `attach-data`) VALUES
('row1', NULL, NULL, 1, 1),
('row2', 'row2-memo', 'row2-memo', 2, 2),
('row3', 'row3-memo', 'row3-memo-again', 3, 3),
('row4', 'row4-memo', 'row4-memo', 4, 4);
COMMIT;

-- 3. Secondary indexes


-- 4. Checks


-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;
