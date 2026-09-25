-- AccessConverter - source: uniqueProbe.accdb (V2010)
-- Target: MySQL 8.0.13 or later, collation utf8mb4_0900_as_ci

SET NAMES utf8mb4;
SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks, @ac_ac = @@SESSION.autocommit;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION foreign_key_checks = 0, autocommit = 0;

-- 1. Tables

CREATE TABLE `T_accent_variants` (
  `v` VARCHAR(20)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci;
CREATE TABLE `T_case_variants` (
  `v` VARCHAR(20)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci;
CREATE TABLE `T_eszett_ss` (
  `v` VARCHAR(20) COLLATE utf8mb4_bin
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci;
CREATE TABLE `T_trailing_space` (
  `v` VARCHAR(20)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci;

-- 2. Data: one transaction per table

INSERT INTO `T_accent_variants` (`v`) VALUES
('resume'),
('résumé');
COMMIT;
INSERT INTO `T_case_variants` (`v`) VALUES
('Code');
COMMIT;
INSERT INTO `T_eszett_ss` (`v`) VALUES
('straße');
COMMIT;
INSERT INTO `T_trailing_space` (`v`) VALUES
('abc');
COMMIT;

-- 3. Secondary indexes

ALTER TABLE `T_accent_variants`
  ADD UNIQUE KEY `u` (`v`);
ALTER TABLE `T_case_variants`
  ADD UNIQUE KEY `u` (`v`);
ALTER TABLE `T_eszett_ss`
  ADD UNIQUE KEY `u` (`v`);
ALTER TABLE `T_trailing_space`
  ADD UNIQUE KEY `u` (`v`);

-- 4. Checks


-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;
