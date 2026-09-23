-- AccessConverter - source: textEdge.accdb (V2010)
-- Target: MySQL 8.0 or later, collation utf8mb4_0900_as_ci

SET NAMES utf8mb4;
SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks, @ac_ac = @@SESSION.autocommit;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION foreign_key_checks = 0, autocommit = 0;

-- 1. Tables

CREATE TABLE `TextEdge` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `Label` VARCHAR(30),
  `Val` VARCHAR(255),
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci AUTO_INCREMENT=8;

-- 2. Data: one transaction per table

INSERT INTO `TextEdge` (`ID`, `Label`, `Val`) VALUES
(1, 'windows-path', 'C:\\temp\\new'),
(2, 'trailing-backslash', 'ends with backslash\\'),
(3, 'quotes', 'single \' and double \"'),
(4, 'newline', 'line1\nline2'),
(5, 'emoji', 'emoji 😀 and Ωμέγα'),
(6, 'ctrl-z', 'a\Zb'),
(7, 'after', 'row after the tricky ones');
COMMIT;

-- 3. Secondary indexes


-- 4. Checks


-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;
