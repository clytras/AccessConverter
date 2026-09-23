-- AccessConverter - source: extDateV2019.accdb (V2019)
-- Target: MariaDB 10.11 or later, collation utf8mb4_uca1400_as_ci

SET NAMES utf8mb4;
SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks, @ac_ac = @@SESSION.autocommit;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION foreign_key_checks = 0, autocommit = 0;

-- 1. Tables

CREATE TABLE `Table1` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `Field1` VARCHAR(255),
  `DateExt` DATETIME(6),
  `DateNormal` DATETIME,
  `DateExtStr` VARCHAR(255),
  `DateNormalCalc` DATETIME COMMENT 'Access expression: [DateNormal]',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci AUTO_INCREMENT=10;

-- 2. Data: one transaction per table

INSERT INTO `Table1` (`ID`, `Field1`, `DateExt`, `DateNormal`, `DateExtStr`, `DateNormalCalc`) VALUES
(1, 'row1', '2020-06-17 00:00:00.000000', '2020-06-17 00:00:00', '6/17/2020', '2020-06-17 00:00:00'),
(2, 'row2', '2021-06-14 00:00:00.000000', '2021-06-14 00:00:00', '6/14/2021', '2021-06-14 00:00:00'),
(3, 'row3', '2021-06-14 12:45:00.000000', '2021-06-14 12:45:00', '6/14/2021 12:45:00.0000000 PM', '2021-06-14 12:45:00'),
(4, 'row4', '2021-06-14 01:45:00.000000', '2021-06-14 01:45:00', '6/14/2021 1:45:00.0000000 AM', '2021-06-14 01:45:00'),
(5, 'row5', NULL, NULL, NULL, NULL),
(6, 'row6', '2021-06-14 22:45:12.345679', '2021-06-14 22:45:12', '6/14/2021 10:45:12.3456789 PM', '2021-06-14 22:45:12'),
(7, 'row7', '1765-06-14 12:45:00.000000', '1765-06-14 12:45:00', '6/14/1765 12:45:00.0000000 PM', '1765-06-14 12:45:00'),
(8, 'row8', '0100-06-14 12:45:00.123457', '0100-06-14 12:45:00', '6/14/100 12:45:00.1234567 PM', '0100-06-14 12:45:00'),
(9, 'row9', '1265-06-14 12:45:00.000000', '1265-06-14 12:45:00', '6/14/1265 12:45:00.0000000 PM', '1265-06-14 12:45:00');
COMMIT;

-- 3. Secondary indexes

ALTER TABLE `Table1`
  ADD KEY `DateExtAsc` (`DateExt`),
  ADD KEY `DateExtDesc` (`DateExt` DESC);

-- 4. Checks


-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;
