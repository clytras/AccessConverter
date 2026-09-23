-- AccessConverter - source: calcFieldV2010.accdb (V2010)
-- Target: MySQL 8.0 or later, collation utf8mb4_0900_as_ci

SET NAMES utf8mb4;
SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks, @ac_ac = @@SESSION.autocommit;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION foreign_key_checks = 0, autocommit = 0;

-- 1. Tables

CREATE TABLE `Table1` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  `FirstName` VARCHAR(255),
  `LastName` VARCHAR(255),
  `LastFirst` VARCHAR(243) COMMENT 'Access expression: [LastName] & \", \" & [FirstName]',
  `City` VARCHAR(255),
  `LastFirstLen` INT COMMENT 'Access expression: Len([LastFirst])',
  `Salary` DECIMAL(19,4),
  `MonthlySalary` DECIMAL(19,4) COMMENT 'Access expression: [Salary]/12',
  `IsRich` BOOLEAN NOT NULL DEFAULT 0 COMMENT 'Access expression: [Salary]>100000',
  `AllNames` LONGTEXT COMMENT 'Access expression: [LastName] & \", \" & [FirstName] & \"=\" & [LastFirst]',
  `WeeklySalary` DECIMAL(44,16) COMMENT 'Access expression: [Salary]/52',
  `SalaryTest` DECIMAL(19,4) COMMENT 'Access expression: [Salary]',
  `BoolTest` BOOLEAN NOT NULL DEFAULT 0 COMMENT 'Access expression: True',
  `Popularity` DECIMAL(18,6) DEFAULT 0,
  `DecimalTest` DECIMAL(34,6) COMMENT 'Access expression: [Popularity]',
  `FloatTest` FLOAT COMMENT 'Access expression: [Salary]*0.13/[DecimalTest]',
  `BigNumTest` DECIMAL(56,28) COMMENT 'Access expression: ([Salary]*[MonthlySalary]/[DecimalTest])*34.12342134',
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci AUTO_INCREMENT=5;

-- 2. Data: one transaction per table

INSERT INTO `Table1` (`ID`, `FirstName`, `LastName`, `LastFirst`, `City`, `LastFirstLen`, `Salary`, `MonthlySalary`, `IsRich`, `AllNames`, `WeeklySalary`, `SalaryTest`, `BoolTest`, `Popularity`, `DecimalTest`, `FloatTest`, `BigNumTest`) VALUES
(1, 'Bruce', 'Wayne', 'Wayne, Bruce', 'Gotham', 12, 1000000.0000, 83333.3333, 1, 'Wayne, Bruce=Wayne, Bruce', 19230.7692307692, 1000000.0000, 1, 50.325000, 50.325000, 2583.2092, 56505085819.424791296572280180),
(2, 'Bart', 'Simpson', 'Simpson, Bart', 'Springfield', 13, -1.0000, -0.0833, 0, 'Simpson, Bart=Simpson, Bart', -0.0192307692307692, -1.0000, 1, -36.222200, -36.222200, 0.0035889593, -0.0784734499180612994241100748),
(3, 'John', 'Doe', 'Doe, John', 'Nowhere', 9, 0.0000, 0.0000, 0, 'Doe, John=Doe, John', 0, 0.0000, 1, 0.012300, 0.012300, 0.0, 0.00000000),
(4, 'Test', 'User', 'User, Test', 'Hockessin', 10, 100.0000, 8.3333, 0, 'User, Test=User, Test', 1.92307692307692, 100.0000, 1, 102030405060.654321, 102030405060.654321, 1.27413E-10, 0.0000002787019289824216980830);
COMMIT;

-- 3. Secondary indexes


-- 4. Checks


-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;
