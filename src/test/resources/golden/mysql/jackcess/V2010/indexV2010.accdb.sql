-- AccessConverter - source: indexV2010.accdb (V2010)
-- Target: MySQL 8.0 or later, collation utf8mb4_0900_as_ci

SET NAMES utf8mb4;
SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks, @ac_ac = @@SESSION.autocommit;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION foreign_key_checks = 0, autocommit = 0;

-- 1. Tables

CREATE TABLE `Table1` (
  `id` INT NOT NULL DEFAULT 0,
  `otherfk1` INT DEFAULT 0,
  `otherfk2` INT DEFAULT 0,
  `data` VARCHAR(50),
  `otherfk3` INT DEFAULT 0,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci;
CREATE TABLE `Table2` (
  `id` INT NOT NULL DEFAULT 0,
  `data` VARCHAR(50),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci;
CREATE TABLE `Table3` (
  `id` INT NOT NULL DEFAULT 0,
  `data` VARCHAR(50),
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci;

-- 2. Data: one transaction per table

INSERT INTO `Table1` (`id`, `otherfk1`, `otherfk2`, `data`, `otherfk3`) VALUES
(0, 0, 10, 'baz0', 0),
(1, 1, 11, 'baz11', 0),
(2, 1, 11, 'baz11-2', 0),
(3, 2, 13, 'baz13', 0);
COMMIT;
INSERT INTO `Table2` (`id`, `data`) VALUES
(0, 'foo0'),
(1, 'foo1'),
(2, 'foo2');
COMMIT;
INSERT INTO `Table3` (`id`, `data`) VALUES
(10, 'bar10'),
(11, 'bar11'),
(12, 'bar12'),
(13, 'bar13');
COMMIT;

-- 3. Secondary indexes

ALTER TABLE `Table1`
  ADD KEY `Table2Table1` (`otherfk1`),
  ADD KEY `Table3Table1` (`otherfk2`);

-- 4. Checks


-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;
ALTER TABLE `Table1`
  ADD CONSTRAINT `Table2Table1` FOREIGN KEY (`otherfk1`) REFERENCES `Table2` (`id`) ON DELETE CASCADE,
  ADD CONSTRAINT `Table3Table1` FOREIGN KEY (`otherfk2`) REFERENCES `Table3` (`id`) ON UPDATE CASCADE;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;
