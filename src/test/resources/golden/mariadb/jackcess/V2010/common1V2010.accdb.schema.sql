-- AccessConverter - source: common1V2010.accdb (V2010)
-- Target: MariaDB 10.11 or later, collation utf8mb4_uca1400_as_ci

SET NAMES utf8mb4;
SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks, @ac_ac = @@SESSION.autocommit;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION foreign_key_checks = 0, autocommit = 0;

-- 1. Tables

CREATE TABLE `Table1` (
  `A` VARCHAR(50) NOT NULL,
  `B` VARCHAR(100),
  `C` TINYINT UNSIGNED DEFAULT 0,
  `D` SMALLINT DEFAULT 0,
  `E` INT DEFAULT 0,
  `F` DOUBLE DEFAULT 0,
  `G` DATETIME,
  `H` DECIMAL(19,4) DEFAULT 0,
  `I` BOOLEAN NOT NULL DEFAULT 0,
  PRIMARY KEY (`A`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Table2` (
  `column1` VARCHAR(50) NOT NULL,
  `column2` TEXT,
  `column3` TEXT,
  `column4` TEXT,
  `column5` TEXT,
  `column6` TEXT,
  `column7` TEXT,
  `column8` TEXT,
  `column9` TEXT,
  `column10` VARCHAR(50),
  `column11` TEXT,
  `column12` TEXT,
  `column13` TEXT,
  `column14` VARCHAR(50),
  `column15` TEXT,
  `column16` TEXT,
  `column17` TEXT,
  `column18` TEXT,
  `column19` TEXT,
  `column20` TEXT,
  `column21` TEXT,
  `column22` TEXT,
  `column23` TEXT,
  `column24` TEXT,
  `column25` TEXT,
  `column26` TEXT,
  `column27` TEXT,
  `column28` TEXT,
  `column29` TEXT,
  `column30` TEXT,
  `column31` TEXT,
  `column32` TEXT,
  `column33` TEXT,
  `column34` TEXT,
  `column35` TEXT,
  `column36` TEXT,
  `column37` TEXT,
  `column38` TEXT,
  `column39` TEXT,
  `column40` TEXT,
  `column41` TEXT,
  `column42` TEXT,
  `column43` TEXT,
  `column44` TEXT,
  `column45` TEXT,
  `column46` TEXT,
  `column47` TEXT,
  `column48` TEXT,
  `column49` TEXT,
  `column50` TEXT,
  `column51` TEXT,
  `column52` TEXT,
  `column53` TEXT,
  `column54` TEXT,
  `column55` TEXT,
  `column56` TEXT,
  `column57` TEXT,
  `column58` TEXT,
  `column59` VARCHAR(50),
  `column60` VARCHAR(50),
  `column61` VARCHAR(50),
  `column62` VARCHAR(50),
  `column63` VARCHAR(50),
  `column64` VARCHAR(50),
  `column65` VARCHAR(50),
  `column66` VARCHAR(50),
  `column67` VARCHAR(50),
  `column68` VARCHAR(50),
  `column69` VARCHAR(50),
  `column70` VARCHAR(50),
  `column71` VARCHAR(50),
  `column72` VARCHAR(50),
  `column73` VARCHAR(50),
  `column74` VARCHAR(50),
  `column75` VARCHAR(50),
  `column76` VARCHAR(50),
  `column77` VARCHAR(50),
  `column78` VARCHAR(50),
  `column79` VARCHAR(50),
  `column80` VARCHAR(50),
  `column81` VARCHAR(50),
  `column82` VARCHAR(50),
  `column83` VARCHAR(50),
  `column84` VARCHAR(50),
  `column85` VARCHAR(50),
  `column86` VARCHAR(50),
  `column87` VARCHAR(50),
  `column88` VARCHAR(50),
  `column89` VARCHAR(50),
  PRIMARY KEY (`column1`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Table3` (
  `a` INT NOT NULL AUTO_INCREMENT,
  `b` VARCHAR(50),
  PRIMARY KEY (`a`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;
CREATE TABLE `Table4` (
  `name` VARCHAR(50),
  `data` CHAR(38) NOT NULL DEFAULT (CONCAT('{', UPPER(UUID()), '}')),
  PRIMARY KEY (`data`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_uca1400_as_ci;

-- 2. Data: one transaction per table

COMMIT;
COMMIT;
COMMIT;
COMMIT;

-- 3. Secondary indexes

ALTER TABLE `Table1`
  ADD KEY `B` (`B`);
ALTER TABLE `Table2`
  ADD KEY `column10` (`column10`),
  ADD KEY `column14` (`column14`),
  ADD KEY `column75` (`column75`);

-- 4. Checks

ALTER TABLE `Table1`
  ADD CONSTRAINT `chk_Table1_A_nonempty` CHECK (CHAR_LENGTH(`A`) > 0),
  ADD CONSTRAINT `chk_Table1_B_nonempty` CHECK (CHAR_LENGTH(`B`) > 0);
ALTER TABLE `Table3`
  ADD CONSTRAINT `chk_Table3_b_nonempty` CHECK (CHAR_LENGTH(`b`) > 0);
ALTER TABLE `Table4`
  ADD CONSTRAINT `chk_Table4_name_nonempty` CHECK (CHAR_LENGTH(`name`) > 0);

-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;

