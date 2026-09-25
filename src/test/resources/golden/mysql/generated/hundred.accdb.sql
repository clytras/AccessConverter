-- AccessConverter - source: hundred.accdb (V2010)
-- Target: MySQL 8.0.13 or later, collation utf8mb4_0900_as_ci

SET NAMES utf8mb4;
SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks, @ac_ac = @@SESSION.autocommit;
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION';
SET SESSION foreign_key_checks = 0, autocommit = 0;

-- 1. Tables

CREATE TABLE `After` (
  `ID` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci;
CREATE TABLE `Hundred` (
  `ID` INT NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_ci AUTO_INCREMENT=101;

-- 2. Data: one transaction per table

INSERT INTO `After` (`ID`) VALUES
(1);
COMMIT;
INSERT INTO `Hundred` (`ID`) VALUES
(1),
(2),
(3),
(4),
(5),
(6),
(7),
(8),
(9),
(10),
(11),
(12),
(13),
(14),
(15),
(16),
(17),
(18),
(19),
(20),
(21),
(22),
(23),
(24),
(25),
(26),
(27),
(28),
(29),
(30),
(31),
(32),
(33),
(34),
(35),
(36),
(37),
(38),
(39),
(40),
(41),
(42),
(43),
(44),
(45),
(46),
(47),
(48),
(49),
(50),
(51),
(52),
(53),
(54),
(55),
(56),
(57),
(58),
(59),
(60),
(61),
(62),
(63),
(64),
(65),
(66),
(67),
(68),
(69),
(70),
(71),
(72),
(73),
(74),
(75),
(76),
(77),
(78),
(79),
(80),
(81),
(82),
(83),
(84),
(85),
(86),
(87),
(88),
(89),
(90),
(91),
(92),
(93),
(94),
(95),
(96),
(97),
(98),
(99),
(100);
COMMIT;

-- 3. Secondary indexes


-- 4. Checks


-- 5. Foreign keys, validated by the server

SET SESSION foreign_key_checks = 1;

SET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;
