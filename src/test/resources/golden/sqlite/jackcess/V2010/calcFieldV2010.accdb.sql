CREATE TABLE "Table1" (
  "ID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "FirstName" VARCHAR(255),
  "LastName" VARCHAR(255),
  "LastFirst" VARCHAR(243), -- Access expression: [LastName] & ", " & [FirstName]
  "City" VARCHAR(255),
  "LastFirstLen" INTEGER, -- Access expression: Len([LastFirst])
  "Salary" DECIMAL(19,4),
  "MonthlySalary" DECIMAL(19,4), -- Access expression: [Salary]/12
  "IsRich" BOOLEAN NOT NULL DEFAULT 0, -- Access expression: [Salary]>100000
  "AllNames" TEXT, -- Access expression: [LastName] & ", " & [FirstName] & "=" & [LastFirst]
  "WeeklySalary" TEXT, -- Access expression: [Salary]/52
  "SalaryTest" DECIMAL(19,4), -- Access expression: [Salary]
  "BoolTest" BOOLEAN NOT NULL DEFAULT 0, -- Access expression: True
  "Popularity" TEXT DEFAULT 0,
  "DecimalTest" TEXT, -- Access expression: [Popularity]
  "FloatTest" FLOAT, -- Access expression: [Salary]*0.13/[DecimalTest]
  "BigNumTest" TEXT -- Access expression: ([Salary]*[MonthlySalary]/[DecimalTest])*34.12342134
);
CREATE TABLE sqlite_sequence(name,seq);
INSERT INTO "Table1" VALUES(1, 'Bruce', 'Wayne', 'Wayne, Bruce', 'Gotham', 12, 1000000, 83333.3333, 1, 'Wayne, Bruce=Wayne, Bruce', '19230.7692307692', 1000000, 1, '50.325000', '50.325000', 2583.2092, '56505085819.424791296572280180');
INSERT INTO "Table1" VALUES(2, 'Bart', 'Simpson', 'Simpson, Bart', 'Springfield', 13, -1, -0.0833, 0, 'Simpson, Bart=Simpson, Bart', '-0.0192307692307692', -1, 1, '-36.222200', '-36.222200', 0.0035889593, '-0.0784734499180612994241100748');
INSERT INTO "Table1" VALUES(3, 'John', 'Doe', 'Doe, John', 'Nowhere', 9, 0, 0, 0, 'Doe, John=Doe, John', '0', 0, 1, '0.012300', '0.012300', 0.0, '0.00000000');
INSERT INTO "Table1" VALUES(4, 'Test', 'User', 'User, Test', 'Hockessin', 10, 100, 8.3333, 0, 'User, Test=User, Test', '1.92307692307692', 100, 1, '102030405060.654321', '102030405060.654321', 1.27413e-10, '0.0000002787019289824216980830');
INSERT INTO "sqlite_sequence" VALUES('Table1', 4);
