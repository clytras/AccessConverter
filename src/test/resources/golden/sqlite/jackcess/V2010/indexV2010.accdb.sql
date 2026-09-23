CREATE TABLE "Table1" (
  "id" INTEGER PRIMARY KEY NOT NULL DEFAULT 0,
  "otherfk1" INTEGER DEFAULT 0,
  "otherfk2" INTEGER DEFAULT 0,
  "data" VARCHAR(50),
  "otherfk3" INTEGER DEFAULT 0,
  FOREIGN KEY ("otherfk1") REFERENCES "Table2" ("id") ON DELETE CASCADE, -- Access relationship: Table2Table1
  FOREIGN KEY ("otherfk2") REFERENCES "Table3" ("id") ON UPDATE CASCADE -- Access relationship: Table3Table1
);
CREATE TABLE "Table2" (
  "id" INTEGER PRIMARY KEY NOT NULL DEFAULT 0,
  "data" VARCHAR(50)
);
CREATE TABLE "Table3" (
  "id" INTEGER PRIMARY KEY NOT NULL DEFAULT 0,
  "data" VARCHAR(50)
);
CREATE INDEX "Table1_Table2Table1" ON "Table1" ("otherfk1");
CREATE INDEX "Table1_Table3Table1" ON "Table1" ("otherfk2");
INSERT INTO "Table1" VALUES(0, 0, 10, 'baz0', 0);
INSERT INTO "Table1" VALUES(1, 1, 11, 'baz11', 0);
INSERT INTO "Table1" VALUES(2, 1, 11, 'baz11-2', 0);
INSERT INTO "Table1" VALUES(3, 2, 13, 'baz13', 0);
INSERT INTO "Table2" VALUES(0, 'foo0');
INSERT INTO "Table2" VALUES(1, 'foo1');
INSERT INTO "Table2" VALUES(2, 'foo2');
INSERT INTO "Table3" VALUES(10, 'bar10');
INSERT INTO "Table3" VALUES(11, 'bar11');
INSERT INTO "Table3" VALUES(12, 'bar12');
INSERT INTO "Table3" VALUES(13, 'bar13');
