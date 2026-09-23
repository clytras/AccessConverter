CREATE TABLE "TableIgnoreNulls1" (
  "row" VARCHAR(50) NOT NULL,
  "data" VARCHAR(50),
  PRIMARY KEY ("row")
);
CREATE TABLE "TableIgnoreNulls1_temp" (
  "row" VARCHAR(50) NOT NULL,
  "data" VARCHAR(50),
  PRIMARY KEY ("row")
);
CREATE TABLE "TableIgnoreNulls2" (
  "row" VARCHAR(50) NOT NULL,
  "data1" VARCHAR(50),
  "data2" INTEGER DEFAULT 0,
  PRIMARY KEY ("row")
);
CREATE TABLE "TableIgnoreNulls2_temp" (
  "row" VARCHAR(50) NOT NULL,
  "data1" VARCHAR(50),
  "data2" INTEGER DEFAULT 0,
  PRIMARY KEY ("row")
);
CREATE TABLE "TableUnique1_temp" (
  "row" VARCHAR(50) NOT NULL,
  "data" VARCHAR(50),
  PRIMARY KEY ("row")
);
CREATE TABLE "TableUnique2_temp" (
  "row" VARCHAR(50) NOT NULL,
  "data1" VARCHAR(50),
  "data2" INTEGER DEFAULT 0,
  PRIMARY KEY ("row")
);
CREATE INDEX "TableIgnoreNulls1_DataIndex" ON "TableIgnoreNulls1" ("data") WHERE "data" IS NOT NULL;
CREATE INDEX "TableIgnoreNulls1_temp_DataIndex" ON "TableIgnoreNulls1_temp" ("data") WHERE "data" IS NOT NULL;
CREATE INDEX "TableIgnoreNulls2_DataIndex" ON "TableIgnoreNulls2" ("data1", "data2") WHERE "data1" IS NOT NULL OR "data2" IS NOT NULL;
CREATE INDEX "TableIgnoreNulls2_temp_DataIndex" ON "TableIgnoreNulls2_temp" ("data1", "data2") WHERE "data1" IS NOT NULL OR "data2" IS NOT NULL;
CREATE UNIQUE INDEX "TableUnique1_temp_DataIndex" ON "TableUnique1_temp" ("data");
CREATE UNIQUE INDEX "TableUnique2_temp_DataIndex" ON "TableUnique2_temp" ("data1", "data2");
INSERT INTO "TableIgnoreNulls1" VALUES('row0', 'stuff');
INSERT INTO "TableIgnoreNulls1" VALUES('row1', NULL);
INSERT INTO "TableIgnoreNulls1" VALUES('row2', 'more');
INSERT INTO "TableIgnoreNulls1" VALUES('row3', NULL);
INSERT INTO "TableIgnoreNulls1" VALUES('row4', 'some');
INSERT INTO "TableIgnoreNulls1" VALUES('row5', NULL);
INSERT INTO "TableIgnoreNulls1" VALUES('row6', 'lots of stuff');
INSERT INTO "TableIgnoreNulls1" VALUES('row7', 'another row');
INSERT INTO "TableIgnoreNulls1" VALUES('row8', NULL);
INSERT INTO "TableIgnoreNulls1" VALUES('row9', 'still more rows');
INSERT INTO "TableIgnoreNulls2" VALUES('row0', 'stuff', 13);
INSERT INTO "TableIgnoreNulls2" VALUES('row1', NULL, 49);
INSERT INTO "TableIgnoreNulls2" VALUES('row10', NULL, -5453);
INSERT INTO "TableIgnoreNulls2" VALUES('row2', 'more', NULL);
INSERT INTO "TableIgnoreNulls2" VALUES('row3', NULL, -45);
INSERT INTO "TableIgnoreNulls2" VALUES('row4', 'some', 0);
INSERT INTO "TableIgnoreNulls2" VALUES('row5', NULL, NULL);
INSERT INTO "TableIgnoreNulls2" VALUES('row6', 'lots of stuff', 9087);
INSERT INTO "TableIgnoreNulls2" VALUES('row7', 'another row', -3462);
INSERT INTO "TableIgnoreNulls2" VALUES('row8', NULL, 0);
INSERT INTO "TableIgnoreNulls2" VALUES('row9', 'still more rows', -12);
INSERT INTO "TableUnique1_temp" VALUES('row0', 'stuff');
INSERT INTO "TableUnique1_temp" VALUES('row1', NULL);
INSERT INTO "TableUnique1_temp" VALUES('row2', 'more');
INSERT INTO "TableUnique1_temp" VALUES('row3', NULL);
INSERT INTO "TableUnique1_temp" VALUES('row4', 'some');
INSERT INTO "TableUnique1_temp" VALUES('row5', NULL);
INSERT INTO "TableUnique1_temp" VALUES('row6', 'lots of stuff');
INSERT INTO "TableUnique1_temp" VALUES('row7', 'another row');
INSERT INTO "TableUnique1_temp" VALUES('row8', NULL);
INSERT INTO "TableUnique1_temp" VALUES('row9', 'still more rows');
INSERT INTO "TableUnique2_temp" VALUES('row0', 'stuff', 13);
INSERT INTO "TableUnique2_temp" VALUES('row1', NULL, 49);
INSERT INTO "TableUnique2_temp" VALUES('row10', NULL, NULL);
INSERT INTO "TableUnique2_temp" VALUES('row2', 'more', NULL);
INSERT INTO "TableUnique2_temp" VALUES('row3', NULL, -45);
INSERT INTO "TableUnique2_temp" VALUES('row4', 'some', 0);
INSERT INTO "TableUnique2_temp" VALUES('row5', NULL, NULL);
INSERT INTO "TableUnique2_temp" VALUES('row6', 'lots of stuff', 9087);
INSERT INTO "TableUnique2_temp" VALUES('row7', 'another row', -3462);
INSERT INTO "TableUnique2_temp" VALUES('row8', NULL, 0);
INSERT INTO "TableUnique2_temp" VALUES('row9', 'still more rows', -12);
