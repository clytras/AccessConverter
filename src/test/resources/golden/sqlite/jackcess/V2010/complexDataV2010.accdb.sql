CREATE TABLE "Table1" (
  "id" VARCHAR(255) NOT NULL,
  "memo-data" TEXT,
  "append-memo-data" TEXT,
  "multi-value-data" INTEGER NOT NULL,
  "attach-data" INTEGER NOT NULL,
  PRIMARY KEY ("id")
);
CREATE TABLE "Table1_attach-data" (
  -- The attachments of Table1.attach-data
  "id" INTEGER PRIMARY KEY NOT NULL,
  "attach-data_ref" INTEGER NOT NULL,
  "file_name" VARCHAR(255),
  "file_type" VARCHAR(255),
  "file_data" BLOB,
  "file_size" BIGINT,
  "file_url" TEXT,
  "file_timestamp" DATETIME,
  "file_flags" INTEGER,
  FOREIGN KEY ("attach-data_ref") REFERENCES "Table1" ("attach-data") ON UPDATE CASCADE ON DELETE CASCADE -- Access relationship: Table1_attach-data
);
CREATE TABLE "Table1_multi-value-data" (
  -- The values of Table1.multi-value-data
  "id" INTEGER PRIMARY KEY NOT NULL,
  "multi-value-data_ref" INTEGER NOT NULL,
  "value" VARCHAR(255),
  FOREIGN KEY ("multi-value-data_ref") REFERENCES "Table1" ("multi-value-data") ON UPDATE CASCADE ON DELETE CASCADE -- Access relationship: Table1_multi-value-data
);
CREATE UNIQUE INDEX "Table1_attach-data_071D71EDD53D45A1A9089929F06857D9" ON "Table1" ("attach-data");
CREATE UNIQUE INDEX "Table1_multi-value-data_F4C67B0F60124C1989D5583C00CF76E2" ON "Table1" ("multi-value-data");
CREATE INDEX "Table1_attach-data_attach-data_ref" ON "Table1_attach-data" ("attach-data_ref");
CREATE INDEX "Table1_multi-value-data_multi-value-data_ref" ON "Table1_multi-value-data" ("multi-value-data_ref");
INSERT INTO "Table1" VALUES('row1', NULL, NULL, 1, 1);
INSERT INTO "Table1" VALUES('row2', 'row2-memo', 'row2-memo', 2, 2);
INSERT INTO "Table1" VALUES('row3', 'row3-memo', 'row3-memo-again', 3, 3);
INSERT INTO "Table1" VALUES('row4', 'row4-memo', 'row4-memo', 4, 4);
INSERT INTO "Table1_attach-data" VALUES(1, 2, 'test_data.txt', 'txt', X'7468697320697320736F6D652074657374206461746120666F72206174746163686D656E742E', 38, NULL, NULL, NULL);
INSERT INTO "Table1_attach-data" VALUES(2, 2, 'test_data2.txt', 'txt', X'7468697320697320736F6D65206D6F72652074657374206461746120666F72206174746163686D656E742E', 43, NULL, NULL, NULL);
INSERT INTO "Table1_attach-data" VALUES(3, 4, 'test_data2.txt', 'txt', X'7468697320697320736F6D65206D6F72652074657374206461746120666F72206174746163686D656E742E', 43, NULL, NULL, NULL);
INSERT INTO "Table1_multi-value-data" VALUES(1, 2, 'value1');
INSERT INTO "Table1_multi-value-data" VALUES(2, 2, 'value4');
INSERT INTO "Table1_multi-value-data" VALUES(3, 3, 'value1');
INSERT INTO "Table1_multi-value-data" VALUES(4, 3, 'value2');
INSERT INTO "Table1_multi-value-data" VALUES(5, 3, 'value3');
INSERT INTO "Table1_multi-value-data" VALUES(6, 3, 'value4');
