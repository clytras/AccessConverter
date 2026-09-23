CREATE TABLE "Table1" (
  "id" VARCHAR(255) NOT NULL,
  "memo-data" TEXT,
  "append-memo-data" TEXT,
  "multi-value-data" INTEGER NOT NULL,
  "attach-data" INTEGER NOT NULL,
  PRIMARY KEY ("id")
);
INSERT INTO "Table1" VALUES('row1', NULL, NULL, 1, 1);
INSERT INTO "Table1" VALUES('row2', 'row2-memo', 'row2-memo', 2, 2);
INSERT INTO "Table1" VALUES('row3', 'row3-memo', 'row3-memo-again', 3, 3);
INSERT INTO "Table1" VALUES('row4', 'row4-memo', 'row4-memo', 4, 4);
