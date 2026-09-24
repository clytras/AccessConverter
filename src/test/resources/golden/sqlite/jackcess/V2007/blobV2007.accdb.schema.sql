CREATE TABLE "Table1" (
  "ID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "name" VARCHAR(255),
  "ole_data" BLOB,
  "attach_data" INTEGER NOT NULL
);
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE "Table1_attach_data" (
  -- The attachments of Table1.attach_data
  "id" INTEGER PRIMARY KEY NOT NULL,
  "attach_data_ref" INTEGER NOT NULL,
  "file_name" VARCHAR(255),
  "file_type" VARCHAR(255),
  "file_data" BLOB,
  "file_size" BIGINT,
  "file_url" TEXT,
  "file_timestamp" DATETIME,
  "file_flags" INTEGER,
  FOREIGN KEY ("attach_data_ref") REFERENCES "Table1" ("attach_data") ON UPDATE CASCADE ON DELETE CASCADE -- Access relationship: Table1_attach_data
);
CREATE UNIQUE INDEX "Table1_attach_data_5C3A6A554ECB44BC86D7CDF1DD11F568" ON "Table1" ("attach_data");
CREATE INDEX "Table1_attach_data_attach_data_ref" ON "Table1_attach_data" ("attach_data_ref");
