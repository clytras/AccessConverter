CREATE TABLE "TextEdge" (
  "ID" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  "Label" VARCHAR(30),
  "Val" VARCHAR(255)
);
CREATE TABLE sqlite_sequence(name,seq);
INSERT INTO "TextEdge" VALUES(1, 'windows-path', 'C:\temp\new');
INSERT INTO "TextEdge" VALUES(2, 'trailing-backslash', 'ends with backslash\');
INSERT INTO "TextEdge" VALUES(3, 'quotes', 'single '' and double "');
INSERT INTO "TextEdge" VALUES(4, 'newline', 'line1
line2');
INSERT INTO "TextEdge" VALUES(5, 'emoji', 'emoji 😀 and Ωμέγα');
INSERT INTO "TextEdge" VALUES(6, 'ctrl-z', 'ab');
INSERT INTO "TextEdge" VALUES(7, 'after', 'row after the tricky ones');
INSERT INTO "sqlite_sequence" VALUES('TextEdge', 7);
