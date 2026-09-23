CREATE TABLE "T_accent_variants" (
  "v" VARCHAR(20)
);
CREATE TABLE "T_case_variants" (
  "v" VARCHAR(20)
);
CREATE TABLE "T_eszett_ss" (
  "v" VARCHAR(20)
);
CREATE TABLE "T_trailing_space" (
  "v" VARCHAR(20)
);
CREATE UNIQUE INDEX "T_accent_variants_u" ON "T_accent_variants" ("v");
CREATE UNIQUE INDEX "T_case_variants_u" ON "T_case_variants" ("v");
CREATE UNIQUE INDEX "T_eszett_ss_u" ON "T_eszett_ss" ("v");
CREATE UNIQUE INDEX "T_trailing_space_u" ON "T_trailing_space" ("v");
INSERT INTO "T_accent_variants" VALUES('resume');
INSERT INTO "T_accent_variants" VALUES('résumé');
INSERT INTO "T_case_variants" VALUES('Code');
INSERT INTO "T_eszett_ss" VALUES('straße');
INSERT INTO "T_trailing_space" VALUES('abc');
