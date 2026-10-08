# Changelog

All notable changes to AccessConverter are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/): a release with new features raises the minor version, one with only
fixes the patch version.

## [Unreleased]

## [3.3.0] - 2026-10-08

### Changed

- Jackcess 5.0.3 (from 5.0.1) and jackcess-encrypt 5.0.2 (from 5.0.0) read the Access files. Fewer warnings on
  Access 97 files from a non-English Access (Greek, for one): Jackcess now reads their catalog by itself, so they no
  longer report `CATALOG_INDEX_UNUSABLE`, and a conversion that had no other warning now exits 0 instead of 1. The
  tables and relationships found are the same. `CATALOG_INDEX_UNUSABLE` is now only reported for a damaged catalog
  index, which is still read by scanning the catalog.
- A date/time whose stored value isn't on a whole millisecond keeps the fraction of a second it holds, where it was
  rounded to milliseconds: Jackcess now reads the value that converts back to the same stored number. Dates and
  times entered in Access are whole seconds and unchanged.
- Two warnings describe the database instead of naming the library that reads it, with the same codes:
  `CATALOG_INDEX_UNUSABLE` says "this database's catalog index is damaged (…); the catalog was read by scanning it
  instead", and `UNSUPPORTED_COLUMN_TYPE` says "this column type (…) can't be decoded; its raw bytes are kept".
- Errors name what failed instead of a Java class, as `error: <what>: <reason>`: `verify --jdbc-url` that can't
  connect says `error: <url>: can't connect: …` (was `error: IOException: can't connect to <url>: …`), and an input
  that isn't there `error: <file>: no such file` (was `error: NoSuchFileException: …`). So do a missing or wrong
  `--jdbc-driver`, a database or SQLite output `verify` can't read back, and a SQLite output that fails while it is
  written, which is named as the output.
- A damaged index that fails while relationships are checked, before anything is written, reads like any damaged
  table: `error: <input>: reading table <name> failed (…); the file may be damaged: try Compact and Repair in Access`,
  where it was `error: table <name> failed: IOException: Unexpected order in index entries …`.

### Fixed

- Access 97 text can no longer lose the bytes Java leaves undefined in a code page (0x81, 0x8D, 0x8F, 0x90 and 0x9D in
  windows-1252, and the few in 874, 1253, 1255 and 1257 that Windows maps into the private use area). The charset they
  are decoded with, as Windows decodes them, could be handed Java's own decoder for the code page, which turns them into
  U+FFFD, silently and with exit 0, once Java's charset had decoded anything on the same thread. Nothing in 3.2.2 made
  that happen, but Jackcess 5.0.3, which this release brings, would have on every Access 97 file. The report, `inspect`
  and the `CHARSET_*` messages still name the code page's charset as before (`windows-1252`).

## [3.2.2] - 2026-09-27

### Fixed

- The Linux and Windows runtime images convert to SQLite with a read-only root filesystem or a `noexec` temporary
  folder, as in a sandbox (bubblewrap, systemd's `ProtectSystem=strict`): sqlite-jdbc's native library is now part of
  the image, in `lib/native`, instead of being unpacked into `java.io.tmpdir` at run time, and the JVM keeps no
  performance data there. SQLite used to fail with `Error opening connection` unless `java.io.tmpdir` was pointed at
  a writable folder that allows running programs. The container image was fixed in 3.2.1; the macOS images are
  unchanged, since macOS's temporary folder is always the user's own.
- An output that can't be written now fails with `error: <output>: <reason>`, naming the output as it was given and
  saying why in words: `error: nodir/out.json: the folder nodir doesn't exist`, and likewise `… is not a folder`,
  `no permission to write in the folder …`, `the disk is full` and `it is a folder`. It used to name a Java class
  and, for JSON and MySQL/MariaDB, the internal `.partial` file (`error: NoSuchFileException: nodir/out.json.partial`);
  SQLite said only `the SQLite output could not be written` and SQLite's own error.
- When the SQLite library can't be loaded, the error now says so, where it was loaded from or unpacked into, and what
  to do: `error: out.sqlite3: the SQLite library could not be unpacked into /tmp and loaded from there: make
  java.io.tmpdir a writable folder that allows running programs (…)`. It used to read as a problem with the output:
  `the SQLite output could not be written: Error opening connection`. `verify` of a SQLite file says the same.
- On macOS, the README's `xattr -dr com.apple.quarantine accessconverter-<version>-macos-*` now clears the download
  quarantine from every file of a runtime image and exits 0, silently: no file in the images is read-only any more.
  It used to print 55 `Permission denied` lines and exit 1, leaving the runtime's legal notices and CDS archives
  quarantined. On Windows, 7-Zip no longer unpacks the two CDS archives read-only, which made deleting the image
  with PowerShell's `Remove-Item -Recurse` fail.

## [3.2.1] - 2026-09-27

### Fixed

- `--on-table-error continue` now also goes on past a table that fails while the data is profiled: it is reported as
  `TABLE_READ_FAILED` and written empty, and the other tables are converted, as when a table fails while being
  written. It used to stop the whole conversion with no output and no report. `verify` reports such a table as a
  difference and compares the rest.
- A MySQL or MariaDB dump no longer needs memory several times the size of its largest value: a row larger than
  `--batch-bytes` is written as it is built, its binary values turned into hex a piece at a time. A database whose
  largest value is 6 MB needed more than 64 MB of heap, and one with a 50 MB value 384 MB; both now convert in the
  heap a JSON export of them needs. The dump itself is unchanged.
- Running out of memory is now a failed conversion like any other: exit 2 and one `error:` line naming the file and
  saying how to give Java more memory, with nothing on standard output and no output or `.partial` file left behind.
  It used to exit 1, which reads as success with warnings, with the JVM's stack trace and the `.partial` file left.
- An encrypted file opened without `--password` is now called "the file", not "the database": a Microsoft Money file
  isn't a database to its user (`the file is encrypted: pass --password`).
- The container image converts to SQLite with a read-only root filesystem (`--read-only`), or a `noexec` `/tmp`:
  sqlite-jdbc's native library is now part of the image instead of being unpacked into `/tmp` at run time, and the
  JVM keeps no performance data there. SQLite used to fail with `Error opening connection`.
- When the SQLite library can't be loaded, sqlite-jdbc's own log records (a timestamped line and a stack trace each)
  are no longer printed before the one `error:` line; `--verbose` shows them.
- On Java 24 and later, `java -jar accessconverter.jar` no longer prints four JVM warnings about a restricted method
  on every SQLite conversion or verification: the jar's manifest enables native access for sqlite-jdbc
  (`Enable-Native-Access: ALL-UNNAMED`). Java 21 ignores it; the runtime images and the container were not affected.

## [3.2.0] - 2026-09-26

### Added

- `--progress` / `--no-progress` on `convert` and `verify`: the stage, the table and its rows on one line on standard
  error, redrawn at most five times a second and erased at the end. It is on by default when standard input and
  output are a terminal; standard output, the output and the report are unchanged by it.
- `--linked resolve` on `convert`, `verify` and `inspect`: a table linked to another Access file is read from that
  file and converted like any other table, under its name in the linking database. The back-end is found by its
  file name in `--linked-root` (default: the input's directory), never at the path Access stored; an Access 97
  back-end is read with its own code page, an encrypted one with the password its link stores, else `--password`.
  The back-end's own relationships between the tables read from it come along, as foreign keys where Access
  enforces them. ODBC links, and links to links, stay skipped. JSON gives such a table a `linkedFrom`, and the
  published JSON Schema allows it.

### Changed

- The `LINKED_TABLE_SKIPPED` message now also suggests `--linked resolve`, so the conversion report and `inspect`
  output of a database with linked tables differ; the issue code and the converted outputs don't.

## [3.1.0] - 2026-09-25

### Added

- The container image can be built from a clone with nothing but Docker installed: `docker build -t accessconverter .`
  compiles the jar inside the build, byte for byte the jar a release builds from the same sources
  ([#14](https://github.com/clytras/AccessConverter/issues/14)).
- `--add-primary-key[=<column>]` gives every table without a primary key in Access an AutoNumber key column (`id`
  unless named), numbering the rows 1, 2, 3, ... in the order Access stores them, for the SQLite and MySQL/MariaDB
  targets; `verify` takes the same option
  ([#1](https://github.com/clytras/AccessConverter/issues/1)).

## [3.0.1] - 2026-09-25

AccessConverter 3 is a complete rewrite of 2.x: a new command line, new output formats and a new result format. The
[migration notes](docs/migrating-from-v2.md) map every 2.x option to its 3.x form.

### Changed

- **License:** Apache License 2.0. Versions 2.x and earlier remain under the MIT License.
- Converts to MySQL 8.0.13+ and MariaDB 10.11+ (as separate dialects), SQLite and JSON, exporting every value exactly
  or naming what it can't in a conversion report.
- Carries the Access schema as far as each target enforces it: primary keys, unique and plain indexes, foreign keys
  with their cascade rules, NOT NULL, defaults, validation rules as CHECK constraints, and descriptions as comments.
- Requires Java 21, or none with the runtime images.

### Added

- Every database Jackcess reads, Access 97 through Microsoft 365, including encrypted ones (`--password`) and Access 97
  files in any Windows code page, decoded exactly as Windows and Access decode them.
- `inspect`, to see what a conversion will read and report, and `verify`, to compare any output with its source.
- A documented JSON format with a published JSON Schema, as one document or one ndjson file per table.
- Binary, OLE and attachment data: raw bytes by default, `--ole-extract`, attachments and multi-value columns as child
  tables, `--binary inline|files|omit`.
- Runtime images for Linux, Windows and macOS that need no Java, a container image, checksums and build-provenance
  attestations.

### Fixed

- Every data-loss problem of 2.x: bytes over 127, clamped currency and decimals, truncated memos, reinterpreted
  backslashes, time-zone-dependent SQLite dates, NULL numbers written as 0, rounded Singles, dropped tables and rows,
  JSON crashes, duplicate indexes, wrong foreign keys, and non-English Access 97 databases losing most of their tables
  ([#5](https://github.com/clytras/AccessConverter/issues/5), [#8](https://github.com/clytras/AccessConverter/issues/8)).

## [2.0] - 2024-07-31

### Added

- Multiple primary keys (MySQL, SQLite).
- Unique and plain indexes (MySQL, SQLite).
- Relationships as foreign keys (MySQL, SQLite).
- Attachment files and OLE files (MySQL, SQLite, JSON).

### Changed

- Requires Java 17.
- Faster SQLite output with batch updates, and text dumps written through a file writer.
- Jackcess 4, SQLite JDBC 3.46.0 and other libraries updated.

## [1.1.1] - 2018-11-07

### Added

- Progress status with `-show-progress`.

### Fixed

- SQLite table and column names are quoted with the grave accent.
- A failed INSERT no longer breaks the whole transaction: each row is inserted on its own.

## [1.1] - 2018-05-24

### Added

- SQL code in the log on errors.

### Changed

- `org.apache.commons.text.TextStringBuilder` replaces the deprecated `org.apache.commons.lang3.StrBuilder`.

## [1.0] - 2017-09-06

- First release: Access databases to JSON, MySQL dumps and SQLite.

[Unreleased]: https://github.com/clytras/AccessConverter/compare/v3.3.0...HEAD
[3.3.0]: https://github.com/clytras/AccessConverter/compare/v3.2.2...v3.3.0
[3.2.2]: https://github.com/clytras/AccessConverter/compare/v3.2.1...v3.2.2
[3.2.1]: https://github.com/clytras/AccessConverter/compare/v3.2.0...v3.2.1
[3.2.0]: https://github.com/clytras/AccessConverter/compare/v3.1.0...v3.2.0
[3.1.0]: https://github.com/clytras/AccessConverter/compare/v3.0.1...v3.1.0
[3.0.1]: https://github.com/clytras/AccessConverter/compare/v2.0...v3.0.1
[2.0]: https://github.com/clytras/AccessConverter/compare/v1.1.1...v2.0
[1.1.1]: https://github.com/clytras/AccessConverter/compare/v1.1...v1.1.1
[1.1]: https://github.com/clytras/AccessConverter/compare/v1.0...v1.1
[1.0]: https://github.com/clytras/AccessConverter/releases/tag/v1.0
