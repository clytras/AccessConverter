# Changelog

All notable changes to AccessConverter are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/): a release with new features raises the minor version, one with only
fixes the patch version.

## [Unreleased]

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

[Unreleased]: https://github.com/clytras/AccessConverter/compare/v3.0.1...HEAD
[3.0.1]: https://github.com/clytras/AccessConverter/compare/v2.0...v3.0.1
[2.0]: https://github.com/clytras/AccessConverter/compare/v1.1.1...v2.0
[1.1.1]: https://github.com/clytras/AccessConverter/compare/v1.1...v1.1.1
[1.1]: https://github.com/clytras/AccessConverter/compare/v1.0...v1.1
[1.0]: https://github.com/clytras/AccessConverter/releases/tag/v1.0
