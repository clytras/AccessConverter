# Migrating from AccessConverter 2 to 3

AccessConverter 3 is a rewrite. It has a new command line, new output formats and a new result format, and none of
the 2.x options are accepted. This page maps every 2.x option to its 3.0 form. It then lists what changes in each
output and in how a script or a service should call the tool. The 2.x code is kept as the tag
[`v2.0`](https://github.com/clytras/AccessConverter/tree/v2.0).

Why the break: 2.x silently corrupted data in all three formats (bytes over 127 turned negative, currency was
clamped, backslashes were reinterpreted, SQLite dates depended on the time zone, NULL numbers became 0), dropped
tables and rows, and crashed on some OLE and NULL values. 3.0 exports every value exactly or names the difference
in the conversion report.

## Running it

| 2.x | 3.0 |
| --- | --- |
| `java -jar AccessConverter.jar …` with Java 17 | A runtime image with its own Java (`bin/accessconverter …`), the container `ghcr.io/clytras/accessconverter:3`, or `java -jar accessconverter-<version>.jar …` with **Java 21 or later**. The 3.0 jar doesn't start on Java 17. |
| One command, chosen with `--task` | Three commands: `convert`, `inspect` and `verify`. |
| Options are `--name value`, flags `-name` | Standard options: `--name value` or `--name=value`, and flags as `--name`. A 2.x flag such as `-compress` is now an error. |

## Every 2.x option

| 2.x | 3.0 | Notes |
| --- | --- | --- |
| `--access-file "<path>"` | `<input>`, the first argument after the command | `accessconverter convert Shop.accdb --to sqlite` |
| `--task convert-json` | `convert --to json` | The JSON is a new format: see [the JSON format](json-format.md). |
| `--task convert-mysql-dump` | `convert --to mysql` (MySQL 8.0.13 or later) or `convert --to mariadb` (MariaDB 10.11 or later) | The dump is written for one server; it no longer loads into MySQL 5.x or older MariaDB versions. |
| `--task convert-sqlite` | `convert --to sqlite` | |
| `--output-file "<path>"` | `-o <path>`, `--output <path>` | **A relative path is now relative to the current directory**; 2.x resolved it against the input's directory. The default is still the input's name with `.json`, `.sql` or `.sqlite3`, next to the input. |
| *(an existing output was deleted)* | `--overwrite` | **An existing output is now an error** unless `--overwrite` is given. |
| `--output-result normal` | `--format-result text` (the default) | |
| `--output-result json`, `--output-result json-pretty` | `--format-result json` | Prints the conversion report, a different document from 2.x's result; see [Scripts and services](#scripts-and-services). There is no compact variant. |
| `--log-file "<path>"` | `--report <path>` | The report replaces the log. Default `<output>.report.json` (2.x: `<input name>.log.json`). |
| `-no-log` | `--no-report` | |
| `-compress`, `--zip-file "<path>"` | — | Removed. Zip the output yourself, together with its `-files` directory when there is one. |
| `--files-mode reference` (the 2.x default) | `--binary omit` | 2.x wrote name and size metadata and **no bytes**; 3.0 writes each value's size. The 3.0 default, `inline`, writes every byte. |
| `--files-mode inline` | `--binary inline` (the default) | The bytes are in the output: `BLOB`/`LONGBLOB` in SQL (2.x: base64 text), base64 in JSON. |
| `--files-mode file-relative` | `--binary files` | One file per value under `<output>-files/`; the output holds its path relative to the output's directory. |
| `--files-mode file-absolute` | `--binary files` | Paths are always relative to the output's directory; join them with it. |
| `-overwrite-existing-files` | `--overwrite` | Replaces the output and its `-files` directory. |
| `--json-data assoc` | `--json-rows object` (the default) | A row is `{"column": value}`. |
| `--json-data array` | `--json-rows array` | A row is `[value, …]` in the order of the schema's columns. |
| `-json-columns` | *(always)* | The schema (tables, columns with every Access property, keys, indexes, relationships) is always written; `--no-schema` leaves it out. |
| `-mysql-drop-tables` | `--drop-existing` | |
| `-show-progress` | `--progress` | Progress on standard error, one line erased at the end; on by default at a terminal, `--no-progress` turns it off. A summary is printed at the end either way. |
| `-debug` | `-v`, `--verbose` | Logs Jackcess's warnings and prints stack traces on errors. |

New in 3.0: `inspect`, `verify`, `--verify`, `--password`, `--charset`, `--tables`, `--exclude-tables`,
`--on-table-error`, `--ole-extract`, `--include-version-history`, `--include-hidden`, `--no-profile`, `--database`,
`--collation`, `--stamp`, `--batch-rows`, `--batch-bytes`, the `--sqlite-*` and `--json-*` options, and `--analyze`.
The [README](../README.md#commands) lists them all.

## What changes in the outputs

**All targets.**
- Values are exact: Byte is 0–255, Currency and Decimal keep every digit and their scale, Single and Double are
  written so they read back as the same number, text is never reinterpreted or cut, and NULL stays NULL.
- Tables with a GUID AutoNumber, attachments or multi-value columns are no longer dropped.
- Attachments and multi-value columns hold their values: child tables `<Table>_<Column>` in SQL, arrays in JSON
  (2.x: metadata in a text column, or nothing).
- OLE objects are their exact stored bytes, whatever they hold; `--ole-extract` also decodes them.
- Hidden indexes Access keeps for relationships are no longer exported as duplicate indexes, and a table without a
  primary key in Access gets none.
- Relationships Access doesn't enforce become an index, not a foreign key; enforced ones become foreign keys with
  their cascade rules, unless the data has orphans (reported).
- Linked tables are skipped and reported (2.x's MySQL dump stopped at the first one); convert a linked back-end
  file directly.
- The same database and options always give byte-identical output.

**MySQL.**
- The dump runs in strict SQL mode and restores the session's settings at the end: nothing is truncated or rounded
  on import, so an import that fails is a bug to report, not data to lose.
- Tables are `utf8mb4` with `utf8mb4_0900_as_ci` (MySQL) or `utf8mb4_uca1400_as_ci` (MariaDB), case-insensitive and
  accent-sensitive like Access.
- Yes/No is `BOOLEAN` (1/0), dates are `DATETIME` (with fractional seconds where the data has them), Currency is
  `DECIMAL(19,4)`.
- Defaults, validation rules (as CHECK constraints) and descriptions (as comments) are carried where the server can
  enforce them and the data complies.
- A Random AutoNumber gets a random `DEFAULT` instead of `AUTO_INCREMENT`; `LAST_INSERT_ID()` doesn't return its
  keys.

**SQLite.**
- **Dates are ISO-8601 text** (`2024-01-02 03:04:05`); 2.x wrote epoch integers that depended on the converting
  machine's time zone.
- Primary keys are real keys; a single integer key is the rowid (`INTEGER PRIMARY KEY`).
- Foreign keys are declared; SQLite only enforces them on connections that run `PRAGMA foreign_keys = ON`.
- Currency and Decimal are exact: `NUMERIC` up to 13 digits, text beyond.

**JSON.** A new, documented format: [json-format.md](json-format.md), with a published JSON Schema.
- The top level holds `format`, `formatVersion`, `producer`, `layout`, `encoding`, `source`, `schema` and `data`,
  where `data` maps each table name to its rows.
- Values are typed: Large Number and decimals are exact numbers (or strings with `--json-bigint string` and
  `--json-decimals string`), dates are `YYYY-MM-DDTHH:MM:SS[.fff]` with no time zone, and bytes are base64.
- Singles are no longer rounded to 2 decimals.
- `--json-layout ndjson` writes one file per table.

## Scripts and services

This section is for anything that runs AccessConverter and reads its result: scripts, scheduled jobs and web
services.

**Exit codes.** 2.x always exited 0 and reported failure only in its output. 3.0:

| Code | Meaning | Treat as |
| --- | --- | --- |
| `0` | success | success |
| `1` | success with warnings: the output is complete; the report names what differs from Access | **success** |
| `2` | failed: unreadable database, wrong or missing password, a failed table, or a `verify` difference | failure |
| `64` | usage error | a bug in the calling code |

**The result on standard output.** 2.x printed `{"result": "success" | "fail", "outputFile", "logFile", "zipFile"}`.
`--format-result json` prints the conversion report instead:

- `outcome` (`success`, `success-with-warnings` or `failed`) replaces `result`.
- `options.output` is the output path as given; there is no absolute path and no log or zip path. Pass `-o` and
  use that path.
- `tables` has each table's rows read and written; `issues` the report's entries (`code`, `severity`, `table`,
  `object`, `message`, `count`, `samples`). See [the README](../README.md#the-conversion-report).
- **When the database can't be read, nothing is printed on standard output.** Exit code 2 comes with one line on
  standard error, `error: <file>: <reason>` (not an Access database, password required, wrong password, damaged, …).
  A usage error (exit 64) also prints its message and the usage on standard error only.

**Output files.**
- Pass `--overwrite` if a previous output may exist (2.x deleted it silently).
- Pass an absolute `-o`, or run in the input's directory: a relative `-o` no longer follows the input.
- There is no `-compress`: zip the output, the report if you want it, and with `--binary files` the `<output>-files/`
  directory.

**Binary data.** 2.x's default (`reference`) exported no bytes, so its outputs were small. 3.0's default exports
every byte of every picture, document and attachment inline. For uploaded databases with large OLE or attachment
data, choose deliberately:
- `--binary files` with a zip of the output and its `-files` directory keeps everything and keeps a MySQL dump
  importable. A value over 16 MiB inline needs a larger `max_allowed_packet` on the server, and is reported as
  `STATEMENT_EXCEEDS_PACKET`.
- `--binary omit` keeps outputs small, as 2.x's default did, and records each value's size.

**Passwords.** 3.0 opens databases encrypted by Access 2007 and later, which need their password; an Access 97–2003
database password isn't needed at all. A service should pass the password in the `ACCESSCONVERTER_PASSWORD`
environment variable, never as `--password=<password>`, which other users of the machine can read in the process
list. An encrypted database without its password fails with exit 2 and `error: <file>: …`; ask the user for the
password and run again.

**Locale.** Run the tool with a UTF-8 locale (`LANG=C.UTF-8`). Web servers and process managers often start
programs with none. Java then can't open a file whose name isn't ASCII (exit 64), and the report on standard output
loses every non-ASCII character. The container image sets a UTF-8 locale itself.

**Target versions.** A MySQL dump needs MySQL 8.0.13 or later; for MariaDB, convert with `--to mariadb` (MariaDB
10.11 or later). Users on MySQL 5.7 or MariaDB before 10.11 can no longer import a dump. SQLite files open in any
current SQLite; `--sqlite-strict` needs 3.37 or later.

**Memory and speed.** 3.0 streams: memory doesn't grow with the database, and a million-row table converts within a
256 MB heap (`ACCESSCONVERTER_JAVA_OPTS=-Xmx256m` for the runtime image). A conversion first reads the data once to
decide the constraints; `--no-profile` skips that for speed, at the cost of leaving out every constraint that depends
on the data.
