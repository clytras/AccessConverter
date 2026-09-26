# AccessConverter

Converts Microsoft Access databases (`.mdb`, `.mde`, `.accdb`, `.accde`, `.accdr`) to **MySQL/MariaDB dumps**,
**SQLite** and **JSON**, on Linux, Windows and macOS.

- **Exact values, or a report that says otherwise.** Every value round-trips exactly: currency and decimals keep
  their scale, dates keep their precision, bytes stay bytes, NULL stays NULL. Anything that can't be exported
  exactly is named in the conversion report, with a reason and a count.
- **The schema, as far as the target can enforce it.** Primary keys, unique and plain indexes, foreign keys with
  their cascade rules, NOT NULL, defaults, validation rules as CHECK constraints, and descriptions as comments. A
  constraint is written only when the data already satisfies it and the target enforces it no more strictly than
  Access did, so the data Access accepted always imports.
- **Every Access version Jackcess reads:** Access 97 through Microsoft 365, including password-protected and
  encrypted databases, and Access 97 files in any Windows code page (Greek, Cyrillic, Japanese, …).
- **Verifiable.** `accessconverter verify` reads the output back and compares its schema and every value with the
  Access database.

AccessConverter is built on [Jackcess](https://jackcess.sourceforge.io/). The online converter at
[lytrax.io](https://lytrax.io/blog/tools/access-converter) runs it.

Upgrading from 2.x? The command line, the outputs and the result format all changed: see
[Migrating from AccessConverter 2](docs/migrating-from-v2.md).

What changed in each release is in the [changelog](CHANGELOG.md).

## Install

**Runtime image (no Java needed).** Download the zip for your platform from the
[releases](https://github.com/clytras/AccessConverter/releases), unzip it anywhere and run
`bin/accessconverter` (`bin\accessconverter.cmd` on Windows):

| Platform | File |
| --- | --- |
| Linux x64 / arm64 | `accessconverter-<version>-linux-x64.zip`, `accessconverter-<version>-linux-aarch64.zip` |
| Windows x64 | `accessconverter-<version>-windows-x64.zip` |
| macOS Intel / Apple silicon | `accessconverter-<version>-macos-x64.zip`, `accessconverter-<version>-macos-aarch64.zip` |

Each holds its own Java runtime and never uses an installed one. On macOS, a zip downloaded with a browser is
quarantined; clear that once with `xattr -dr com.apple.quarantine accessconverter-<version>-macos-*`. To pass JVM
options (a larger heap, say), set `ACCESSCONVERTER_JAVA_OPTS=-Xmx4g`.

**Jar.** With Java 21 or later: `java -jar accessconverter-<version>.jar …`.

**Locale.** Java takes the encoding of file names and of standard output from the locale. Under a POSIX locale
(`LANG` unset or `C`, common for services and cron jobs), a file whose name isn't ASCII can't be opened and non-ASCII
text on standard output turns into `?`. Run AccessConverter with a UTF-8 locale, such as `LANG=C.UTF-8`; the
container image sets one.

**Container.** `ghcr.io/clytras/accessconverter:3` works on the current directory, mounted at `/data`:

```sh
docker run --rm -u "$(id -u):$(id -g)" -v "$PWD:/data" ghcr.io/clytras/accessconverter:3 convert Shop.accdb --to sqlite
```

To build the image yourself, from a clone with nothing but Docker installed: `docker build -t accessconverter .`, then
run it the same way with `accessconverter` in place of `ghcr.io/clytras/accessconverter:3`. The jar is compiled
inside the build, identical byte for byte to the released one for the same version.

**Checking a download.** Every release has `SHA256SUMS` (`sha256sum -c SHA256SUMS --ignore-missing`) and a
build-provenance attestation (`gh attestation verify <file> --repo clytras/AccessConverter`).

**From source.** `./mvnw -B package` builds `target/accessconverter-<version>.jar`; `./mvnw -B verify` also runs the
tests.

## Quick start

```sh
accessconverter convert Shop.accdb --to sqlite            # Shop.sqlite3
accessconverter convert Shop.accdb --to mysql             # Shop.sql, for MySQL 8.0.13 or later
accessconverter convert Shop.accdb --to mariadb           # Shop.sql, for MariaDB 10.11 or later
accessconverter convert Shop.accdb --to json              # Shop.json
accessconverter inspect Shop.accdb                        # what's in the database, and what the conversion will say
accessconverter verify  Shop.accdb Shop.sqlite3           # compare an output with its source
```

Each conversion prints a summary and writes the conversion report next to the output (`Shop.sqlite3.report.json`).

## Commands

```
accessconverter convert <input> --to sqlite|mysql|mariadb|json [-o <output>] [options]
accessconverter inspect <input> [--profile] [--format text|json] [-o <file>]
accessconverter verify  <input> [<output>] [options]
```

`-h`/`--help` on any command lists its options; `-V`/`--version` prints the version; `-v`/`--verbose` logs
Jackcess's warnings and prints stack traces on errors.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success. |
| `1` | Success with warnings: the output is complete and correct, and the report names what differs from Access (a constraint left out, a value that doesn't fit, …). |
| `2` | Failed: the database can't be read (not an Access file, a wrong or missing password, a damaged file), a table failed, or `verify` found a difference. Unless `--on-table-error continue` was given, no output is left behind. |
| `64` | Usage error: an unknown option, a missing value, an unknown charset. |

A database that can't be read prints one line on standard error, `error: <file>: <reason>`, and nothing on standard
output.

### `convert`

| Option | Meaning |
| --- | --- |
| `<input>` | The Access database. |
| `--to <target>` | `sqlite`, `mysql`, `mariadb` or `json`. Required. |
| `-o`, `--output <file>` | Default: the input's name with the target's extension (`.sqlite3`, `.sql`, `.json`) next to the input; for `--json-layout ndjson`, a directory named `<input>-ndjson`. |
| `--overwrite` | Replace an existing output instead of failing. |
| `--report <file>` | Where the conversion report goes. Default: `<output>.report.json`. |
| `--no-report` | Don't write the report file; the issues are still printed. |
| `--format-result text\|json` | How the result is printed on standard output. `json` prints the conversion report itself, for scripts. Default `text`. |
| `--progress`, `--no-progress` | Show or hide progress on standard error (see [Progress](#progress)). Default: shown when standard input and output are a terminal. |
| `--on-table-error fail\|continue` | A table that can't be read or written. `fail` (default) deletes the output; `continue` finishes the other tables, leaves the failed one empty and still exits 2. |
| `--verify` | SQLite and JSON: after writing, compare the output's schema and every value with the source (SQLite also checks its integrity). For a dump, import it and run `verify --jdbc-url`. |
| `--tables <glob>[,<glob>...]` | Only these tables (`*` and `?` allowed). Relationships to a table left out are skipped and reported. |
| `--exclude-tables <glob>[,<glob>...]` | Leave these tables out. |
| `--no-profile` | Skip the pass that reads the data before writing. Faster, but every constraint that depends on the data is left out, and SQLite stores exact decimals as text. |
| `--include-hidden` | Also write Access's own hidden columns (`s_GUID`, `s_Lineage` and the like). |
| `--add-primary-key[=<column>]` | SQLite, MySQL/MariaDB: give every table that has no primary key in Access one (see [Tables without a primary key](#tables-without-a-primary-key)). |
| `--binary inline\|files\|omit` | Where the bytes of Binary, OLE and attachment values go (see [Binary data](#binary-data-ole-objects-and-attachments)). Default `inline`. |
| `--ole-extract` | Also decode OLE objects (see [Binary data](#binary-data-ole-objects-and-attachments)). |
| `--include-version-history` | Write the version history of append-only memos, which is skipped otherwise. |
| `--password[=<password>]` | The password of a protected or encrypted database (see [Passwords](#passwords-and-encrypted-databases)). |
| `--charset <name>` | Access 97 only: decode text with this charset instead of the code page in the file's header (see [Access 97 text](#access-97-text)). |
| `--linked skip\|resolve` | Linked tables: `skip` (default) lists and reports them without their data; `resolve` reads each table linked to another Access file from that file (see [Linked tables](#linked-tables)). |
| `--linked-root <dir>` | With `--linked resolve`: the only directory the back-end files are looked for in. Default: the input's directory. |
| `--batch-rows <n>` | SQLite, MySQL/MariaDB: rows per insert batch. Default 1000. |
| `--batch-bytes <n>` | MySQL/MariaDB: the largest `INSERT` statement in bytes; a single larger row gets its own. Default 1048576. |
| `--drop-existing` | MySQL/MariaDB: `DROP TABLE IF EXISTS` before each table, so the dump can be imported again. |
| `--database <name>` | MySQL/MariaDB: create this database if it doesn't exist, and use it. |
| `--collation <name>` | MySQL/MariaDB: the collation of every table (see [MySQL and MariaDB](#mysql-and-mariadb)). |
| `--stamp` | MySQL/MariaDB and JSON: put the export time in the output. Without it, the same database always gives byte-identical output. |
| `--sqlite-strict` | SQLite: `STRICT` tables (readable by SQLite 3.37 or later). |
| `--sqlite-nocase` | SQLite: `COLLATE NOCASE` on every text column, so comparisons ignore (ASCII) case as Access does. |
| `--sqlite-metadata` | SQLite: add the tables `_access_columns`, `_access_relationships` and `_access_export` with the full Access metadata. |
| `--analyze` | SQLite: run `ANALYZE` on the finished file, not only `PRAGMA optimize`. |
| `--json-layout document\|ndjson` | JSON: one file (default), or a directory with `schema.json` and one `<table>.ndjson` file per table, one row per line. |
| `--json-rows object\|array` | JSON: a row as an object keyed by column name (default) or an array in column order (smaller). |
| `--json-bigint number\|string` | JSON: Large Number values as exact numbers (default) or strings, for parsers that lose precision past 2^53, such as JavaScript's. |
| `--json-decimals number\|string` | JSON: Currency and Decimal values as exact numbers (default) or strings, for parsers that read every number as a double. |
| `--json-hyperlinks string\|object` | JSON: a hyperlink as Access stores it, `display#address#subaddress#screentip` (default), or as an object. |
| `--no-schema` | JSON: leave out the schema section (not recommended). |

### `inspect`

Prints what AccessConverter reads from a database: tables, columns with their types and properties, keys, indexes
(with the hidden ones Access adds folded in), relationships, and the issues a conversion would report. It writes
nothing else.

| Option | Meaning |
| --- | --- |
| `--profile` | Also read the data and print the statistics the conversion decides with (NULLs, lengths, rule violations, orphans). |
| `--format text\|json` | Default `text`. |
| `-o`, `--output <file>` | Write to this file (UTF-8) instead of standard output. |
| `--password[=<password>]`, `--charset <name>`, `--linked skip\|resolve`, `--linked-root <dir>` | As for `convert`. |

### `verify`

```sh
accessconverter verify Shop.accdb Shop.sqlite3
accessconverter verify Shop.accdb Shop.json
accessconverter verify Shop.accdb --jdbc-url jdbc:mariadb://localhost:3306/shop --jdbc-driver mariadb-java-client-3.5.10.jar --db-user root
```

`verify` plans the conversion again from the source and compares the output with it: every table, column type, key,
index, foreign key, default and CHECK, and every value (exact decimals, dates at their precision, bytes by
SHA-256). **Give it the same options the conversion used**, since they decide what the output should hold
(`--tables`, `--exclude-tables`, `--no-profile`, `--include-hidden`, `--add-primary-key`, `--sqlite-strict`, `--sqlite-nocase`,
`--sqlite-metadata`, `--collation`, `--binary`, `--ole-extract`, `--include-version-history`, `--password`,
`--charset`, `--linked`, `--linked-root`). It exits 0 when the output matches and 2 when it doesn't, listing each
difference. `--progress` and `--no-progress` work as for `convert`.

A MySQL or MariaDB output is the database the dump was imported into:

| Option | Meaning |
| --- | --- |
| `--jdbc-url <url>` | `jdbc:mariadb://host:3306/db` with MariaDB Connector/J (works with both servers; add `?allowPublicKeyRetrieval=true` for MySQL 8), or `jdbc:mysql://host:3306/db` with MySQL Connector/J. |
| `--jdbc-driver <jar>` | The driver's jar. None is bundled; not needed when the driver is already on the class path. |
| `--db-user <user>` | The database user (or put it in the URL). |
| `--db-password <password>` | The user's password; `ACCESSCONVERTER_DB_PASSWORD` keeps it out of the process list. |
| `--to mysql\|mariadb` | The dialect the dump was written for. Default: the server's own. |
| `--dump-directory <dir>` | With `--binary files`: the directory the dump was written in, which the stored paths are relative to. Default: the current directory. |

Without an output, `verify <input>` prints the source side: each table's row count and a digest of its rows.

### Progress

`convert` and `verify` show their progress on standard error, on one line that is redrawn in place and erased when
they finish:

```
write 3/21 Orders  45,000 / 120,000 rows  37%
```

It names the stage (`profile`, the pass that reads the data before writing; `write`; `verify`), the table and how
many of its rows have been read. Nothing is shown in the first half second, and the line changes at most five times
a second, only when the stage, the table or the percentage changes. Standard output, the output file and the report
are the same with or without it.

It is on by default when standard input and output are both a terminal, which is all Java can tell: it can't tell
whether standard error is one. When standard error goes to a file or a log (`2> convert.log`) while the command runs
at a terminal, pass `--no-progress` so the progress line isn't written there. `--progress` shows it even when standard
output isn't a terminal, for instance with `--format-result json | jq`.

## The conversion report

Every conversion writes a JSON report (`<output>.report.json`, or `--report <file>`); `--format-result json` prints
the same report on standard output.

```json
{
  "format": "accessconverter-report",
  "formatVersion": 1,
  "tool": "accessconverter 3.2.0",
  "command": "convert",
  "options": { "to": "mysql", "binary": "inline", "collation": "utf8mb4_0900_as_ci", "…": "…" },
  "source": { "file": "Shop.mdb", "fileFormat": "V1997", "codePage": 1253, "charset": "windows-1253" },
  "outcome": "success-with-warnings",
  "tables": [ { "table": "Customers", "rowsRead": 8, "rowsWritten": 8 } ],
  "issues": [
    { "code": "CHECK_VIOLATED_BY_DATA", "severity": "warning", "table": "Orders", "object": "Quantity",
      "message": "no CHECK for the validation rule >0: 2 existing rows violate it, as Access allows",
      "count": 1, "samples": [ "(17); (41)" ] }
  ],
  "timings": { "extractMillis": 48, "planMillis": 30, "profileMillis": 47, "writeMillis": 19 }
}
```

- `outcome` is `success`, `success-with-warnings` or `failed`, matching exit codes 0, 1 and 2.
- `tables` has the rows read from Access and written to the output for each table; they differ only for a table
  that failed.
- Each issue has a `code` (stable, for scripts), a `severity`, the `table` and `object` (a column, index or
  relationship) it concerns, a `message` for people, a `count`, and `samples`: the primary keys of a few affected
  rows (`(17); (41)`, or `row 5` in a table without one), so you can find them in Access.
- `info` issues are decisions made the way Access would want them (a hidden index folded into the primary key, a
  name made legal for the target). `warning` issues are where the output differs from Access: read these. `error`
  issues mean the conversion failed.

The warnings you are most likely to meet:

| Code | What it means | What to do |
| --- | --- | --- |
| `CHECK_VIOLATED_BY_DATA` | A validation rule became no CHECK, because existing rows break it (Access doesn't recheck old rows when a rule is added). | Fix the rows named in `samples` if the rule matters. |
| `CHECK_UNTRANSLATABLE` | A validation rule has no equivalent in the target; it is kept as a comment. | Enforce it in your application. |
| `NOT_NULL_DROPPED_NULLS_PRESENT` | A Required column holds NULLs, so it isn't NOT NULL. | As above. |
| `FK_SKIPPED_ORPHANS` | An enforced relationship became no foreign key, because child rows have no parent. | Fix or delete the orphans named in `samples`. |
| `FK_SKIPPED_NOT_ENFORCED` (info) | Access doesn't enforce this relationship, so neither does the output; the child columns get an index. | — |
| `DEFAULT_UNTRANSLATABLE` | An Access default expression has no equivalent; it is kept as a comment. | Set it in your application. |
| `AUTONUMBER_RANDOM_DEFAULT`, `AUTONUMBER_RANDOM_SEQUENTIAL` | A Random AutoNumber (see below). | Read the message. |
| `COLLATION_BINARY_KEY` | MySQL/MariaDB: a key column compares with `utf8mb4_bin`, because the default collation would reject values Access holds apart. | — |
| `DECIMAL_STORED_AS_TEXT` (info) | SQLite: a decimal with more than 13 digits is stored as text to stay exact; `CAST(col AS REAL)` makes it numeric. | — |
| `STATEMENT_EXCEEDS_PACKET` | MySQL/MariaDB: a row is bigger than the server's default `max_allowed_packet`. | Raise it, or convert with `--binary files`. |
| `LINKED_TABLE_SKIPPED` | A linked table: its data lives in another database and isn't converted. | Pass `--linked resolve` for a table linked to another Access file, or convert that database too. |
| `LINKED_TABLE_RESOLVED` (info) | `--linked resolve` read a linked table from its back-end; the message names the file. | — |
| `LINKED_CONNECTION_PASSWORD` | JSON: a linked table's ODBC connection string, kept as Access stores it, holds a password. | Remove it before sharing the file, or export with `--no-schema`. |
| `CATALOG_INDEX_UNUSABLE` | The database's catalog index couldn't be used (common in non-English Access 97 files); the catalog was read by scanning instead. | Nothing: every table and relationship is still found. |

## MySQL and MariaDB

`--to mysql` writes a dump for **MySQL 8.0.13 or later** (tested on 8.0 and 8.4), `--to mariadb` one for **MariaDB
10.11 or later** (tested on 10.11, 11.4 and 11.8). Import it with the server's own client:

```sh
mysql   -u root -p shop < Shop.sql      # MySQL
mariadb -u root -p shop < Shop.sql      # MariaDB
```

The database must exist, or convert with `--database shop` to have the dump create and use it. The dump sets
everything it needs for its own session and restores it at the end: `utf8mb4`, strict SQL mode (so nothing is ever
silently truncated or rounded), and foreign-key checks. Each table is loaded in one transaction, then the secondary
indexes, CHECKs and foreign keys are added, so the server validates every constraint against the data. An import
that prints nothing succeeded.

- **Collation.** Tables use `utf8mb4_0900_as_ci` (MySQL) or `utf8mb4_uca1400_as_ci` (MariaDB): case-insensitive and
  accent-sensitive like Access. Where a primary-key or unique column holds text these collations would compare more
  strictly than Access (some control characters, compatibility forms), that column and its foreign keys use
  `utf8mb4_bin` instead and the report says so. `--collation utf8mb4_bin` is also safe, but compares case.
- **Large values.** A value of more than 16 MiB needs a larger `max_allowed_packet` on the server and the client
  (`mysql --max-allowed-packet=1G`); `--binary files` keeps the bytes out of the dump.
- **Yes/No** is `BOOLEAN`, 1 for Yes (Access stores -1).
- **Random AutoNumbers.** An AutoNumber whose New Values are Random gets a random `DEFAULT`, as in Access: new rows
  get random 32-bit keys, never a counter that could run out. The server doesn't report that key back:
  `LAST_INSERT_ID()` and the client calls built on it (PHP's `lastInsertId()`, …) return 0, so an application that
  needs the new key must select the row. As in Access, a drawn value that is already taken fails the insert with a
  duplicate-key error; the report says how often that happens at the table's size.
- **`Time()` defaults** are the time of day on Access's day zero, `1899-12-30`, because that is how Access stores a
  time-only value; an application that expects a bare time reads that date. The same holds in SQLite.

Check an import with `verify --jdbc-url` (see [`verify`](#verify)).

## SQLite

The output is a finished SQLite database. Every conversion runs `foreign_key_check` on it; `--verify` also runs
`integrity_check` and compares every value with the source.

- **Foreign keys are only enforced when the connection asks for it.** SQLite ignores foreign keys unless each
  connection runs `PRAGMA foreign_keys = ON;` first. The file has them; your application has to switch them on.
- **Dates** are ISO-8601 text (`2024-01-02 03:04:05`, plus `.fff` when there are milliseconds), which sorts and
  compares correctly and works with SQLite's date functions.
- **Currency and Decimal** are exact: `NUMERIC` when every value has at most 13 digits, otherwise text (reported as
  `DECIMAL_STORED_AS_TEXT`; `CAST(col AS REAL)` makes it numeric).
- **Text comparison.** Access compares text ignoring case; SQLite's default doesn't. CHECKs and foreign keys that
  depend on it compare with `NOCASE` where they must; `--sqlite-nocase` makes every text column `COLLATE NOCASE`.
  SQLite's `NOCASE` only folds ASCII letters, so a validation rule whose data only complies when Greek, Cyrillic or
  other letters are compared ignoring case becomes no CHECK, and the report says so.
- **AutoNumbers** are `INTEGER PRIMARY KEY AUTOINCREMENT` when they are the primary key. A Random AutoNumber counts
  upward here, from its largest value: SQLite can only generate a key for such a column in sequence, and its 64-bit
  counter never runs out (`AUTONUMBER_RANDOM_SEQUENTIAL`).
- `--sqlite-metadata` adds `_access_columns` (each column's Access type, length, precision, Required, descriptions,
  format, default and validation rule as Access wrote them, and what it became), `_access_relationships` (every
  relationship, enforced or not, and whether it became a foreign key) and `_access_export` (the AccessConverter
  version and the source file, format and code page).
- **Every file says who wrote it** without an extra table: `PRAGMA application_id` is `0x41434356` ("ACCV") and
  `PRAGMA user_version` is the layout version, `1`. A MySQL/MariaDB dump names the producer in its first comment
  line, and JSON in its `producer` property. No output records a time unless `--stamp` is given.

## JSON

JSON holds the whole database: the schema (tables, columns with every Access property, keys, indexes and all
relationships, including those Access doesn't enforce) and every value, spelled exactly. The format is documented
in [docs/json-format.md](docs/json-format.md) and published as a JSON Schema,
[`accessconverter-json-v1.schema.json`](src/main/resources/io/lytrax/accessconverter/target/json/accessconverter-json-v1.schema.json).
The output streams, so a database of any size converts in little memory, and `--json-layout ndjson` gives one file
per table for tools that read a row per line.

## Tables without a primary key

Access allows tables without a primary key, and by default the output has none for them either: AccessConverter
never invents a key. Applications and frameworks that need one on every table can ask for it:

```sh
accessconverter convert Shop.accdb --to sqlite --add-primary-key            # a column named id
accessconverter convert Shop.accdb --to mysql  --add-primary-key=row_id     # or any name
```

Each table without a primary key gets an AutoNumber column, first in the table, that numbers the rows 1, 2, 3, …
in the order Access stores them: `INTEGER PRIMARY KEY AUTOINCREMENT` in SQLite, `INT AUTO_INCREMENT PRIMARY KEY` in
MySQL/MariaDB, so new rows get the next number. When the table already has a column of that name, the new one is
named `id_2` (and so on). MySQL allows only one `AUTO_INCREMENT` column per table: an AutoNumber the table already
has keeps its values but stops generating them. The report lists every table that got a key (`PRIMARY_KEY_ADDED`).
Tables with a primary key are left as they are. JSON doesn't need keys, so the option applies to the SQL targets
only. To verify such an output, pass the same option to `verify`.

## Binary data, OLE objects and attachments

**Binary and OLE Object values are exported as their exact stored bytes** by default, whatever they hold. That can't
fail and loses nothing. `--binary` decides where the bytes go:

| `--binary` | The value in the output |
| --- | --- |
| `inline` (default) | The bytes: `BLOB`/`LONGBLOB` in SQL, base64 in JSON. |
| `files` | One file per value under `<output>-files/<table>/<column>/`, named after the row's key; the output holds its path, relative to the output's directory. Recommended for large pictures and documents, and the only way to keep a MySQL dump small. |
| `omit` | The bytes are dropped and the output holds their size, so a missing value and a 4 MB picture stay apart. |

`--ole-extract` also decodes each OLE object: its kind (`package`, `embedded`, `link`, `compound` or `raw`), its name
(the file name of a packaged file, the program of an embedded object, the path of a link), its content type (from
the bytes, never from a name) and its content (the packaged file, or the object's own bytes). In SQL they are four
extra columns, `<column>__kind`, `__name`, `__mime` and `__content`; in JSON the value becomes an object. The raw
bytes are always kept.

**Attachments and multi-value columns** become child tables in SQLite and MySQL/MariaDB, `<Table>_<Column>`,
linked to the parent column by Access's own id with a cascading foreign key: attachments with `file_name`,
`file_type`, `file_data`, `file_size`, `file_url`, `file_timestamp` and `file_flags`, multi-value columns with
`value`. In JSON they are arrays inside the row. **Version history** of append-only memos is only written with
`--include-version-history`.

## Passwords and encrypted databases

AccessConverter opens databases encrypted by Access 2007 and later (RC4 CryptoAPI and Agile encryption) and Microsoft
Money files, which need their password. Encoded Access 97–2003 files, and the "database password" of Access 97–2003,
need none: that password only guards the file inside Access, doesn't encrypt it, and is reported as
`PASSWORD_NOT_REQUIRED`. The password is given one of three ways:

- `--password=<password>` on the command line;
- `--password` alone: it is asked for without echo. Put it after the input, or the input is taken as the password;
  without a console, the prompt goes to standard error and the password is read from the first line of standard
  input;
- the `ACCESSCONVERTER_PASSWORD` environment variable, which keeps the password out of the process list.

The password is never printed or written to the report. A missing or wrong password for an encrypted database
exits 2 with `error: <file>: …`.

## Access 97 text

Access 97 stores text in the Windows code page of the database's sort order, not in Unicode. AccessConverter reads
the code page from the file's header, so Greek, Cyrillic, Central European, Japanese, Chinese, Korean, Thai and other
Access 97 files convert without any option; the report's `source.charset` says which charset was used. If a file was
made on a system whose code page doesn't match its header, `--charset windows-1253` (or any Java charset name)
overrides it. Access 2000 and later store Unicode, and ignore `--charset` with a warning.

Every byte is decoded as Windows, and so Access, decodes it. A few code pages (Greek 1253, Hebrew 1255, Baltic 1257,
Thai 874) leave some bytes undefined. Windows turns those into private-use characters (U+E000 to U+F8FF), which map
back to the same byte but show as blank in most fonts, so AccessConverter writes the same characters and reports
each column that holds one (`TEXT_PRIVATE_USE`). A byte no charset can decode is written as U+FFFD and reported as
`TEXT_UNDECODABLE`.

## Linked tables

A linked table's data lives in another database, the back-end. By default it is left out: the report lists it
(`LINKED_TABLE_SKIPPED`), JSON's schema lists it under `linkedTables`, and relationships to it are skipped.

With `--linked resolve`, each table linked to another Access file is read from that file and converted like any
other table, under the name the linking database gives it:

```sh
accessconverter convert Front.accdb --to sqlite --linked resolve
accessconverter convert Front.accdb --to sqlite --linked resolve --linked-root /data/backends
```

- **Where the back-end is found.** Access stores the back-end's path as it was on the machine that linked it
  (`Z:\Shared\Back.accdb`). Only its file name is used, looked up in `--linked-root` (the input's directory by
  default): the exact name first, then a single match ignoring case. The stored path itself is never opened; only
  files directly in the root are considered.
- **How it is read.** Like the input: an Access 97 back-end with its own code page (and `--charset`, which applies
  to every Access 97 file of the conversion), an encrypted one with the password the link stores, else
  `--password`. What opening it reports is reported under its file name. Each back-end is opened once.
- **Relationships.** Access can't enforce a relationship between a local and a linked table, so those stay join
  lines (an index and metadata). The back-end's own relationships between tables that are all read from it come
  along, under their names here, and an enforced one becomes a foreign key. One that reaches a back-end table that
  isn't linked is left out and reported; a relationship name the front-end already uses gets a suffix.
- **What stays skipped.** ODBC links (their data lives on a server), and a table that is itself a link in its
  back-end: links of links aren't followed.
- **Errors.** A back-end that can't be found or read, or that lacks the linked table, is a table error: the
  conversion fails, or with `--on-table-error continue` leaves that table out and still exits 2.
- The report names the file each table was read from (`LINKED_TABLE_RESOLVED`), and JSON gives each such table a
  `linkedFrom` with the path and table name Access stored. Pass the same `--linked` options to `verify`.

## What isn't converted

- **Queries, forms, reports, macros and VBA modules.** Jackcess doesn't read them, and they have no equivalent in
  the targets.
- **Linked tables, without `--linked resolve`**, and ODBC linked tables always: their data lives in another database
  or on a server; they are listed in the report (and in JSON's schema). See [Linked tables](#linked-tables).
- **Access security** (user-level permissions and workgroup files).

## License

AccessConverter is released under the [Apache License, Version 2.0](LICENSE), from 3.0.1 on; versions 2.x and earlier
were released under the MIT License. The runtime images and the container image bundle third-party software under
its own licenses: see [NOTICE](NOTICE).
