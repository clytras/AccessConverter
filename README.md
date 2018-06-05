# AccessConverter
A Microsoft Access database conversion tool to convert old and new Access database formats to some other popular SQL based databases and formats. It is built with [Jackess](http://jackcess.sourceforge.net/), a Java library for reading and writing MS Access databases. It supports Access 97 and all versions 2000-2013.

## Online Application
An online application that uses AccessConverter to convert databases can be found here https://lytrax.io/blog/tools/access-converter.

## Dependencies
- JDK 1.8
- Apache Commons IO 2.5 ([commons-io-2.5](http://commons.apache.org/proper/commons-io/download_io.cgi))
- Apache Commons Lang 2.6 ([commons-lang-2.6](https://commons.apache.org/proper/commons-lang/download_lang.cgi))
- ~~Apache Commons Lang 3.6 ([commons-lang3-3.6](https://commons.apache.org/proper/commons-lang/download_lang.cgi))~~
- Apache Commons Text 1.3 ([commons-text-1.3](https://commons.apache.org/proper/commons-text/download_text.cgi))
- Apache Commons Logging 1.2 ([commons-logging-1.2](http://commons.apache.org/proper/commons-logging/download_logging.cgi))
- SQLite JDBC Driver 3.18.0 ([sqlite-jdbc-3.18.0](https://github.com/xerial/sqlite-jdbc))
- JSR 353 (JSON Processing) 1.0.2 ([javax.json-1.0.2](https://docs.oracle.com/javaee/7/api/javax/json/package-summary.html))
- Jackcess 2.1.8 ([jackcess-2.1.8](http://jackcess.sourceforge.net/))

## Usage
It is a command line tool and it accepts arguments. The output result on the screen can be either JSON, JSON prettified or normal (human readable) output. It creates a log file for each conversion that contains the conversion result along with all input parameters and conversion errors.

| Parameter | Accepts      | Description |
| --- | ------------- | --- |
| `--output-result` | `json`<br>`json-pretty`<br>`normal` | The console output format.<br>Can be JSON, JSON prettified or normal (human readable) output |
| `--access-file` | `"<path>"` | The input access database file (mdb, accdb) |
| `--log-file` | `"<path>"` | The output log file |
| `--zip-file` | `"<path>"` | The output zip archive file that contains the converted file |
| `--output-file` | `"<path>"` | The output file with the converted data (.json, .sql, .sqlite3, etc.) |
| `--task` | `convert-json`<br>`convert-mysql-dump`<br>`convert-sqlite` | The task to perform.<br>Convert to JSON or MySQL dump or SQLite |
| `--json-data` | `assoc`<br>`array` | Either to use associative arrays or simple indexed tables for tha JSON data |
| `-json-columns` | | Add extended columns information for each table |
| `-mysql-drop-tables` | | Add `DROP TABLE IF EXISTS` for each table |
| `-compress` | | Compress the output file to a zip archive file |
| `-no-log` | | Does not generate a log file |

## Examples
**Convert a .accdb file to JSON**

*Output file will have the same filename as the access file and it will be saved at the same location*

    java -jar AccessConverter.jar --access-file "/home/test/somedb.accdb" --task convert-json --json-data assoc -json-columns

**Convert a .accdb file to MySQL dump file**

*Use different files and location for both output and log files. Console output JSON pretty-print result*

    java -jar AccessConverter.jar --access-file "/home/test/somedb.accdb" --task convert-mysql-dump --output-file "/home/sql/somedb_dump.sql" --log-file "/home/logs/somedb.log" -mysql-drop-tables --output-result json-pretty

## Update 24/5/2018 (v1.1)

- Added SQL code logging on errors
- Replaces *org.apache.commons.lang3.StrBuilder* with *org.apache.commons.text.TextStringBuilder* due to deprecation of the first
