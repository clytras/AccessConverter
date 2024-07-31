# AccessConverter
A Microsoft Access database conversion tool to convert old and new Access database formats to some other popular SQL based databases and formats. It is built with [Jackess](http://jackcess.sourceforge.net/), a Java library for reading and writing MS Access databases. It supports Access 97 and all versions 2000-2013.

## Online Application
An online application that uses AccessConverter to convert databases can be found here https://lytrax.io/blog/tools/access-converter.

## Dependencies
- JRE JavaSE 17
- Apache Commons IO 2.5 ([commons-io-2.5](https://commons.apache.org/proper/commons-io/download_io.cgi))
- Apache Commons Codec 1.17.1 ([commons-coded-1.17.1](https://commons.apache.org/codec/download_codec.cgi))
- Apache Commons Lang 3.6 ([commons-lang3-3.15.0](https://commons.apache.org/proper/commons-lang/download_lang.cgi))
- Apache Commons Text 1.3 ([commons-text-1.3](https://commons.apache.org/proper/commons-text/download_text.cgi))
- Apache Commons Logging 1.2 ([commons-logging-1.2](https://commons.apache.org/proper/commons-logging/download_logging.cgi))
- Google Gson 2.11.0 ([gson-2.11.0](https://github.com/google/gson))
- SQLite JDBC Driver 3.46.0 ([sqlite-jdbc-3.46.0](https://github.com/xerial/sqlite-jdbc))
- JSR 353 (JSON Processing) 1.0.2 ([javax.json-1.0.2](https://docs.oracle.com/javaee/7/api/javax/json/package-summary.html))
- Jackcess 4.0.7 ([jackcess-4.0.7](https://jackcess.sourceforge.net/))

## Usage
It is a command line tool and it accepts arguments. The output result on the screen can be either JSON, JSON prettified or normal (human readable) output. It creates a log file for each conversion that contains the conversion result along with all input parameters and conversion errors.

| Parameter | Accepts      | Description |
| --- | ------------- | --- |
| `--output-result` | `json`<br>`json-pretty`<br>`normal` | The console output format.<br>Can be JSON, JSON prettified or normal (human readable) output |
| `--access-file` | `"<path>"` | The input access database file (*mdb*, *accdb*) |
| `--log-file` | `"<path>"` | The output log file |
| `--zip-file` | `"<path>"` | The output zip archive file that contains the converted file |
| `--output-file` | `"<path>"` | The output file with the converted data (*.json*, *.sql*, *.sqlite3*, etc.) |
| `--files-mode` | `file-relative`<br>`file-absolute`<br>`inline`<br>`reference` | The strategy to follow regarding attachment and OLE files.<br>`file-relative`/`file-absolute` will both save the files to the filesystem inside a path under a directory named after the DB + "-files". `file-absolute` will store the absolute path in the path table record, `file-relative` will store the relative path.<br>`inline` will store the data into the DB table or the output file with all the data encoded to Base64.<br>`reference` will store the file record, but it won't store any data.<br>Default behavior is `reference` |
| `--task` | `convert-json`<br>`convert-mysql-dump`<br>`convert-sqlite` | The task to perform.<br>Convert to JSON or MySQL dump or SQLite |
| `--json-data` | `assoc`<br>`array` | Either to use associative arrays or simple indexed tables for the JSON data |
| `-json-columns` | | Add extended columns information for each table |
| `-mysql-drop-tables` | | Add `DROP TABLE IF EXISTS` for each table |
| `-compress` | | Compress the output file to a zip archive file |
| `-no-log` | | Does not generate a log file |
| `-show-progress` | | Displays progress status (Current table name, records inserted and total progress percentage) |
| `-overwrite-existing-files` | | Will overwrite existing file when exporting files to filesystem for attachments and OLE objects. |


## Examples
**Convert a .accdb file to JSON**

*Output file will have the same filename as the access file and it will be saved at the same location*

    java -jar AccessConverter.jar --access-file "/home/test/somedb.accdb" --task convert-json --json-data assoc -json-columns

**Convert a .accdb file to MySQL dump file**

*Use different files and location for both output and log files. Console output JSON pretty-print result*

    java -jar AccessConverter.jar --access-file "/home/test/somedb.accdb" --task convert-mysql-dump --output-file "/home/sql/somedb_dump.sql" --log-file "/home/logs/somedb.log" -mysql-drop-tables --output-result json-pretty

## Changelog

### Update 24/5/2018 (v1.1)

- Added SQL code logging on errors
- Replaces *org.apache.commons.lang3.StrBuilder* with *org.apache.commons.text.TextStringBuilder* due to deprecation of the first

### Update 29/5/2018 (v1.1.1)

- Fixed SQLite names conversion by enclosing all names (tables/fields) using the grave accent character "`"
- Fixed when an INSERT error would break the entire transaction. Now each row is inserted individually
- Added progress status when using the flag parameter "-show-progress"

### Update 1/8/2024 (v2.0)

- JRE JavaSE 17
- Multiple primary keys (MySQL, SQLite)
- Unique and plain indexes (MySQL, SQLite)
- Relationships with foreign keys (MySQL, SQLite)
- Attachment files (MySQL, SQLite, JSON)
- OLE files (MySQL, SQLite, JSON)
- Better performance using SQLite batch updates and FileWriter for dumping data to files
- Update Jackcess to v4, SQLite to v3.46.0 and more packages

## License

Access Converter is released under the [MIT License](LICENSE).

```
Copyright (c) 2024 Christos Lytras

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
