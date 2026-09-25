# AccessConverter JSON format, version 1

`accessconverter convert <database> --to json` exports a Microsoft Access database as JSON: the whole schema,
including relationships Access doesn't enforce, and every value exactly. This document is the contract for
readers of that output. The machine-readable version is the JSON Schema
[`accessconverter-json-v1.schema.json`](../src/main/resources/io/lytrax/accessconverter/target/json/accessconverter-json-v1.schema.json),
which ships inside the AccessConverter jar.

`formatVersion` is `1`. It changes whenever the format changes in a way a reader could trip over; new optional
properties don't count.

## Layouts

| `--json-layout` | Output |
| --- | --- |
| `document` (default) | One file, `<database>.json`. |
| `ndjson` | A directory, `<database>-ndjson/`, holding `schema.json` and one `<table>.ndjson` file per table with one row per line. |

Both are UTF-8 with LF line ends. The same database and options always give byte-identical output, unless
`--stamp` adds the export time.

## The document

```json
{
  "format": "accessconverter",
  "formatVersion": 1,
  "producer": "AccessConverter 3.1.0",
  "layout": "document",
  "encoding": { "rows": "object", "bigint": "number", "decimals": "number", "hyperlinks": "string",
                "binary": "base64", "ole": "raw" },
  "source": { "file": "Shop.accdb", "fileFormat": "V2010", "charset": "UTF-16LE" },
  "schema": { "tables": [ … ], "linkedTables": [ … ], "relationships": [ … ] },
  "data": {
    "Customers": [
      {"CustomerID":-5,"Code":"C1","Discount":12.3456,"Created":"2024-01-02T03:04:05"}
    ]
  }
}
```

The properties come in this order, and `data` is always last, so a streaming reader has the schema before the
first row.

| Property | Meaning |
| --- | --- |
| `format` | Always `"accessconverter"`. |
| `formatVersion` | `1`. |
| `producer` | The tool and its version. |
| `layout` | `"document"` or `"ndjson"`. |
| `encoding` | How values are spelled in this file (see Values): `rows`, `bigint`, `decimals`, `hyperlinks`, `binary` (`base64`, `files` or `omit`) and `ole` (`raw` or `extracted`). A reader never needs to know which options were used. |
| `source` | `file`; `fileFormat` (such as `V1997` or `V2010`); `codePage` (Access 97 files only); `charset` (the charset text was decoded with); `exported` (UTC, only with `--stamp`). |
| `schema` | The database's structure. Left out with `--no-schema`. |
| `data` | Each table's rows, keyed by table name, in the order of `schema.tables`. |

In the ndjson layout, `schema.json` has the same properties except `data`, and adds `files`: an object mapping each
table name to its file name in the same directory. File names are table names made safe for every operating
system, so always look them up in `files` rather than deriving them.

## Schema

### Tables

```json
{
  "name": "Customers",
  "description": null,
  "columns": [ … ],
  "primaryKey": { "name": "PrimaryKey", "columns": [{ "name": "CustomerID", "order": "asc" }],
                  "unique": true, "ignoreNulls": false, "required": true, "accessNames": ["PrimaryKey"] },
  "indexes": [ … ],
  "validationRule": null
}
```

- `primaryKey` has the same shape as an index, or is `null` when the table has none. Nothing is ever invented:
  a table without a primary key in Access has none here.
- `indexes` are Access's indexes, cleaned up: the hidden indexes Access keeps for relationships are folded in or
  dropped, and duplicates merged; `accessNames` lists every Access index merged into one, its own name first. Each
  column has an `order`, `asc` or `desc`. `ignoreNulls` means Access leaves rows whose key columns are all NULL out
  of the index; `required` means the columns can't be NULL.
- `validationRule` is the table-level rule, `{ "access": "[EndDate]>=[StartDate]", "validationText": … }`, or `null`.
  Access doesn't recheck old rows when a rule is added, so the data may not satisfy it.
- `linkedTables` lists linked tables (`name`, `database`, `remoteTable`, `odbc`). Their data lives in another
  database and is not exported. `database` is exactly what Access stores: a file path, or an ODBC connection string,
  which may hold a password (`PWD=`). When it does, the conversion report raises `LINKED_CONNECTION_PASSWORD` for that
  table; `--no-schema` leaves the section out.

### Columns

```json
{ "name": "CustomerID", "type": "int32", "accessType": "LONG", "nullable": false, "required": false, "autoNumber": "increment" }
```

| Property | Meaning |
| --- | --- |
| `name` | The Access column name, exactly. |
| `type` | How the values are spelled: see Values. |
| `accessType` | The Access storage type (`BOOLEAN`, `BYTE`, `INT`, `LONG`, `BIG_INT`, `MONEY`, `FLOAT`, `DOUBLE`, `NUMERIC`, `SHORT_DATE_TIME`, `EXT_DATE_TIME`, `TEXT`, `MEMO`, `HYPERLINK`, `GUID`, `BINARY`, `OLE`, `ATTACHMENT`, `MULTI_VALUE`, `VERSION_HISTORY`, `COMPLEX_UNSUPPORTED`, `UNSUPPORTED`). |
| `nullable` | `false` only when every value in this file is non-null: Access guarantees it (Yes/No, AutoNumber, primary key columns), or requires it and no row holds NULL. |
| `required` | Access's Required property, as Access has it. |
| `autoNumber` | `increment` or `random` for an AutoNumber stored as a Long Integer (Access's New Values setting), `guid` for a Replication ID AutoNumber. |
| `length` | Characters for text, bytes for binary. |
| `precision`, `scale` | Decimal; Currency is always 19, 4. |
| `allowZeroLength` | Text: whether Access accepts `""`. |
| `default` | Access's default value (below). |
| `validationRule` | `{ "access", "validationText" }`. |
| `description`, `format`, `decimalPlaces` | Access's properties of the same names. |
| `richText` | `true` for a rich-text (HTML) memo. |
| `calculatedExpression` | A calculated column's Access expression; its values are exported as Access stored them. |
| `appendOnly`, `hidden` | `true` for an append-only memo, and for Access's own hidden columns (only written with `--include-hidden`). |
| `element` | Multi-value columns only: the type of each value in the array, `{ "type", "accessType", "length", "precision", "scale" }`. |

`default` is `{ "access": "<the expression as Access stores it>", "kind": … }`:

| `kind` | Meaning |
| --- | --- |
| `literal` | A constant; `value` holds it in its own JSON form (a date literal as a date-time string). It is not converted to the column's type. |
| `null` | `Null`. |
| `currentTimestamp`, `currentDate`, `currentTime` | `Now()`, `Date()`, `Time()`: local time when a row is added. |
| `newGuid` | `GenGUID()`. |
| `expression` | Another expression AccessConverter understands. |
| `unsupported` | An expression AccessConverter doesn't translate; `reason` says why. |

A Random AutoNumber is stored by Access as the default `GenUniqueID()`; it shows as `"autoNumber": "random"` and not
as a default.

### Relationships

```json
{ "name": "CustomersOrders", "parent": { "table": "Customers", "columns": ["CustomerID"] },
  "child": { "table": "Orders", "columns": ["CustomerID"] },
  "enforced": true, "onUpdate": "cascade", "onDelete": "cascade", "oneToOne": false, "join": "inner" }
```

Every relationship between two exported tables is listed, **including those Access doesn't enforce**
(`"enforced": false`): those are join lines only, and the child may hold values the parent doesn't. `onUpdate` and
`onDelete` are `noAction`, `cascade` or `setNull`; `join` (`inner`, `leftOuter`, `rightOuter`) is the join type
Access's query designer uses. Parent and child columns are listed pairwise.

## Rows

With `encoding.rows` `object` (the default), a row is `{"column": value, …}` with keys in column order. With `array`
(`--json-rows array`), it is `[value, …]` in the order of `schema.tables[].columns`. In the document each row is one
line; in ndjson each line of a table file is one row.

A table that couldn't be read is empty (`[]`, or an empty file), never partly filled; the conversion report names
it and the conversion exits with code 2.

## Values

NULL is always `null`. Nothing is ever substituted, rounded or truncated.

| `type` | Access | JSON |
| --- | --- | --- |
| `boolean` | Yes/No | `true` / `false` |
| `uint8` | Byte | number, 0 to 255 |
| `int16`, `int32` | Integer, Long Integer, AutoNumber | number |
| `int64` | Large Number | number, exact; a string with `encoding.bigint` `string` (JavaScript's numbers lose precision past 2⁵³) |
| `decimal` | Currency, Decimal | number written exactly with its scale (`922337203685477.5807`, `9.9900`), never in exponent form; a string with `encoding.decimals` `string` |
| `float32` | Single | number: the shortest text that reads back as the same 32-bit float (`0.1`, `1.4E-45`); parse it as a float |
| `float64` | Double | number: the shortest text that reads back as the same double (`0.30000000000000004`) |
| `datetime` | Date/Time | `"YYYY-MM-DDTHH:MM:SS"`, plus `.fff` when there are milliseconds; local time, no offset, since Access has no time zones. Four-digit years (`0100-01-01T00:00:00`); a year past 9999 is written `+10000-01-01T00:00:00`. A time-only value is on Access's day zero, `1899-12-30`. |
| `datetime` | Date/Time Extended | Always seven fraction digits: `"2021-06-14T22:45:12.3456789"` |
| `string` | Short Text, Long Text | string |
| `hyperlink` | Hyperlink | the stored text, `display#address#subaddress#screentip`; with `encoding.hyperlinks` `object`, `{"display", "address", "subAddress", "screenTip"}`, a missing or empty part being `null` |
| `guid` | Replication ID | `"{XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}"`, uppercase |
| `binary` | Binary, and column types Access has that AccessConverter can't read | the stored bytes (see Binary values) |
| `ole` | OLE Object | the exact stored bytes, whatever the object is (see Binary values); an object with `--ole-extract` |
| `attachments` | Attachment | an array of the cell's files, in Access's order (below); `[]` when there are none |
| `multiValue` | Multi-value lookup | an array of the cell's values, each spelled as `element.type` says; `[]` when there are none |
| `versionHistory` | An append-only memo's history | an array of `{"value", "modified"}`, only with `--include-version-history` (the column is left out otherwise) |
| `complexId` | A complex column of a kind AccessConverter can't read | Access's internal id of the cell's values |

Special cases:

- Single and Double NaN or ±Infinity have no JSON form: they are written as `null` and the conversion report
  says so (`DOUBLE_NON_FINITE`).
- Text holding a lone UTF-16 surrogate (half of a pair, which UTF-8 can't encode) is written with a `\uD800`-style
  escape. That is valid JSON and reads back as the same text, but some strict parsers reject it; the report says so
  (`TEXT_UNPAIRED_SURROGATE`).

## Binary values

`encoding.binary` says where the bytes of Binary, OLE and attachment values are (`--binary`):

| `encoding.binary` | A value |
| --- | --- |
| `base64` (default) | the bytes as base64 (standard alphabet, padded, no line breaks); an empty value is `""` |
| `files` | `{"file": "Shop.json-files/Photos/Image/12.jpg", "size": 48213}`: the bytes are in that file, whose path is relative to the output's directory, with `/` on every operating system |
| `omit` | `{"size": 48213}`: the bytes are left out, their count stays; an empty value is `{"size": 0}` |

NULL is `null` in every mode. In `files` mode the files are under `<output>-files/<table>/<column>/`, named after the
row's primary key (the row's position when the table has none); names come from the data only after they are made
safe, so always use the `file` property rather than building a path.

With `--ole-extract` (`encoding.ole` `extracted`), an OLE value is an object: the raw bytes, which are always
there, and what decoding them found.

```json
{ "raw": "FRwyAAIAAAA…", "kind": "package", "name": "test_data.txt", "mime": null, "content": "dGhpcyBpcy…" }
```

| Property | Meaning |
| --- | --- |
| `raw` | The stored bytes, spelled as `encoding.binary` says. |
| `kind` | `package` (a file packaged by the Windows Packager), `embedded` (an object of another program, such as a Word document), `compound` (OLE compound storage), `link` (a linked file), `raw` (bytes that aren't OLE-wrapped, often an image an application stored directly), or `null` when the value couldn't be decoded. |
| `name` | `package`: the file name; `embedded`, `compound`: the OLE class (`Word.Document.8`, `PBrush`); `link`: the link path; otherwise `null`. |
| `mime` | The content's type from its bytes' magic number (PNG, JPEG, GIF, BMP, PDF, ZIP, Office Open XML, OLE storage), never from a name; `null` when not recognized. For `raw`, sniffed from the raw bytes. |
| `content` | `package`: the file; `embedded`, `compound`: the object's bytes; spelled as `encoding.binary` says. `null` for `link` and `raw`. |

An attachment is an object:

```json
{ "fileName": "test_data.txt", "fileType": "txt", "size": 38, "data": "dGhpcyBpcy…", "url": null, "timestamp": null, "flags": null }
```

`data` holds the bytes (Access stores them compressed; these are the file's own bytes) with `encoding.binary`
`base64`; `file` replaces it with `files`; with `omit` neither is there. `size` is always the file's length.

## Validating

The JSON Schema validates a document, and an ndjson `schema.json`. Each line of a table file is a
`#/$defs/objectRow` or a `#/$defs/arrayRow`, as `encoding.rows` says. `accessconverter verify <database> <output>`
goes further: it compares the schema section and every value with the Access database.
