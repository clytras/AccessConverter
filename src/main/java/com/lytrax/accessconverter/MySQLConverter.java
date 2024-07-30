/*
 * The MIT License
 *
 * Copyright 2024 Christos Lytras <christos.lytras@gmail.com>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.lytrax.accessconverter;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Relationship;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.Attachment;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.text.TextStringBuilder;

/**
 *
 * @author Christos Lytras <christos.lytras@gmail.com>
 */
public class MySQLConverter extends Converter {
    public final String DefaultCollate = "utf8mb4_unicode_ci";
    public final String DefaultCharset = "utf8mb4";
    public final String DefaultEngine = "InnoDB";
    public final int DefaultMaxInsertRows = 100;

    public String collate = DefaultCollate;
    public String charset = DefaultCharset;
    public String engine = DefaultEngine;
    public int maxInsertRows = DefaultMaxInsertRows;

    public class AutoIncrement {
        public int maxId = 0;
        public String columnName;
        public String tableName;
        public void setMaxId(int newMaxId) {
            maxId = Math.max(maxId, newMaxId);
        }
    }

    public class IndexDefinitions {
        public String name = null;
        public String tableName;
        public List<String> columns;
        public Boolean isUnique;
        public Boolean isPrimary;
    }

    public class RelationshipDefinitions {
        public String name = null;
        public String tableName;
        public List<String> columns;
        public String refTableName;
        public List<String> refColumns;
        public String onDelete = null;
        public String onUpdate = null;
    }

    public Database db;
    public Args args;
    public Map<String, AutoIncrement> autoIncrements = new HashMap<>();
    public List<IndexDefinitions> indexes = new ArrayList<>();
    public List<RelationshipDefinitions> relationships = new ArrayList<>();
    private SqlFileWriter sqlWriter;

    public MySQLConverter(Args args, Database db, SqlFileWriter sqlWriter) {
        this.args = args;
        this.db = db;
        this.sqlWriter = sqlWriter;
    }

    public boolean toMySQLDump() {
        boolean result = false;
        final String methodName = "toMySQLDump";

        try {
            addHeader();
            Set<String> tableNames = db.getTableNames();

            tableNames.forEach((tableName) -> {
                try {
                    Table table = db.getTable(tableName);
                    AccessConverter.progressStatus.startTable(table);
                    addTableCreate(table);
                    addTableInsert(table);
                    addIndexes();
                    addAutoIncrements();
                    AccessConverter.progressStatus.endTable();
                } catch (IOException e) {
                    Error(String.format("Could not load table '%s'", tableName), e, methodName);
                }
            });

            addRelationships();
            addFooter();
            AccessConverter.progressStatus.resetLine();
            result = true;
        } catch (IOException e) {
            Error("Could not fetch tables from the database", e, methodName);
        }

        return result;
    }

    private void addHeader() throws IOException {
        sqlWriter.writeln("-- %s", Application.Title);
        sqlWriter.writeln("-- version %s", Application.Version);
        sqlWriter.writeln("-- author %s", Application.Author);
        sqlWriter.writeln("-- %s", Application.Web);
        sqlWriter.writeln("--");
        Locale locale = new Locale("en", "US");
        String generationTime = LocalDateTime.now().format(
            DateTimeFormatter.ofPattern("EEE d, yyyy 'at' hh:mm a", locale)
        );
        sqlWriter.writeln("-- Generation time: %s", generationTime);
        sqlWriter.writeNewLine();

        sqlWriter.writeln("SET SQL_MODE = \"NO_AUTO_VALUE_ON_ZERO\";");
        sqlWriter.writeln("SET AUTOCOMMIT = 0;");
        sqlWriter.writeln("START TRANSACTION;");
        sqlWriter.writeln("SET time_zone = \"+00:00\";");

        sqlWriter.writeNewLine();
        sqlWriter.writeNewLine();

        sqlWriter.writeln("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;");
        sqlWriter.writeln("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;");
        sqlWriter.writeln("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;");
        sqlWriter.writeln("/*!40101 SET NAMES %s */;", charset);
        sqlWriter.writeNewLine();
    }

    private void addFooter() throws IOException {
        sqlWriter.writeln("COMMIT;");
        sqlWriter.writeNewLine();

        sqlWriter.writeln("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;");
        sqlWriter.writeln("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;");
        sqlWriter.writeln("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;");
        sqlWriter.writeNewLine();
    }

    private void addTableCreate(Table table) throws IOException {
        if (args.HasFlag("mysql-drop-tables")) {
            sqlWriter.writeln("--");
            sqlWriter.writeln("-- Drop table `%s` if exists", table.getName());
            sqlWriter.writeln("--");
            sqlWriter.writeNewLine();
            sqlWriter.writeln("DROP TABLE IF EXISTS `%s`;", table.getName());
            sqlWriter.writeNewLine();
        }

        sqlWriter.writeln("--");
        sqlWriter.writeln("-- Table structure for table `%s`", table.getName());
        sqlWriter.writeln("--");
        sqlWriter.writeNewLine();

        sqlWriter.writeln("CREATE TABLE IF NOT EXISTS `%s` (", table.getName());

        boolean isFirst = true;

        for (Column column : table.getColumns()) {
            String name = column.getName();
            String type = column.getType().toString().toUpperCase();
            short length = column.getLength();
            String defaultValue = null;
            Boolean required = false;
            Boolean useCollate = false;

            try {
                var defaultColVal = column.getProperties().getValue(PropertyMap.DEFAULT_VALUE_PROP, null);

                if (defaultColVal != null) {
                    defaultValue = Utils.removeQuotation(defaultColVal.toString());
                }

                required = (Boolean) column.getProperties().getValue(PropertyMap.REQUIRED_PROP, false);
            } catch (IOException e) {}

            Boolean notNull = required;
            String defVal = defaultValue != null ? defaultValue : "0";
            Boolean isDefaultValueModifier = false;

            if (!isFirst) {
                sqlWriter.writeln(",");
            } else {
                isFirst = false;
            }

            sqlWriter.write("  `%s` ", name);

            switch (type) {
                case "BYTE":
                case "INT":
                case "LONG": {
                    if (column.isAutoNumber()) {
                        AutoIncrement autoIncrement = new AutoIncrement();
                        autoIncrement.tableName = table.getName();
                        autoIncrement.columnName = name;
                        autoIncrements.put(autoIncrement.tableName, autoIncrement);
                        sqlWriter.write("INT(10) UNSIGNED");
                        notNull = true;
                        defVal = null;
                    } else {
                        switch (length) {
                            case 1: {
                                sqlWriter.write("TINYINT(3)");
                                break;
                            }
                            case 2: {
                                sqlWriter.write("SMALLINT(5)");
                                break;
                            }
                            // case 4:
                            // case 6:
                            default: {
                                sqlWriter.write("INT(10)");
                            }
                        }

                        // If the column has a relationship,
                        // make it unsigned because relationships will fail otherwise
                        for (Relationship rel : this.db.getRelationships(table)) {
                            if (!rel.getToTable().getName().equals(table.getName())) {
                                continue;
                            }

                            // Related column is auto increment, thus we have to make it unsigned
                            boolean hasAutoIncrementRelationship = false;

                            for (Column relFromCol : rel.getFromColumns()) {
                                if (relFromCol.isAutoNumber()) {
                                    hasAutoIncrementRelationship = true;
                                    break;
                                }
                            }

                            if (!hasAutoIncrementRelationship) {
                                continue;
                            }

                            for (Column relToCol : rel.getToColumns()) {
                                if (relToCol.getName().equals(name)) {
                                    sqlWriter.write(" UNSIGNED");
                                    break;
                                }
                            }
                        }
                    }
                    break;
                }
                case "FLOAT": {
                    sqlWriter.write("FLOAT");
                    break;
                }
                case "DOUBLE": {
                    sqlWriter.write("DOUBLE");
                    break;
                }
                case "NUMERIC": {
                    sqlWriter.write("DECIMAL(28,0)");
                    break;
                }
                case "MONEY": {
                    sqlWriter.write("DECIMAL(15,4)");
                    break;
                }
                case "BOOLEAN": {
                    defVal = Utils.booleanDefaultValue(defVal);
                    sqlWriter.write("TINYINT(3)");
                    break;
                }
                case "SHORT_DATE_TIME": {
                    defVal = defaultValue != null ? defaultValue : "0000-00-00 00:00:00";

                    if (Utils.isDatetimeNow(defVal)) {
                        defVal = "CURRENT_TIMESTAMP";
                        isDefaultValueModifier = true;
                    }

                    sqlWriter.write("DATETIME");
                    break;
                }
                case "MEMO": {
                    defVal = null;
                    useCollate = true;
                    sqlWriter.write("TEXT");
                    break;
                }
                case "GUID": {
                    defVal = defaultValue != null ? defaultValue : "{00000000-0000-0000-0000-000000000000}";
                    useCollate = true;
                    sqlWriter.write("VARCHAR(50)");
                    break;
                }
                case "BINARY": {
                    sqlWriter.write("BLOB");
                    break;
                }
                case "OLE":
                case "COMPLEX_TYPE": {
                    // We will be storing complex type attachments and OLE objects as JSON data
                    // with attachment/content info and binary data or file paths
                    sqlWriter.write("LONGTEXT");

                    if (notNull) {
                        defVal = "NULL";
                        isDefaultValueModifier = true;
                    }

                    break;
                }
                case "TEXT":
                default: {
                    defVal = defaultValue != null ? defaultValue : "";
                    useCollate = true;
                    sqlWriter.write("VARCHAR(255)");
                    break;
                }
            }

            if (useCollate) {
                sqlWriter.write(" COLLATE %s", collate);
            }

            if (notNull) {
                sqlWriter.write(" NOT NULL");
            }

            if (defVal != null) {
                if (isDefaultValueModifier) {
                    sqlWriter.write(" DEFAULT %s", defVal);
                } else {
                    // Add single quotes if it's not a modifier
                    sqlWriter.write(" DEFAULT '%s'", defVal);
                }
            }
        }

        // Make relationship definitions

        for (Relationship rel : this.db.getRelationships(table)) {
            if (!table.getName().equals(rel.getToTable().getName())) {
                continue;
            }

            var relationship = new RelationshipDefinitions();
            relationship.name = rel.getName();
            relationship.tableName = rel.getToTable().getName();
            relationship.columns = Utils.quoteSqlNames(rel.getToColumns().stream().map(Column::getName).collect(Collectors.toList()));
            relationship.refTableName = rel.getFromTable().getName();
            relationship.refColumns = Utils.quoteSqlNames(rel.getFromColumns().stream().map(Column::getName).collect(Collectors.toList()));
            relationship.onDelete = getRelationshipOnDelete(rel);
            relationship.onUpdate = getRelationshipOnUpdate(rel);
            relationships.add(relationship);
        }

        // Make indexe definitions

        for (Index idx : table.getIndexes()) {
            var index = new IndexDefinitions();
            index.name = idx.getName();
            index.tableName = table.getName();
            index.isPrimary = idx.isPrimaryKey();
            index.isUnique = idx.isUnique();
            index.columns = Utils.quoteSqlNames(idx.getColumns().stream().map(c -> c.getName()).collect(Collectors.toList()));
            indexes.add(index);
        }

        sqlWriter.writeNewLine();
        sqlWriter.writeln(") ENGINE=%s DEFAULT CHARSET=%s COLLATE=%s;", engine, charset, collate);
        sqlWriter.writeNewLine();
    }

    private void addTableInsert(Table table) throws IOException {
        if (table.getRowCount() == 0) {
            return;
        }

        String tableName = table.getName();
        List<String> columnNames = table.getColumns()
            .stream()
            .map(Column::getName)
            .collect(Collectors.toList());

        sqlWriter.writeln("--");
        sqlWriter.writeln(String.format("-- Dumping data for table `%s`", tableName));
        sqlWriter.writeln("--");
        sqlWriter.writeNewLine();

        TextStringBuilder insertHeader = new TextStringBuilder();

        insertHeader.append(
            "INSERT INTO `%s` (%s) VALUES",
            tableName,
            String.join(", ", Utils.quoteSqlNames(columnNames))
        );

        sqlWriter.write(insertHeader);

        boolean isFirstRow = true;
        boolean isFirstColumn;
        int insertRows = 0;

        for (Row row : table) {
            if (!isFirstRow) {
                sqlWriter.write(", ");
            } else {
                isFirstRow = false;
            }

            isFirstColumn = true;
            sqlWriter.write("(");

            for (Column column : table.getColumns()) {
                String type = column.getType().toString().toUpperCase();
                String name = column.getName();

                if (!isFirstColumn) {
                    sqlWriter.write(", ");
                } else {
                    isFirstColumn = false;
                }

                try {
                    switch (type) {
                        case "BYTE": {
                            sqlWriter.write(Utils.valueOrNull(row.getByte(name)));
                            break;
                        }
                        case "INT": {
                            sqlWriter.write(Utils.valueOrNull(row.getShort(name)));
                            break;
                        }
                        case "LONG": {
                            if (column.isAutoNumber() && autoIncrements.containsKey(tableName)) {
                                autoIncrements.get(tableName).setMaxId(row.getInt(name));
                            }

                            sqlWriter.write(Utils.valueOrNull(row.getInt(name)));
                            break;
                        }
                        case "FLOAT": {
                            sqlWriter.write(Utils.valueOrNull(row.getFloat(name)));
                            break;
                        }
                        case "DOUBLE": {
                            sqlWriter.write(Utils.valueOrNull(row.getDouble(name)));
                            break;
                        }
                        case "NUMERIC":
                        case "MONEY": {
                            sqlWriter.write(Utils.valueOrNull(row.getBigDecimal(name)));
                            break;
                        }
                        case "BOOLEAN": {
                            var value = row.getBoolean(name);

                            if (value == null) {
                                sqlWriter.write("NULL");
                            } else {
                                sqlWriter.write(value ? 1 : 0);
                            }

                            break;
                        }
                        case "SHORT_DATE_TIME": {
                            LocalDateTime value = row.getLocalDateTime(name);

                            if (value == null) {
                                sqlWriter.write("NULL");
                            } else {
                                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                                sqlWriter.write("'%s'", value.format(format));
                            }

                            break;
                        }
                        case "MEMO":
                        case "GUID":
                        case "TEXT": {
                            var value = row.getString(name);

                            if (value == null) {
                                sqlWriter.write("NULL");
                            } else {
                                sqlWriter.write("'%s'", Utils.escapeSingleQuotes(row.getString(name)));
                            }

                            break;
                        }
                        case "BINARY": {
                            byte[] data = row.getBytes(name);

                            if (data.length > 0) {
                                sqlWriter.write("UNHEX('%s')", Hex.encodeHexString(data));
                            } else {
                                sqlWriter.write("NULL");
                            }

                            break;
                        }
                        case "OLE": {
                            var fileValue = new FileValue(args, Globals.OUTPUT_MYSQL, this);

                            try {
                                if (fileValue.handleOle(column, row, row.getBlob(name))) {
                                    var json = fileValue.getRecordsJson();
                                    sqlWriter.write("'%s'", Utils.escapeSingleQuotes(json));
                                } else {
                                    sqlWriter.write("NULL");
                                }
                            } catch (IOException e) {
                                sqlWriter.write("NULL");
                                Error(
                                    String.format(
                                        "Count not fetch OLE data for column '%s' (%s) in table '%s'",
                                        name, row.getId().hashCode(), tableName
                                    )
                                );
                            }

                            break;
                        }
                        case "COMPLEX_TYPE": {
                            if (column.getComplexInfo().getType().name() == "ATTACHMENT") {
                                try {
                                    ComplexValueForeignKey valueFk =
                                        (ComplexValueForeignKey)column.getRowValue(row);
                                    List<Attachment> attachments = valueFk.getAttachments();

                                    if (!attachments.isEmpty()) {
                                        var fileValue = new FileValue(args, Globals.OUTPUT_MYSQL, this);

                                        if (fileValue.handleAttachments(column, row, attachments)) {
                                            var json = fileValue.getRecordsJson();
                                            sqlWriter.write("'%s'", Utils.escapeSingleQuotes(json));
                                        } else {
                                            sqlWriter.write("NULL");
                                        }
                                    } else {
                                        sqlWriter.write("NULL");
                                    }
                                } catch (IOException ex) {
                                    sqlWriter.write("NULL");
                                    Error(
                                        String.format(
                                            "Count not fetch attachments for column '%s' (%s) in table '%s'",
                                            name, row.getId().hashCode(), tableName
                                        )
                                    );
                                }
                            } else {
                                sqlWriter.write("NULL");
                            }

                            break;
                        }
                        default: {
                            sqlWriter.write("NULL");
                            break;
                        }
                    }
                } catch (NullPointerException e) {
                    sqlWriter.write("NULL");
                }
            }

            sqlWriter.write(")");

            if (++insertRows >= maxInsertRows) {
                sqlWriter.writeln(";");
                sqlWriter.write(insertHeader);
                sqlWriter.flush();
                insertRows = 0;
                isFirstRow = true;
            }

            AccessConverter.progressStatus.step();
        }

        if (!isFirstRow) {
            sqlWriter.writeln(";");
            sqlWriter.flush();
        }
    }

    public void addAutoIncrements() throws IOException {
        Boolean infoAdded = false;

        for (AutoIncrement autoIncrement : autoIncrements.values()) {
            if (!infoAdded) {
                sqlWriter.writeln("--");
                sqlWriter.writeln("-- AUTO_INCREMENT for table `%s`", autoIncrement.tableName);
                sqlWriter.writeln("--");
                sqlWriter.writeNewLine();
                infoAdded = true;
            }

            sqlWriter.writeln("ALTER TABLE `%s`", autoIncrement.tableName);
            sqlWriter.writeln(
                "  MODIFY `%s` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=%d;",
                autoIncrement.columnName,
                autoIncrement.maxId + 1
            );
            sqlWriter.writeNewLine();
        }

        if (!autoIncrements.isEmpty()) {
            autoIncrements.clear();
        }
    }

    public void addIndexes() throws IOException {
        Boolean infoAdded = false;

        for (IndexDefinitions index : indexes) {
            if (!infoAdded) {
                sqlWriter.writeln("--");
                sqlWriter.writeln("-- Indexes for table `%s`", index.tableName);
                sqlWriter.writeln("--");
                sqlWriter.writeNewLine();
                infoAdded = true;
            }

            var columns = String.join(", ", index.columns);

            if (index.isPrimary) {
                sqlWriter.writeln("ALTER TABLE `%s`", index.tableName);
                sqlWriter.writeln("  ADD PRIMARY KEY (%s);", columns);
            } else {
                if (index.isUnique) {
                    sqlWriter.writeln("CREATE UNIQUE INDEX `%s`", index.name);
                } else {
                    sqlWriter.writeln("CREATE INDEX `%s`", index.name);
                }

                sqlWriter.writeln("  ON `%s` (%s);", index.tableName, columns);
            }

            sqlWriter.writeNewLine();
        }

        if (!indexes.isEmpty()) {
            indexes.clear();
        }
    }

    public void addRelationships() throws IOException {
        List<String> infoAddedForTable = new ArrayList<>();

        for (RelationshipDefinitions rel : relationships) {
            if (!infoAddedForTable.contains(rel.tableName)) {
                sqlWriter.writeln("--");
                sqlWriter.writeln("-- Relationships for table `%s`", rel.tableName);
                sqlWriter.writeln("--");
                sqlWriter.writeNewLine();
                infoAddedForTable.add(rel.tableName);
            }

            sqlWriter.writeln("ALTER TABLE `%s`", rel.tableName);

            if (rel.name != null) {
                sqlWriter.writeln("  ADD CONSTRAINT `%s` FOREIGN KEY", rel.name);
            } else {
                sqlWriter.writeln("  ADD FOREIGN KEY");
            }

            sqlWriter.writeln("  (%s)", String.join(", ", rel.columns));
            sqlWriter.write(
                "  REFERENCES `%s` (%s)",
                rel.refTableName,
                String.join(", ", rel.refColumns)
            );

            if (rel.onDelete != null) {
                sqlWriter.writeNewLine();
                sqlWriter.write("  ON DELETE %s", rel.onDelete);
            }

            if (rel.onUpdate != null) {
                sqlWriter.writeNewLine();
                sqlWriter.write("  ON UPDATE %s", rel.onUpdate);
            }

            sqlWriter.writeln(";");
            sqlWriter.writeNewLine();
        }

        if (!relationships.isEmpty()) {
            relationships.clear();
        }
    }

    private String getRelationshipOnDelete(Relationship relationship) {
        if (relationship.cascadeDeletes()) {
            return "CASCADE";
        }

        if (relationship.cascadeNullOnDelete()) {
            return "SET NULL";
        }

        return null;
    }

    private String getRelationshipOnUpdate(Relationship relationship) {
        if (relationship.cascadeUpdates()) {
            return "CASCADE";
        }

        return null;
    }
}
