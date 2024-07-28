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
    public TextStringBuilder sqlDump;
    
    public MySQLConverter(Args args, Database db) {
        this.args = args;
        this.db = db;
    }
    
    public boolean toMySQLDump() {
        boolean result = false;
        final String methodName = "toMySQLDump";
        
        try {
            sqlDump = new TextStringBuilder();
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

    private void addHeader() {
        sqlDump.appendln("-- %s", Application.Title);
        sqlDump.appendln("-- version %s", Application.Version);
        sqlDump.appendln("-- author %s", Application.Author);
        sqlDump.appendln("-- %s", Application.Web);
        sqlDump.appendln("--");
        Locale locale = new Locale("en", "US");
        String generationTime = LocalDateTime.now().format(
            DateTimeFormatter.ofPattern("EEE d, yyyy 'at' hh:mm a", locale)
        );
        sqlDump.appendln("-- Generation time: %s", generationTime);
        sqlDump.appendNewLine();
        
        sqlDump.appendln("SET SQL_MODE = \"NO_AUTO_VALUE_ON_ZERO\";");
        sqlDump.appendln("SET AUTOCOMMIT = 0;");
        sqlDump.appendln("START TRANSACTION;");
        sqlDump.appendln("SET time_zone = \"+00:00\";");
        
        sqlDump.appendNewLine();
        sqlDump.appendNewLine();
        
        sqlDump.appendln("/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;");
        sqlDump.appendln("/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;");
        sqlDump.appendln("/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;");
        sqlDump.appendln("/*!40101 SET NAMES %s */;", charset);
        sqlDump.appendNewLine();
    }
    
    private void addFooter() {
        sqlDump.appendln("COMMIT;");
        sqlDump.appendNewLine();
        
        sqlDump.appendln("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;");
        sqlDump.appendln("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;");
        sqlDump.appendln("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;");
        sqlDump.appendNewLine();
    }
    
    private void addTableCreate(Table table) throws IOException {
        if(args.HasFlag("mysql-drop-tables")) {
            sqlDump.appendln("--");
            sqlDump.appendln("-- Drop table `%s` if exists", table.getName());
            sqlDump.appendln("--");
            sqlDump.appendNewLine();
            sqlDump.appendln("DROP TABLE IF EXISTS `%s`;", table.getName());
            sqlDump.appendNewLine();
        }
        
        sqlDump.appendln("--");
        sqlDump.appendln("-- Table structure for table `%s`", table.getName());
        sqlDump.appendln("--");
        sqlDump.appendNewLine();
        
        sqlDump.appendln("CREATE TABLE IF NOT EXISTS `%s` (", table.getName());
        
        boolean isFirst = true;
        
        for (Column column: table.getColumns()) {
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

                required = ((Boolean)column.getProperties().getValue(PropertyMap.REQUIRED_PROP, false));
            } catch (IOException e) {}

            Boolean notNull = required;
            String defVal = defaultValue != null ? defaultValue : "0";
            Boolean isDefaultValueModifier = false;
            
            if (!isFirst)
                sqlDump.appendln(",");
            else
                isFirst = false;
            
            sqlDump.append("  `%s` ", name);

            switch (type) {
                case "BYTE":
                case "INT":
                case "LONG": {
                    if (column.isAutoNumber()) {
                        AutoIncrement autoIncrement = new AutoIncrement();
                        autoIncrement.tableName = table.getName();
                        autoIncrement.columnName = name;
                        autoIncrements.put(autoIncrement.tableName, autoIncrement);
                        sqlDump.append("INT(10) UNSIGNED");
                        notNull = true;
                        defVal = null;
                    } else {
                        switch (length) {
                            case 1: {
                                sqlDump.append("TINYINT(3)");
                                break;
                            }
                            case 2: {
                                sqlDump.append("SMALLINT(5)");
                                break;
                            }
                            // case 4:
                            // case 6:
                            default: {
                                sqlDump.append("INT(10)");
                            }
                        }

                        // If the column has a relationship,
                        // make it unsigned because relationships will fail otherwise
                        for (Relationship rel: this.db.getRelationships(table)) {
                            if (!rel.getToTable().getName().equals(table.getName())) {
                                continue;
                            }

                            // Related column is auto increment, thus we have to make it unsigned
                            boolean hasAutoIncrementRelationship = false;

                            for (Column relFromCol: rel.getFromColumns()) {
                                if (relFromCol.isAutoNumber()) {
                                    hasAutoIncrementRelationship = true;
                                    break;
                                }
                            }

                            if (!hasAutoIncrementRelationship) {
                                continue;
                            }

                            for (Column relToCol: rel.getToColumns()) {
                                if (relToCol.getName().equals(name)) {
                                    sqlDump.append(" UNSIGNED");
                                    break;
                                }
                            }
                        }
                    }
                    break;
                }
                case "FLOAT": {
                    sqlDump.append("FLOAT");
                    break;
                }
                case "DOUBLE": {
                    sqlDump.append("DOUBLE");
                    break;
                }
                case "NUMERIC": {
                    sqlDump.append("DECIMAL(28,0)");
                    break;
                }
                case "MONEY": {
                    sqlDump.append("DECIMAL(15,4)");
                    break;
                }
                case "BOOLEAN": {
                    defVal = Utils.booleanDefaultValue(defVal);
                    sqlDump.append("TINYINT(3)");
                    break;
                }
                case "SHORT_DATE_TIME": {
                    defVal = defaultValue != null ? defaultValue : "0000-00-00 00:00:00";

                    if (Utils.isDatetimeNow(defVal)) {
                        defVal = "CURRENT_TIMESTAMP";
                        isDefaultValueModifier = true;
                    }

                    sqlDump.append("DATETIME");
                    break;
                }
                case "MEMO": {
                    defVal = null;
                    useCollate = true;
                    sqlDump.append("TEXT");
                    break;
                }
                case "GUID": {
                    defVal = defaultValue != null ? defaultValue : "{00000000-0000-0000-0000-000000000000}";
                    useCollate = true;
                    sqlDump.append("VARCHAR(50)");
                    break;
                }
                case "BINARY":
                case "OLE": {
                    sqlDump.append("BLOB");
                    break;
                }
                case "COMPLEX_TYPE": {
                    // We will be storing complex type attachments as JSON data
                    // with attachment info and binary data
                    sqlDump.append("LONGTEXT");
                    break;
                }
                case "TEXT":
                default: {
                    defVal = defaultValue != null ? defaultValue : "";
                    useCollate = true;
                    sqlDump.append("VARCHAR(255)");
                    break;
                }
            }

            if (useCollate) {
                sqlDump.append(" COLLATE %s", collate);
            }

            if (notNull) {
                sqlDump.append(" NOT NULL");
            }

            if (defVal != null) {
                if (isDefaultValueModifier) {
                    sqlDump.append(" DEFAULT %s", defVal);
                } else {
                    // Add single quotes if it's not a modifier
                    sqlDump.append(" DEFAULT '%s'", defVal);
                }
            }
        }

        // Make relationship definitions

        for (Relationship rel: this.db.getRelationships(table)) {
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

        for (Index idx: table.getIndexes()) {
            var index = new IndexDefinitions();
            index.name = idx.getName();
            index.tableName = table.getName();
            index.isPrimary = idx.isPrimaryKey();
            index.isUnique = idx.isUnique();
            index.columns = Utils.quoteSqlNames(idx.getColumns().stream().map(c -> c.getName()).collect(Collectors.toList()));
            indexes.add(index);
        }
        
        sqlDump.appendNewLine();
        sqlDump.appendln(") ENGINE=%s DEFAULT CHARSET=%s COLLATE=%s;", engine, charset, collate);
        sqlDump.appendNewLine();
    }
    
    private void addTableInsert(Table table) {
        if (table.getRowCount() == 0)
            return;
        
        String tableName = table.getName();
        
        sqlDump.appendln("--");
        sqlDump.appendln(String.format("-- Dumping data for table `%s`", tableName));
        sqlDump.appendln("--");
        sqlDump.appendNewLine();
        
        TextStringBuilder insertHeader = new TextStringBuilder();
        insertHeader.append(String.format("INSERT INTO `%s` (", tableName));
        boolean isFirst = true;
        
        for (Column column : table.getColumns()) {
            if (!isFirst)
                insertHeader.append(", ");
            else
                isFirst = false;
            insertHeader.append(String.format("`%s`", column.getName()));
        }
        
        insertHeader.appendln(") VALUES");
        sqlDump.append(insertHeader);
        boolean isFirstRow = true;
        boolean isFirstColumn;
        int insertRows = 0;
        
        for (Row row : table) {
            String rowId = row.getString("id");

            if (!isFirstRow)
                sqlDump.append(", ");
            else
                isFirstRow = false;
                
            isFirstColumn = true;
            sqlDump.append("(");
            
            for (Column column : table.getColumns()) {
                String type = column.getType().toString().toUpperCase();
                String name = column.getName();
                
                if (!isFirstColumn)
                    sqlDump.append(", ");
                else
                    isFirstColumn = false;
                
                try {
                    switch (type) {
                        case "BYTE": {
                            sqlDump.append(Utils.valueOrNull(row.getByte(name)));
                            break;
                        }
                        case "INT": {
                            sqlDump.append(Utils.valueOrNull(row.getShort(name)));
                            break;
                        }
                        case "LONG": {
                            if (column.isAutoNumber() && autoIncrements.containsKey(tableName)) {
                                autoIncrements.get(tableName).setMaxId(row.getInt(name));
                            }
                            sqlDump.append(Utils.valueOrNull(row.getInt(name)));
                            break;
                        }
                        case "FLOAT": {
                            sqlDump.append(Utils.valueOrNull(row.getFloat(name)));
                            break;
                        }
                        case "DOUBLE": {
                            sqlDump.append(Utils.valueOrNull(row.getDouble(name)));
                            break;
                        }
                        case "NUMERIC":
                        case "MONEY": {
                            sqlDump.append(Utils.valueOrNull(row.getBigDecimal(name)));
                            break;
                        }
                        case "BOOLEAN": {
                            var value = row.getBoolean(name);

                            if (value == null) {
                                sqlDump.append("NULL");
                            } else {
                                sqlDump.append(value ? 1 : 0);
                            }

                            break;
                        }
                        case "SHORT_DATE_TIME": {
                            LocalDateTime value = row.getLocalDateTime(name);

                            if (value == null) {
                                sqlDump.append("NULL");
                            } else {
                                
                                // SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                                // sqlDump.append("'%s'", format.format(value));
                                sqlDump.append("'%s'", value.format(format));
                            }
                            break;
                        }
                        case "MEMO":
                        case "GUID":
                        case "TEXT": {
                            var value = row.getString(name);

                            if (value == null) {
                                sqlDump.append("NULL");
                            } else {
                                sqlDump.append("'%s'", row.getString(name).replace("'", "''"));
                            }
                            break;
                        }
                        case "BINARY":
                        case "OLE": {
                            // byte[] data = row.getBytes(name);
                            // if (data.length > 0) {
                            //     sqlDump.append("UNHEX('%s')", Hex.encodeHexString(data));
                            // } else {
                                sqlDump.append("NULL");
                            // }
                            break;
                        }
                        case "COMPLEX_TYPE": {
                            if (column.getComplexInfo().getType().name() == "ATTACHMENT") {
                                try {
                                    ComplexValueForeignKey valueFk =
                                        (ComplexValueForeignKey)column.getRowValue(row);
                                    List<Attachment> attachments = valueFk.getAttachments();

                                    if (!attachments.isEmpty()) {
                                        var fileValue = new FileValue(args, Globals.OUTPUT_SQLITE, this);
                                        
                                        if (fileValue.handleAttachments(column, row, attachments)) {
                                            var json = fileValue.getRecordsJson();
                                            sqlDump.append("'%s'", Utils.escapeSingleQuotes(json));
                                        } else {
                                            sqlDump.append("NULL");
                                        }

                                        // for (Attachment attachment : attachments) {
                                        //     System.out.println(
                                        //         String.format(
                                        //             "Attachment: %s, type %s",
                                        //             attachment.getFileName(),
                                        //             attachment.getFileType()
                                        //         )
                                        //     );
                                        // }

                                        // sqlDump.append("NULL");
                                    } else {
                                        sqlDump.append("NULL");
                                    }
                                } catch (IOException ex) {
                                    sqlDump.append("NULL");
                                }

                                // sqlDump.append("UNHEX('%s')", Hex.encodeHexString(row.getBytes(name)));
                            } else {
                                sqlDump.append("NULL");
                            }
                            break;
                        }
                        default: {
                            sqlDump.append("NULL");
                            break;
                        }
                    }
                } catch (NullPointerException e) {
                    sqlDump.append("NULL");
                }
            }

            sqlDump.append(")");

            if (++insertRows >= maxInsertRows) {
                sqlDump.appendln(";");
                sqlDump.append(insertHeader);
                insertRows = 0;
                isFirstRow = true;
            }
        }

        if(!isFirstRow)
            sqlDump.appendln(";");
    }
    
    public void addAutoIncrements() {
        Boolean infoAdded = false;

        for (AutoIncrement autoIncrement : autoIncrements.values()) {
            if (!infoAdded) {
                sqlDump.appendln("--");
                sqlDump.appendln("-- AUTO_INCREMENT for table `%s`", autoIncrement.tableName);
                sqlDump.appendln("--");
                sqlDump.appendNewLine();
                infoAdded = true;
            }

            sqlDump.appendln("ALTER TABLE `%s`", autoIncrement.tableName);
            sqlDump.appendln(
                "  MODIFY `%s` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=%d;", 
                autoIncrement.columnName, 
                autoIncrement.maxId + 1
            );
            sqlDump.appendNewLine();
        }

        if (!autoIncrements.isEmpty()) {
            autoIncrements.clear();
        }
    }

    public void addIndexes() {
        Boolean infoAdded = false;

        for (IndexDefinitions index : indexes) {
            if (!infoAdded) {
                sqlDump.appendln("--");
                sqlDump.appendln("-- Indexes for table `%s`", index.tableName);
                sqlDump.appendln("--");
                sqlDump.appendNewLine();
                infoAdded = true;
            }

            var columns = String.join(", ", index.columns);

            if (index.isPrimary) {
                sqlDump.appendln("ALTER TABLE `%s`", index.tableName);
                sqlDump.appendln("  ADD PRIMARY KEY (%s);", columns);
            } else {
                if (index.isUnique) {
                    sqlDump.appendln("CREATE UNIQUE INDEX `%s`", index.name);
                } else {
                    sqlDump.appendln("CREATE INDEX `%s`", index.name);
                }

                sqlDump.appendln("  ON `%s` (%s);", index.tableName, columns);
            }

            sqlDump.appendNewLine();
        }

        if (!indexes.isEmpty()) {
            indexes.clear();
        }
    }

    public void addRelationships() {
        List<String> infoAddedForTable = new ArrayList<>();

        for (RelationshipDefinitions rel : relationships) {
            if (!infoAddedForTable.contains(rel.tableName)) {
                sqlDump.appendln("--");
                sqlDump.appendln("-- Relationships for table `%s`", rel.tableName);
                sqlDump.appendln("--");
                sqlDump.appendNewLine();
                infoAddedForTable.add(rel.tableName);
            }

            sqlDump.appendln("ALTER TABLE `%s`", rel.tableName);

            if (rel.name != null) {
                sqlDump.appendln("  ADD CONSTRAINT `%s` FOREIGN KEY", rel.name);
            } else {
                sqlDump.appendln("  ADD FOREIGN KEY");
            }

            sqlDump.appendln("  (%s)", String.join(", ", rel.columns));
            sqlDump.append(
                "  REFERENCES `%s` (%s)",
                rel.refTableName,
                String.join(", ", rel.refColumns)
            );

            if (rel.onDelete != null) {
                sqlDump.appendNewLine();
                sqlDump.append("  ON DELETE %s", rel.onDelete);
            }

            if (rel.onUpdate != null) {
                sqlDump.appendNewLine();
                sqlDump.append("  ON UPDATE %s", rel.onUpdate);
            }

            sqlDump.appendln(";");
            sqlDump.appendNewLine();
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
