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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.text.TextStringBuilder;

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
 */
public class SQLiteConverter extends Converter {
    public Database db;
    public Args args;
    public File sqliteFile;
    //public List<String> lastError = new ArrayList<>();
    private Connection connection = null;
    
    public SQLiteConverter(Args args, Database db, File sqliteFile) {
        this.args = args;
        this.db = db;
        this.sqliteFile = sqliteFile;
    }
    
    public boolean toSQLiteFile() {
        boolean result = false;
        final String methodName = "toSQLiteFile";
        
        try {
            // load the sqlite-JDBC driver using the current class loader
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            //Logger.getLogger(SQLiteConverter.class.getName()).log(Level.SEVERE, null, ex);
            //AccessConverter.Error("Coud not load class org.sqlite.JDBC");
            Error("Coud not load class org.sqlite.JDBC", e, methodName);
            return false;
        }
        
        try {
            connection = DriverManager.getConnection(String.format("jdbc:sqlite:%s", sqliteFile.getAbsolutePath()));
            Set<String> tableNames = db.getTableNames();

            tableNames.forEach((tableName) -> {
                try {
                    Table table = db.getTable(tableName);
                    AccessConverter.progressStatus.startTable(table);
                    if (!createTable(table)) {
                        Log(String.format("Could not create table schema '%s'", tableName), methodName);
                    } else {
                        insertData(table);
                    }
                    AccessConverter.progressStatus.endTable();
                } catch (IOException e) {
                    //lastError.add(String.format("Could not load table '%s'", tableName));
                    //AccessConverter.Error(String.format("Could not load table '%s'", tableName));
                    Error(String.format("Could not load table '%s'", tableName), e, methodName);
                }
            });
            
            AccessConverter.progressStatus.resetLine();
            
            //addFooter();
            result = true;
        } catch (IOException e) {
            //lastError.add("Could not fetch tables from the database");
            //AccessConverter.Error("Could not fetch tables from the database");
            Error("Could not fetch tables from the database", e, methodName);
        } catch (SQLException e) {
            //lastError.add(String.format("SQLite database creation/execution error '%s'", sqliteFile.getName()));
            //AccessConverter.Error(String.format("SQLite database creation/execution error '%s'", sqliteFile.getName()));
            Error(String.format("SQLite database creation/execution error '%s'", sqliteFile.getName()), e, methodName);
        }
        
        return result;
    }
    
    private boolean createTable(Table table) throws IOException {
        final String methodName = "createTable";
        List<String> primaryKeys = new ArrayList<>();
        String autoIncrementColumn = "";
        List<String> body = new ArrayList<>();
        
        for (Column column : table.getColumns()) {
            List<String> definitions = new ArrayList<>();
            String type = column.getType().toString().toUpperCase();
            String name = column.getName();

            if (name == "Index") {
                // "Index" is a reserved keyword in SQLite
                name = "Idx";
            }

            definitions.add(String.format("`%s`", name));

            String defaultValue = null;
            Boolean columnRequired = false;

            try {
                var defaultColVal = column.getProperties().getValue(PropertyMap.DEFAULT_VALUE_PROP, null);

                if (defaultColVal != null) {
                    defaultValue = Utils.datetimeDefaultValue(defaultColVal.toString(), "CURRENT_TIMESTAMP");
                    defaultValue = Utils.removeQuotation(defaultColVal.toString());
                }

                columnRequired = ((Boolean)column.getProperties().getValue(PropertyMap.REQUIRED_PROP, false));
            } catch (IOException e) {}

            var columnTypeDef = getColumnType(type, defaultValue);
            var columnType = columnTypeDef.getLeft();
            var useDefaultValue = columnTypeDef.getMiddle();
            // var columnIsNumeric = columnTypeDef.getRight();

            definitions.add(columnType);
            
            if (column.isAutoNumber()) {
                primaryKeys.add(name);

                if (autoIncrementColumn.isEmpty()) {
                    autoIncrementColumn = name;
                    definitions.add("PRIMARY KEY AUTOINCREMENT");
                }
            } else {
                if (columnRequired) {
                    definitions.add("NOT NULL");
                }

                if (useDefaultValue != null) {
                    definitions.add("DEFAULT " + useDefaultValue);
                } else if (columnRequired) {
                    definitions.add("DEFAULT NULL");
                }
            }

            body.add(String.join(" ", definitions));
        }

        if (!primaryKeys.isEmpty()) {
            List<String> primaryKeyList = primaryKeys.stream().map(s -> "`" + s + "`").collect(Collectors.toList());
    
            if (primaryKeys.size() > 1 && !autoIncrementColumn.isEmpty()) {
                body.add(String.format("UNIQUE (%s)", String.join(",", primaryKeyList)));
            }
        } else if (!autoIncrementColumn.isEmpty()) {
            body.add(String.format("PRIMARY KEY (%s)", autoIncrementColumn));
        }

        // Make relationships

        for (Relationship rel: this.db.getRelationships(table)) {
            if (!table.getName().equals(rel.getToTable().getName())) {
                continue;
            }

            var fromColumns = Utils.quoteSqlNames(rel.getFromColumns().stream().map(Column::getName).collect(Collectors.toList()));
            var toColumns = Utils.quoteSqlNames(rel.getToColumns().stream().map(Column::getName).collect(Collectors.toList()));

            body.add(
                String.format(
                    "FOREIGN KEY (%s) REFERENCES `%s` (%s)",
                    String.join(", ", toColumns),
                    rel.getFromTable().getName(),
                    String.join(", ", fromColumns)
                )
            );
        }

        List<String> statements = new ArrayList<>();
        
        statements.add(String.format("CREATE TABLE `%s` (%s)", table.getName(), String.join(", \n", body)));

        // Make indexes

        for (Index idx: table.getIndexes()) {
            if (idx.isPrimaryKey()) {
                // PK is already handled
                continue;
            }

            var columnNames = idx.getColumns().stream().map(c -> "`" + c.getName() + "`").collect(Collectors.toList());

            statements.add(
                String.format(
                    "%s INDEX `%s` ON `%s` (%s)",
                    idx.isUnique() ? "CREATE UNIQUE" : "CREATE",
                    idx.getName(),
                    table.getName(),
                    String.join(", ", columnNames)
                )
            );
        }

        // Execute SQL statements

        for (String sql : statements) {
            try {
                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate(sql);
                }
            } catch (SQLException e) {
                Error(String.format("Could not execute statement on table '%s'", table.getName()), e, methodName, sql);
                return false;
            }
        }

        return true;
    }

    private String getDefaultValue(String dataDefaultValue, String genDefaultValue, Boolean quotetion) {
        var value = dataDefaultValue != null ? dataDefaultValue : genDefaultValue;

        if (value == null) {
            return value;
        }

        return quotetion == true ? "'" + value + "'" : value;
    }

    private Triple<String, String, Boolean> getColumnType(String type, String defaultValue) {
        return switch (type) {
            case "BYTE", "INT", "LONG" ->
                Triple.of("INTEGER", getDefaultValue(defaultValue, "0", false), true);
            case "FLOAT" ->
                Triple.of("FLOAT", getDefaultValue(defaultValue, "0", false), true);
            case "DOUBLE" ->
                Triple.of("DOUBLE", getDefaultValue(defaultValue, "0", false), true);
            case "NUMERIC" ->
                Triple.of("DECIMAL(28,0)", getDefaultValue(defaultValue, "0", false), true);
            case "MONEY" ->
                Triple.of("DECIMAL(15,4)", getDefaultValue(defaultValue, "0", false), true);
            case "BOOLEAN" ->
                Triple.of("TINYINT", getDefaultValue(defaultValue, "0", false), true);
            case "SHORT_DATE_TIME" ->
                Triple.of("DATETIME", getDefaultValue(defaultValue, "0000-00-00 00:00:00", true), false);
            case "MEMO" ->
                Triple.of("TEXT", null, null);
            case "GUID" ->
                Triple.of("VARCHAR(50)", getDefaultValue(defaultValue, "{00000000-0000-0000-0000-000000000000}", true), false);
            case "TEXT" ->
                Triple.of("VARCHAR(255)", getDefaultValue(defaultValue, "", true), false);
            case "BINARY", "OLE" ->
                Triple.of("BLOB", null, null);
            default ->
                Triple.of("VARCHAR(255)", getDefaultValue(defaultValue, "", true), false);
        };
    }
    
    private boolean insertData(Table table) {
        final String methodName = "insertData";
        boolean result = false;
        
        if (table.getRowCount() == 0)
            return true;
        
        String tableName = table.getName();
        TextStringBuilder sql = new TextStringBuilder();
        TextStringBuilder insertHeader = new TextStringBuilder();
        
        insertHeader.append("INSERT INTO `%s` (", tableName);
        boolean isFirst = true;
        
        for (Column column : table.getColumns()) {
            if (!isFirst)
                insertHeader.append(", ");
            else
                isFirst = false;
            insertHeader.append("`%s`", column.getName());
        }
        
        insertHeader.append(") VALUES (");

        boolean isFirstColumn;
        int autoIncrement = -1;
        boolean hasAutoIncrement = false;
        
        Statement statement = null;
        
        try {
            connection.setAutoCommit(true);
            statement = connection.createStatement();
            
            for (Row row : table) {
                isFirstColumn = true;
                sql.append(insertHeader);

                for (Column column : table.getColumns()) {
                    String type = column.getType().toString().toUpperCase();
                    String name = column.getName();

                    if (!isFirstColumn)
                        sql.append(", ");
                    else
                        isFirstColumn = false;

                    try {
                        switch (type) {
                            case "BYTE": {
                                sql.append(row.getByte(name).byteValue());
                                break;
                            }
                            case "INT": {
                                sql.append(row.getShort(name).shortValue());
                                break;
                            }
                            case "LONG": {
                                if (column.isAutoNumber()) {
                                    hasAutoIncrement = true;
                                    autoIncrement = Math.max(autoIncrement, row.getInt(name));
                                }
                                sql.append(row.getInt(name).intValue());
                                break;
                            }
                            case "FLOAT": {
                                sql.append(Globals.defaultIfNullFloat(row.getFloat(name)));
                                break;
                            }
                            case "DOUBLE": {
                                sql.append(Globals.defaultIfNullDouble(row.getDouble(name)));
                                break;
                            }
                            case "NUMERIC":
                            case "MONEY": {
                                sql.append(Globals.defaultIfNullBigDecimal(row.getBigDecimal(name)));
                                break;
                            }
                            case "BOOLEAN": {
                                sql.append(row.getBoolean(name) ? 1 : 0);
                                break;
                            }
                            case "SHORT_DATE_TIME": {
                                LocalDateTime value = row.getLocalDateTime(name);

                                if (value == null) {
                                    sql.append("NULL");
                                } else {
                                    DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                                    sql.append("'%s'", value.format(format));
                                }

                                break;
                            }
                            case "MEMO":
                            case "GUID":
                            case "TEXT": {
                                sql.append("'%s'", Utils.escapeSingleQuotes(row.getString(name)));
                            	break;
                            }
                            case "BINARY":
                            case "OLE": {
                                byte[] data = row.getBytes(name);
                                if (data.length > 0) {
                                    sql.append("x'%s'", Hex.encodeHexString(data));

                                    // final String hexAlphabet = "0123456789ABCDEF";
                                    // sql.append("x'");
                                    // for (byte b: data) {
                                    //     sql.append(hexAlphabet.charAt((b & 0xF0) >> 4)).append(hexAlphabet.charAt((b & 0x0F)));
                                    // }
                                    // sql.append("'");
                                } else {
                                    sql.append("NULL");
                                }
                                break;
                            }
                            case "COMPLEX_TYPE":
                            default: {
                                sql.append("NULL");
                                break;
                            }
                        }
                    } catch (NullPointerException e) {
                        sql.append("NULL");
                    }
                }

                sql.append(")");
                
                try {
                    statement.executeUpdate(sql.build());
                    AccessConverter.progressStatus.step();
                } catch(SQLException e) {
                    Error(String.format("Could not insert to table '%s'; Continue to next row", table.getName()), e, methodName, sql.build());
                }
                
                sql.clear();
                
                //if(AccessConverter.progressStatus.currentTableCurrentRow > 10) break;
            }
            
            //connection.commit();
            
            if (hasAutoIncrement) {
                //System.out.printf("Adding autoIncrement %s", autoIncrement);
                statement.executeUpdate(String.format("UPDATE SQLITE_SEQUENCE SET seq = %d WHERE name = '%s'", autoIncrement, tableName));
                //connection.commit();
            }
            
            result = true;
        } catch (SQLException e) {
            //lastError.add(String.format("Could not create table '%s'", table.getName()));
            //AccessConverter.Error(String.format("Could not insert to table '%s'", table.getName()));
            //Error(String.format("Could not insert to table '%s'", table.getName()), e, methodName, sql.build());
            Error(String.format("Could not create statement for table '%s'", table.getName()), e, methodName);
            result = false;
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    //lastError.add("Could not close connection statement");
                    //AccessConverter.Error("Could not close connection statement");
                    Error("Could not close connection statement", e, methodName);
                    result = false;
                }
            }
            
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                //lastError.add("Could not restore connection auto commit");
                //AccessConverter.Error("Could not restore connection auto commit");
                Error("Could not restore connection auto commit", e, methodName);
                result = false;
            }
        }
        
        return result;
    }
}
