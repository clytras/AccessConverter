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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.text.TextStringBuilder;

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
 */
public class SQLiteConverter extends Converter {
    public Database db;
    public Args args;
    public File sqliteFile;
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
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
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
                    Error(String.format("Could not load table '%s'", tableName), e, methodName);
                }
            });

            AccessConverter.progressStatus.resetLine();

            result = true;
        } catch (IOException e) {
            Error("Could not fetch tables from the database", e, methodName);
        } catch (SQLException e) {
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

        for (Relationship rel : this.db.getRelationships(table)) {
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

        statements.add(
            String.format(
                "CREATE TABLE `%s` (%s)",
                table.getName(),
                String.join(", \n", body)
            )
        );

        // Make indexes

        for (Index idx : table.getIndexes()) {
            if (idx.isPrimaryKey()) {
                // PK is already handled
                continue;
            }

            var columnNames = idx.getColumns().stream().map(c -> "`" + c.getName() + "`").collect(Collectors.toList());

            statements.add(
                String.format(
                    "%s INDEX `%s` ON `%s` (%s)",
                    idx.isUnique() ? "CREATE UNIQUE" : "CREATE",
                    String.format("%s_%s", table.getName(), idx.getName()),
                    table.getName(),
                    String.join(", ", columnNames)
                )
            );
        }

        // Execute SQL statements

        for (String sql : statements) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(sql);
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
            case "BINARY" ->
                Triple.of("BLOB", null, null);
            case "COMPLEX_TYPE", "OLE" ->
                Triple.of("TEXT", null, null);
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
        List<String> columnNames = table.getColumns()
            .stream()
            .map(Column::getName)
            .collect(Collectors.toList());

        sql.append(
            "INSERT INTO `%s` (%s) VALUES (%s)",
            tableName,
            String.join(", ", Utils.quoteSqlNames(columnNames)),
            String.join(", ", new ArrayList<String>(Collections.nCopies(table.getColumns().size(), "?")))
        );

        int autoIncrement = -1;
        boolean hasAutoIncrement = false;

        try (
            Statement statement = connection.createStatement();
            PreparedStatement ps = connection.prepareStatement(sql.build());
        ) {
            connection.setAutoCommit(true);
            sql.clear();

            int batchCount = 0;

            for (Row row : table) {
                for (Column column : table.getColumns()) {
                    var type = column.getType().toString().toUpperCase();
                    var name = column.getName();
                    var columnIndex = column.getColumnIndex() + 1;

                    try {
                        switch (type) {
                            case "BYTE": {
                                ps.setByte(columnIndex, row.getByte(name).byteValue());
                                break;
                            }
                            case "INT": {
                                ps.setInt(columnIndex, row.getShort(name).shortValue());
                                break;
                            }
                            case "LONG": {
                                if (column.isAutoNumber()) {
                                    hasAutoIncrement = true;
                                    autoIncrement = Math.max(autoIncrement, row.getInt(name));
                                }

                                ps.setInt(columnIndex, row.getInt(name).intValue());
                                break;
                            }
                            case "FLOAT": {
                                ps.setFloat(columnIndex, Globals.defaultIfNullFloat(row.getFloat(name)));
                                break;
                            }
                            case "DOUBLE": {
                                ps.setDouble(columnIndex, Globals.defaultIfNullDouble(row.getDouble(name)));
                                break;
                            }
                            case "NUMERIC":
                            case "MONEY": {
                                ps.setBigDecimal(columnIndex, Globals.defaultIfNullBigDecimal(row.getBigDecimal(name)));
                                break;
                            }
                            case "BOOLEAN": {
                                ps.setBoolean(columnIndex, row.getBoolean(name));
                                break;
                            }
                            case "SHORT_DATE_TIME": {
                                LocalDateTime value = row.getLocalDateTime(name);

                                if (value == null) {
                                    ps.setNull(columnIndex, Types.DATE);
                                } else {
                                    DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                                    ps.setTimestamp(columnIndex, Timestamp.valueOf(value.format(format)));
                                }

                                break;
                            }
                            case "MEMO":
                            case "GUID":
                            case "TEXT": {
                                ps.setString(columnIndex, row.getString(name));
                                break;
                            }
                            case "BINARY": {
                                byte[] data = row.getBytes(name);

                                if (data.length > 0) {
                                    ps.setBytes(columnIndex, data);
                                } else {
                                    ps.setNull(columnIndex, Types.BLOB);
                                }

                                break;
                            }
                            case "OLE": {
                                var fileValue = new FileValue(args, Globals.OUTPUT_SQLITE, this);

                                try {
                                    if (fileValue.handleOle(column, row, row.getBlob(name))) {
                                        var json = fileValue.getRecordsJson();
                                        ps.setString(columnIndex, json);
                                    } else {
                                        ps.setNull(columnIndex, Types.BLOB);
                                    }
                                } catch (IOException e) {
                                    ps.setNull(columnIndex, Types.BLOB);
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
                                            var fileValue = new FileValue(args, Globals.OUTPUT_SQLITE, this);

                                            if (fileValue.handleAttachments(column, row, attachments)) {
                                                var json = fileValue.getRecordsJson();
                                                ps.setString(columnIndex, json);
                                            } else {
                                                ps.setNull(columnIndex, Types.BLOB);
                                            }
                                        } else {
                                            ps.setNull(columnIndex, Types.BLOB);
                                        }
                                    } catch (IOException ex) {
                                        ps.setNull(columnIndex, Types.BLOB);
                                        Error(
                                            String.format(
                                                "Count not fetch attachments for column '%s' (%s) in table '%s'",
                                                name, row.getId().hashCode(), tableName
                                            )
                                        );
                                    }
                                } else {
                                    ps.setNull(columnIndex, Types.BLOB);
                                }

                                break;
                            }
                            default: {
                                ps.setNull(columnIndex, Types.BLOB);
                                break;
                            }
                        }
                    } catch (NullPointerException e) {
                        ps.setNull(columnIndex, Types.BLOB);
                    }
                }

                ps.addBatch();

                if (++batchCount == 500) {
                    ps.executeBatch();
                    batchCount = 0;
                }

                AccessConverter.progressStatus.step();
            }

            if (batchCount != 500) {
                ps.executeBatch();
            }

            if (hasAutoIncrement) {
                statement.executeUpdate(String.format("UPDATE SQLITE_SEQUENCE SET seq = %d WHERE name = '%s'", autoIncrement, tableName));
            }

            result = true;
        } catch (SQLException e) {
            Error(String.format("Could not create statement for table '%s'", table.getName()), e, methodName);
            result = false;
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                Error("Could not restore connection auto commit", e, methodName);
                result = false;
            }
        }

        return result;
    }
}
