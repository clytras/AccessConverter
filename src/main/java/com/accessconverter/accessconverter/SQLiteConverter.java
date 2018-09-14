/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.accessconverter.accessconverter;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
import java.util.Set;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//import org.apache.commons.lang3.text.StrBuilder;
import org.apache.commons.text.TextStringBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
//import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 *
 * @author Christos Lytras <christos.lytras@gmail.com>
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
                    createTable(table);
                    insertData(table);
                    AccessConverter.progressStatus.endTable();
                } catch(IOException e) {
                    //lastError.add(String.format("Could not load table '%s'", tableName));
                    //AccessConverter.Error(String.format("Could not load table '%s'", tableName));
                    Error(String.format("Could not load table '%s'", tableName), e, methodName);
                }
            });
            
            AccessConverter.progressStatus.resetLine();
            
            //addFooter();
            result = true;
        } catch(IOException e) {
            //lastError.add("Could not fetch tables from the database");
            //AccessConverter.Error("Could not fetch tables from the database");
            Error("Could not fetch tables from the database", e, methodName);
        } catch(SQLException e) {
            //lastError.add(String.format("SQLite database creation/execution error '%s'", sqliteFile.getName()));
            //AccessConverter.Error(String.format("SQLite database creation/execution error '%s'", sqliteFile.getName()));
            Error(String.format("SQLite database creation/execution error '%s'", sqliteFile.getName()), e, methodName);
        }
        
        return result;
    }
    
    private boolean createTable(Table table) {
        final String methodName = "createTable";
        TextStringBuilder sql = new TextStringBuilder();
        sql.append(String.format("CREATE TABLE `%s` (", table.getName()));
        
        boolean isFirst = true;
        
        for(Column column : table.getColumns()) {
            String name = column.getName();
            String type = column.getType().toString().toUpperCase();
            //short length = column.getLength();
            
            if(!isFirst)
                sql.appendln(",");
            else
                isFirst = false;
            
            switch(type) {
                case "BYTE":
                case "INT":
                case "LONG":
                    if(column.isAutoNumber()) {
                        sql.append(String.format("`%s` INTEGER PRIMARY KEY AUTOINCREMENT", name));
                    } else {
                        sql.append(String.format("`%s` INT NOT NULL DEFAULT 0", name));
                    }
                    break;
                case "FLOAT":
                    sql.append(String.format("`%s` FLOAT NOT NULL DEFAULT 0", name));
                    break;
                case "DOUBLE":
                    sql.append(String.format("`%s` DOUBLE NOT NULL DEFAULT 0", name));
                    break;
                case "NUMERIC":
                    sql.append(String.format("`%s` DECIMAL(28,0) NOT NULL DEFAULT 0", name));
                    break;
                case "MONEY":
                    sql.append(String.format("`%s` DECIMAL(15,4) NOT NULL DEFAULT 0", name));
                    break;
                case "BOOLEAN":
                    sql.append(String.format("`%s` TINYINT NOT NULL DEFAULT 0", name));
                    break;
                case "SHORT_DATE_TIME":
                    sql.append(String.format("`%s` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00'", name));
                    break;
                case "MEMO":
                    sql.append(String.format("`%s` TEXT", name));
                    break;
                case "GUID":
                    sql.append(String.format("`%s` VARCHAR(50) DEFAULT '{00000000-0000-0000-0000-000000000000}'", name));
                    break;
                case "TEXT":
                default:
                    sql.append(String.format("`%s` VARCHAR(255) DEFAULT ''", name));
                    break;
            }
        }
        
        sql.append(")");
        
        try {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(sql.build());
            }
            return true;
        } catch (SQLException e) {
            //lastError.add(String.format("Could not create table '%s'", table.getName()));
            //AccessConverter.Error(String.format("Could not create table '%s'", table.getName()));
            Error(String.format("Could not create table '%s'", table.getName()), e, methodName, sql.build());
            return false;
        }
    }
    
    private boolean insertData(Table table) {
        final String methodName = "insertData";
        boolean result = false;
        
        if(table.getRowCount() == 0)
            return true;
        
        String tableName = table.getName();
        TextStringBuilder sql = new TextStringBuilder();
        TextStringBuilder insertHeader = new TextStringBuilder();
        
        insertHeader.append(String.format("INSERT INTO `%s` (", tableName));
        boolean isFirst = true;
        
        for(Column column : table.getColumns()) {
            if(!isFirst)
                insertHeader.append(", ");
            else
                isFirst = false;
            insertHeader.append(String.format("`%s`", column.getName()));
        }
        
        insertHeader.append(") VALUES (");

        boolean isFirstColumn;
        int autoIncrement = -1;
        boolean hasAutoIncrement = false;
        
        Statement statement = null;
        
        try {
            //connection.setAutoCommit(false);
            connection.setAutoCommit(true);
            statement = connection.createStatement();
            
            for(Row row : table) {
                isFirstColumn = true;
                sql.append(insertHeader);

                for(Column column : table.getColumns()) {
                    String type = column.getType().toString().toUpperCase();
                    String name = column.getName();

                    if(!isFirstColumn)
                        sql.append(", ");
                    else
                        isFirstColumn = false;

                    try {
                        switch(type) {
                            case "BYTE":
                                sql.append(row.getByte(name).byteValue());
                                break;
                            case "INT":
                                sql.append(row.getShort(name).shortValue());
                                break;
                            case "LONG":
                                if(column.isAutoNumber()) {
                                    hasAutoIncrement = true;
                                    autoIncrement = Math.max(autoIncrement, row.getInt(name));
                                }
                                sql.append(row.getInt(name).intValue());
                                break;
                            case "FLOAT":
                                //Float f = row.getFloat(name);
                                sql.append(Globals.defaultIfNullFloat(row.getFloat(name)));
                                break;
                            case "DOUBLE":
                                //Double dd = row.getDouble(name);
                                sql.append(Globals.defaultIfNullDouble(row.getDouble(name)));
                                break;
                            case "NUMERIC":
                            case "MONEY":
                                //BigDecimal bd = row.getBigDecimal(name);
                                sql.append(Globals.defaultIfNullBigDecimal(row.getBigDecimal(name)));
                                break;
                            case "BOOLEAN":
                                sql.append(row.getBoolean(name) ? 1 : 0);
                                break;
                            case "SHORT_DATE_TIME":
                                Date d = row.getDate(name);
                                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                sql.append(String.format("'%s'", format.format(d)));
                                break;
                            case "MEMO":
                            case "GUID":
                            case "TEXT":
                                sql.append(String.format("'%s'", row.getString(name).replace("'", "''")));
                                break;
                            case "COMPLEX_TYPE":
                            default:
                                sql.append("NULL");
                                break;
                        }
                    } catch(NullPointerException e) {
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
            
            if(hasAutoIncrement) {
                //System.out.printf("Adding autoIncrement %s", autoIncrement);
                statement.executeUpdate(String.format("UPDATE SQLITE_SEQUENCE SET seq = %d WHERE name = '%s'", autoIncrement, tableName));
                //connection.commit();
            }
            
            result = true;
        } catch(SQLException e) {
            //lastError.add(String.format("Could not create table '%s'", table.getName()));
            //AccessConverter.Error(String.format("Could not insert to table '%s'", table.getName()));
            //Error(String.format("Could not insert to table '%s'", table.getName()), e, methodName, sql.build());
            Error(String.format("Could not create statement for table '%s'", table.getName()), e, methodName);
            result = false;
        } finally {
            if(statement != null) {
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
