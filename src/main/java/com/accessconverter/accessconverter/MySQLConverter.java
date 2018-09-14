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
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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
    
    public Database db;
    public Args args;
    //public List<String> lastError = new ArrayList<>();
    public Map<String, AutoIncrement> autoIncrements = new HashMap<>();
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
                    addAutoIncrements();
                    AccessConverter.progressStatus.endTable();
                } catch(IOException e) {
                    //lastError.add(String.format("Could not load table '%s'", tableName));
                    Error(String.format("Could not load table '%s'", tableName), e, methodName);
                }
            });
            
            addFooter();
            AccessConverter.progressStatus.resetLine();
            result = true;
        } catch(IOException e) {
            //lastError.add("Could not fetch tables from the database");
            Error("Could not fetch tables from the database", e, methodName);
        }
        
        return result;
    }

    private void addHeader() {
        sqlDump.appendln(String.format("-- %s", Application.Title));
        sqlDump.appendln(String.format("-- version %s", Application.Version));
        sqlDump.appendln(String.format("-- author %s", Application.Author));
        sqlDump.appendln(String.format("-- %s", Application.Web));
        sqlDump.appendln("--");
        LocalDateTime datetime = LocalDateTime.now();
        Locale locale = new Locale("en", "US");
        sqlDump.appendln(String.format("-- Generation time: %s", datetime.format(DateTimeFormatter.ofPattern("EEE d, yyyy 'at' hh:mm a", locale))));
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
        sqlDump.appendln(String.format("/*!40101 SET NAMES %s */;", charset));
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
    
    private void addTableCreate(Table table) {
        if(args.HasFlag("mysql-drop-tables")) {
            sqlDump.appendln("--");
            sqlDump.appendln(String.format("-- Drop table `%s` if exists", table.getName()));
            sqlDump.appendln("--");
            sqlDump.appendNewLine();
            sqlDump.appendln(String.format("DROP TABLE IF EXISTS `%s`;", table.getName()));
            sqlDump.appendNewLine();
        }
        
        sqlDump.appendln("--");
        sqlDump.appendln(String.format("-- Table structure for table `%s`", table.getName()));
        sqlDump.appendln("--");
        sqlDump.appendNewLine();
        
        sqlDump.appendln(String.format("CREATE TABLE IF NOT EXISTS `%s` (", table.getName()));
        
        boolean isFirst = true;
        
        for(Column column : table.getColumns()) {
            String name = column.getName();
            String type = column.getType().toString().toUpperCase();
            short length = column.getLength();
            
            if(!isFirst)
                sqlDump.appendln(",");
            else
                isFirst = false;
            
            switch(type) {
                case "BYTE":
                case "INT":
                case "LONG":
                    if(column.isAutoNumber()) {
                        AutoIncrement autoIncrement = new AutoIncrement();
                        autoIncrement.tableName = table.getName();
                        autoIncrement.columnName = name;
                        autoIncrements.put(autoIncrement.tableName, autoIncrement);
                        sqlDump.append(String.format("  `%s` INT(10) UNSIGNED NOT NULL", name));
                    } else {
                        switch(length) {
                            case 1:
                                sqlDump.append(String.format("  `%s` TINYINT(3) NOT NULL DEFAULT '0'", name));
                                break;
                            case 2:
                                sqlDump.append(String.format("  `%s` SMALLINT(5) NOT NULL DEFAULT '0'", name));
                                break;
                            case 4:
                                sqlDump.append(String.format("  `%s` INT(10) NOT NULL DEFAULT '0'", name));
                                break;
                            case 6:
                                sqlDump.append(String.format("  `%s` INT(10) NOT NULL DEFAULT '0'", name));
                                break;
                            default:
                                break;
                        }
                    }
                    break;
                case "FLOAT":
                    sqlDump.append(String.format("  `%s` FLOAT NOT NULL DEFAULT '0'", name));
                    break;
                case "DOUBLE":
                    sqlDump.append(String.format("  `%s` DOUBLE NOT NULL DEFAULT '0'", name));
                    break;
                case "NUMERIC":
                    sqlDump.append(String.format("  `%s` DECIMAL(28,0) NOT NULL DEFAULT '0'", name));
                    break;
                case "MONEY":
                    sqlDump.append(String.format("  `%s` DECIMAL(15,4) NOT NULL DEFAULT '0'", name));
                    break;
                case "BOOLEAN":
                    sqlDump.append(String.format("  `%s` TINYINT(3) NOT NULL DEFAULT '0'", name));
                    break;
                case "SHORT_DATE_TIME":
                    sqlDump.append(String.format("  `%s` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00'", name));
                    break;
                case "MEMO":
                    sqlDump.append(String.format("  `%s` TEXT COLLATE %s", name, collate));
                    break;
                case "GUID":
                    sqlDump.append(String.format("  `%s` VARCHAR(50) COLLATE %s DEFAULT '{00000000-0000-0000-0000-000000000000}'", name, collate));
                    break;
                case "TEXT":
                default:
                    sqlDump.append(String.format("  `%s` VARCHAR(255) COLLATE %s DEFAULT ''", name, collate));
                    break;
            }
        }
        
        sqlDump.appendNewLine();
        sqlDump.appendln(String.format(") ENGINE=%s DEFAULT CHARSET=%s COLLATE=%s;", engine, charset, collate));
        sqlDump.appendNewLine();
    }
    
    private void addTableInsert(Table table) {
        if(table.getRowCount() == 0)
            return;
        
        String tableName = table.getName();
        
        sqlDump.appendln("--");
        sqlDump.appendln(String.format("-- Dumping data for table `%s`", tableName));
        sqlDump.appendln("--");
        sqlDump.appendNewLine();
        
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
        
        insertHeader.appendln(") VALUES");
        sqlDump.append(insertHeader);
        boolean isFirstRow = true;
        boolean isFirstColumn;
        int insertRows = 0;
        
        for(Row row : table) {
            if(!isFirstRow)
                sqlDump.append(", ");
            else
                isFirstRow = false;
                
            isFirstColumn = true;
            sqlDump.append("(");
            
            for(Column column : table.getColumns()) {
                String type = column.getType().toString().toUpperCase();
                String name = column.getName();
                
                if(!isFirstColumn)
                    sqlDump.append(", ");
                else
                    isFirstColumn = false;
                
                try {
                    switch(type) {
                        case "BYTE":
                            sqlDump.append(row.getByte(name));
                            break;
                        case "INT":
                            sqlDump.append(row.getShort(name));
                            break;
                        case "LONG":
                            if(column.isAutoNumber() && autoIncrements.containsKey(tableName))
                                autoIncrements.get(tableName).setMaxId(row.getInt(name));
                            sqlDump.append(row.getInt(name));
                            break;
                        case "FLOAT":
                            sqlDump.append(row.getFloat(name));
                            break;
                        case "DOUBLE":
                            sqlDump.append(row.getDouble(name));
                            break;
                        case "NUMERIC":
                        case "MONEY":
                            sqlDump.append(row.getBigDecimal(name));
                            break;
                        case "BOOLEAN":
                            sqlDump.append(row.getBoolean(name) ? 1 : 0);
                            break;
                        case "SHORT_DATE_TIME":
                            Date d = row.getDate(name);
                            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            sqlDump.append(String.format("'%s'", format.format(d)));
                            break;
                        case "MEMO":
                        case "GUID":
                        case "TEXT":
                            sqlDump.append(String.format("'%s'", row.getString(name).replace("'", "''")));
                            break;
                        case "COMPLEX_TYPE":
                        default:
                            sqlDump.append("NULL");
                            break;
                    }
                } catch(NullPointerException e) {
                    sqlDump.append("NULL");
                }
            }
            
            sqlDump.append(")");
            
            if(++insertRows >= maxInsertRows) {
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
        for(AutoIncrement autoIncrement : autoIncrements.values()) {
            sqlDump.appendln("--");
            sqlDump.appendln(String.format("-- Indexes for table `%s`", autoIncrement.tableName));
            sqlDump.appendln("--");
            sqlDump.appendln(String.format("ALTER TABLE `%s`", autoIncrement.tableName));
            sqlDump.appendln(String.format("  ADD PRIMARY KEY (`%s`);", 
                    autoIncrement.columnName));
            
            sqlDump.appendNewLine();
            
            sqlDump.appendln("--");
            sqlDump.appendln(String.format("-- AUTO_INCREMENT for table `%s`", autoIncrement.tableName));
            sqlDump.appendln("--");
            sqlDump.appendln(String.format("ALTER TABLE `%s`", autoIncrement.tableName));
            sqlDump.appendln(String.format("  MODIFY `%s` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=%d;", 
                    autoIncrement.columnName, 
                    autoIncrement.maxId + 1));
        }
        sqlDump.appendNewLine();
    }
}
