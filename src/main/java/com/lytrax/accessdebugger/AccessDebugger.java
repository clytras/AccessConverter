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
package com.lytrax.accessdebugger;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.JackcessException;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.PropertyMap.Property;
import com.healthmarketscience.jackcess.complex.ComplexColumnInfo;
import com.healthmarketscience.jackcess.Relationship;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
 */
public class AccessDebugger {
    public static void main(String[] args) throws Exception {
        String filePath = args[0];
        String tableName = args[1];

        Log("DB: " + filePath);
        Log("Table: " + tableName);
        System.out.println();

        File dbFile = new File(filePath);

        if (!dbFile.exists()) {
            Log("DB file does not exists");
            return;
        }

        debugInfo(filePath, tableName);
    }

    public static void Log(String str) {
        System.out.println(str);
    }

    public static void debugInfo(String filePath, String tableName) throws SQLException {
        Log(String.format("Access File %s", filePath));
        
        Database db = null;
        
        try {
            File dbFile = new File(filePath);
            db = DatabaseBuilder.open(dbFile);
            
            try {
                Table table = db.getTable(tableName);
                
                for (Column column : table.getColumns()) {
                    String columnName = column.getName();

                    try {
                        Log(
                            "Column \"" + columnName + "\"\n" +
                            "  type: " + column.getType().toString() + "\n"
                        );
                        Log("  sql type: " + column.getSQLType() + "\n");
                    } catch (JackcessException ex) {
                        Log("ERROR: " + ex.getMessage());
                    }

                    if (column.getComplexInfo() != null) {
                        Log(
                            "  complex info: " + column.getComplexInfo() + "\n" +
                            "  complex type: " + column.getComplexInfo().getType().name() + "\n"
                        );

                        // for (ComplexColumnInfo cci: column.getComplexInfo()) {

                        // }
                    }

                    for (Property prop: column.getProperties()) {
                        Log(
                            "  prop: " + prop.getName() +
                            ", type: " + prop.getType().toString() +
                            ", value: " + prop.getValue().toString());
                    }

                    Log(
                        "  default value: " + column.getProperties().getValue(PropertyMap.DEFAULT_VALUE_PROP, null)
                    );
                }
                
                for (Index idx: table.getIndexes()) {
                    Log(
                        "Index: " + idx.getName() +
                        "  PK: " + (idx.isPrimaryKey() ? 'Y' : 'N') +
                        "  FK: " + (idx.isForeignKey() ? 'Y' : 'N') +
                        "  required: " + (idx.isRequired() ? 'Y' : 'N') +
                        "  unique: " + (idx.isUnique() ? 'Y' : 'N')
                    );
                    
                    for (Index.Column col: idx.getColumns()) {
                        Log(
                            "    index column: " + col.getName()
                        );
                    }
                    
                }
                
                for (Relationship rel: db.getRelationships(table)) {
                    Log(
                        "Relationship: " + rel.getName() +
                        "  from table: " + rel.getFromTable().getName() +
                        "     from columns: " + rel.getFromColumns().toString() +
                        "  to table: " + rel.getToTable().getName() +
                        "     to columns: " + rel.getToColumns().toString()
                    );
                }
                
                for (Row row : table) {
                    for (Column column : table.getColumns()) {
                        String columnName = column.getName();
                        Object value = row.get(columnName);
                        
                        String cls = "";
                        try {
                            cls = value.getClass().toString();
                        } catch(NullPointerException e) {
                            
                        }

                        Log("Column " + columnName + " = '" + value + "' (" + column.getType() + ") (" + column.getLength() + " : p - "+ column.getPrecision() + ") (" + cls + ")");
                    }
                }
                
            } catch (IOException e) {
                System.out.println("Error: " + e.toString());
            }
        } catch (IOException e) {
            Log("Can't open Access file");
        }
    }
}
