/*
 * The MIT License
 *
 * Copyright 2020 Christos Lytras <christos.lytras@gmail.com>.
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

import com.healthmarketscience.jackcess.*;
import java.io.IOException;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.json.*;
import javax.json.stream.JsonGenerator;

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
 */
public class JSONConverter extends Converter {
    public Database db;
    public Args args;
    public List<String> lastError = new ArrayList<>();
    public JsonArrayBuilder json;
    private JsonBuilderFactory factory;
    
    public JSONConverter(Args args, Database db) {
        this.args = args;
        this.db = db;
    }
    
    public String getPrettyPrinted() {
        Map<String, Object> properties = new HashMap<String, Object>(1);
        properties.put(JsonGenerator.PRETTY_PRINTING, true);
        factory = Json.createBuilderFactory(properties);
        StringWriter stringWriter = new StringWriter();
        JsonWriter jsonWriter = Json.createWriterFactory(properties).createWriter(stringWriter);
        jsonWriter.write(json.build());
        jsonWriter.close();
        return stringWriter.toString();
    }
    
    public boolean toJson() {
        boolean result = false;
        final String methodName = "toJson";
        json = Json.createArrayBuilder();
        
        try {
            Set<String> tableNames = db.getTableNames();
            //Json.createArrayBuilder().
            //List<JsonObject> jsonTables;
            
            tableNames.forEach((tableName) -> {
                try {
                    boolean isDataAssoc = args.GetOption("json-data", "assoc").equals("assoc");
                    Table table = db.getTable(tableName);
                    AccessConverter.progressStatus.startTable(table);

                    JsonObjectBuilder jsonTable = Json.createObjectBuilder();
                    JsonArrayBuilder jsonColumns = Json.createArrayBuilder();
                    
                    jsonTable.add("name", tableName);
                    
                    if(args.HasFlag("json-columns")) {
                        for(Column column : table.getColumns()) {
                            JsonObjectBuilder jsonColumn = Json.createObjectBuilder();
                            jsonColumn.add("name", column.getName());
                            jsonColumn.add("type", column.getType().toString());
                            jsonColumn.add("size", column.getLength());
                            jsonColumns.add(jsonColumn);
                        }

                        jsonTable.add("columns", jsonColumns);
                    }
                    
                    JsonArrayBuilder jsonRows = Json.createArrayBuilder();
                    JsonArrayBuilder jsonDataArray = Json.createArrayBuilder();
                    JsonObjectBuilder jsonDataObject = Json.createObjectBuilder();

                    for(Row row : table) {
                        for(Column column : table.getColumns()) {
                            //String columnName = column.getName();

                            if(isDataAssoc)
                                addToJson(jsonDataObject, column, row);
                            else
                                addToJson(jsonDataArray, column, row);
                        }

                        if(isDataAssoc)
                            jsonRows.add(jsonDataObject);
                        else
                            jsonRows.add(jsonDataArray);
                    }

                    jsonTable.add("data", jsonRows);
                    json.add(jsonTable);
                    
                    AccessConverter.progressStatus.endTable();
                } catch(IOException e) {
                    //lastError.add(String.format("Could not load table '%s'", tableName));
                    Error(String.format("Could not load table '%s'", tableName), e, methodName);
                }
            });
            
            AccessConverter.progressStatus.resetLine();
            result = true;
        } catch(IOException e) {
            //lastError.add("Could not fetch tables from the database");
            Error("Could not fetch tables from the database", e, methodName);
        }
        
        return result;
    }
    
    private void addToJson(JsonArrayBuilder jsonData, Column column, Row row) {
        String type = column.getType().toString().toUpperCase();
        String name = column.getName();
        //Object value = row.get(name);
        
        switch(type) {
            case "INT":
                //jsonData.add(Short.parseShort(value.toString()));
                jsonData.add(row.getShort(name));
            break;
            case "LONG":
                //jsonData.add(Integer.parseInt(value.toString()));
                jsonData.add(row.getInt(name));
            break;
            case "BYTE":
                //jsonData.add(Byte.parseByte(value.toString()));
                jsonData.add(row.getByte(name));
            break;
            case "FLOAT":
                Object value = row.get(name);
                jsonData.add(Globals.floatValue(value, column));
                //jsonData.add(row.getFloat(name));
            break;
            case "DOUBLE":
                //jsonData.add(Double.parseDouble(value.toString()));
                jsonData.add(row.getDouble(name));
            break;
            case "NUMERIC":
            case "MONEY":
                //jsonData.add(new BigDecimal(value.toString()));
                jsonData.add(row.getBigDecimal(name));
            break;
            case "BOOLEAN":
                //jsonData.add(Boolean.parseBoolean(value.toString()));
                jsonData.add(row.getBoolean(name));
            break;
            case "SHORT_DATE_TIME":
                try {
                    Date d = row.getDate(name);
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd kk:mm:ss");
                    jsonData.add(format.format(d));
                } catch(Exception e) {
                    jsonData.add("");
                }
            break;
            case "TEXT":
            case "MEMO":
            case "GUID":
                //jsonData.add(value.toString());
                try {
                    jsonData.add(row.getString(name));
                } catch(Exception e) {
                    jsonData.add("");
                }
            break;
            default:
                jsonData.addNull();
        }
    }
    
    private void addToJson(JsonObjectBuilder jsonData, Column column, Row row) {
        String type = column.getType().toString().toUpperCase();
        String name = column.getName();
        //Object value = row.get(name);
        
        if(name == null) return;
        
        try {
        
            switch(type) {
                case "INT":
                    //jsonData.add(name, Short.parseShort(value.toString()));
                    jsonData.add(name, row.getShort(name));
                    break;
                case "LONG":
                    //jsonData.add(name, Integer.parseInt(value.toString()));
                    jsonData.add(name, row.getInt(name));
                    break;
                case "BYTE":
                    //jsonData.add(name, Byte.parseByte(value.toString()));
                    jsonData.add(name, row.getByte(name));
                    break;
                case "FLOAT":
                    Object objectValue = row.get(name);
                    jsonData.add(name, Globals.floatValue(objectValue, column));
                    //jsonData.add(name, row.getFloat(name));
                    break;
                case "DOUBLE":
                    //jsonData.add(name, Double.parseDouble(value.toString()));
                    jsonData.add(name, row.getDouble(name));
                    break;
                case "NUMERIC":
                case "MONEY":
                    //jsonData.add(name, new BigDecimal(value.toString()));
                    jsonData.add(name, row.getBigDecimal(name));
                    break;
                case "BOOLEAN":
                    //jsonData.add(name, Boolean.parseBoolean(value.toString()));
                    jsonData.add(name, row.getBoolean(name));
                    break;
                case "SHORT_DATE_TIME":
                    Date d = row.getDate(name);
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    jsonData.add(name, format.format(d));
                    break;
                case "TEXT":
                case "MEMO":
                case "GUID":
                    //jsonData.add(name, value.toString());
                    jsonData.add(name, row.getString(name));
                    break;
                default:
                    jsonData.addNull(name);
                    break;
            }
        } catch(NullPointerException e) {
            jsonData.addNull(name);
        }
    }
}
