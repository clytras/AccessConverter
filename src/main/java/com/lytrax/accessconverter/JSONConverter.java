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

import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.complex.Attachment;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
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
    private FileWriter writer;

    public JSONConverter(Args args, Database db, FileWriter writer) {
        this.args = args;
        this.db = db;
        this.writer = writer;
    }

    public void writeJson() {
        Map<String, Object> properties = new HashMap<String, Object>(1);
        properties.put(JsonGenerator.PRETTY_PRINTING, true);
        JsonWriter jsonWriter = Json.createWriterFactory(properties).createWriter(writer);
        jsonWriter.write(json.build());
        jsonWriter.close();
    }

    public boolean toJson() {
        boolean result = false;
        final String methodName = "toJson";
        json = Json.createArrayBuilder();

        try {
            Set<String> tableNames = db.getTableNames();

            tableNames.forEach((tableName) -> {
                try {
                    boolean isDataAssoc = args.GetOption("json-data", "assoc").equals("assoc");
                    Table table = db.getTable(tableName);
                    AccessConverter.progressStatus.startTable(table);

                    JsonObjectBuilder jsonTable = Json.createObjectBuilder();
                    JsonArrayBuilder jsonColumns = Json.createArrayBuilder();

                    jsonTable.add("name", tableName);

                    if (args.HasFlag("json-columns")) {
                        for (Column column : table.getColumns()) {
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

                    for (Row row : table) {
                        for (Column column : table.getColumns()) {
                            if (isDataAssoc) {
                                addToJson(jsonDataObject, column, row);
                            } else {
                                addToJson(jsonDataArray, column, row);
                            }
                        }

                        if (isDataAssoc) {
                            jsonRows.add(jsonDataObject);
                        } else {
                            jsonRows.add(jsonDataArray);
                        }

                        AccessConverter.progressStatus.step();
                    }

                    jsonTable.add("data", jsonRows);
                    json.add(jsonTable);

                    AccessConverter.progressStatus.endTable();
                } catch (IOException e) {
                    Error(String.format("Could not load table '%s'", tableName), e, methodName);
                }
            });

            AccessConverter.progressStatus.resetLine();
            result = true;
        } catch (IOException e) {
            Error("Could not fetch tables from the database", e, methodName);
        }

        return result;
    }

    private <T, U> void addData(T json, String name, U data) {
        if (json instanceof JsonArrayBuilder) {
            if (data == null) {
                ((JsonArrayBuilder)json).addNull();
            } else {
                if (data instanceof JsonArrayBuilder) {
                    ((JsonArrayBuilder)json).add((JsonArrayBuilder)data);
                } else if (data instanceof JsonObjectBuilder) {
                    ((JsonArrayBuilder)json).add((JsonObjectBuilder)data);
                } else if (data instanceof Boolean) {
                    ((JsonArrayBuilder)json).add((Boolean)data);
                } else if (data instanceof Integer) {
                    ((JsonArrayBuilder)json).add((Integer)data);
                } else if (data instanceof Long) {
                    ((JsonArrayBuilder)json).add((Long)data);
                } else if (data instanceof Float) {
                    ((JsonArrayBuilder)json).add((Float)data);
                } else if (data instanceof Short) {
                    ((JsonArrayBuilder)json).add((Short)data);
                } else if (data instanceof Byte) {
                    ((JsonArrayBuilder)json).add((Byte)data);
                } else if (data instanceof Double) {
                    ((JsonArrayBuilder)json).add((Double)data);
                } else if (data instanceof BigDecimal) {
                    ((JsonArrayBuilder)json).add((BigDecimal)data);
                } else {
                    ((JsonArrayBuilder)json).add((String)data);
                }
            }
        } else if (json instanceof JsonObjectBuilder) {
            if (data == null) {
                ((JsonObjectBuilder)json).addNull(name);
            } else {
                if (data instanceof JsonArrayBuilder) {
                    ((JsonObjectBuilder)json).add(name, (JsonArrayBuilder)data);
                } else if (data instanceof JsonObjectBuilder) {
                    ((JsonObjectBuilder)json).add(name, (JsonObjectBuilder)data);
                } else if (data instanceof Boolean) {
                    ((JsonObjectBuilder)json).add(name, (Boolean)data);
                } else if (data instanceof Integer) {
                    ((JsonObjectBuilder)json).add(name, (Integer)data);
                } else if (data instanceof Long) {
                    ((JsonObjectBuilder)json).add(name, (Long)data);
                } else if (data instanceof Float) {
                    ((JsonObjectBuilder)json).add(name, (Float)data);
                } else if (data instanceof Short) {
                    ((JsonObjectBuilder)json).add(name, (Short)data);
                } else if (data instanceof Byte) {
                    ((JsonObjectBuilder)json).add(name, (Byte)data);
                } else if (data instanceof Double) {
                    ((JsonObjectBuilder)json).add(name, (Double)data);
                } else if (data instanceof BigDecimal) {
                    ((JsonObjectBuilder)json).add(name, (BigDecimal)data);
                } else {
                    ((JsonObjectBuilder)json).add(name, (String)data);
                }
            }
        }
    }

    private void addToJson(Object json, Column column, Row row) {
        var type = column.getType().toString().toUpperCase();
        var name = column.getName();
        var tableName = column.getTable().getName();

        switch (type) {
            case "INT": {
                addData(json, name, row.getShort(name));
                break;
            }
            case "LONG": {
                addData(json, name, row.getInt(name));
                break;
            }
            case "BYTE": {
                addData(json, name, row.getByte(name));
                break;
            }
            case "FLOAT": {
                Object value = row.get(name);
                addData(json, name, Globals.floatValue(value, column));
                break;
            }
            case "DOUBLE": {
                addData(json, name, row.getDouble(name));
                break;
            }
            case "NUMERIC":
            case "MONEY": {
                addData(json, name, row.getBigDecimal(name));
                break;
            }
            case "BOOLEAN": {
                addData(json, name, row.getBoolean(name));
                break;
            }
            case "SHORT_DATE_TIME": {
                LocalDateTime value = row.getLocalDateTime(name);

                if (value == null) {
                    addData(json, name, null);
                } else {
                    DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    addData(json, name, value.format(format));
                }

                break;
            }
            case "BINARY": {
                byte[] data = row.getBytes(name);

                if (data.length > 0) {
                    addData(json, name, Base64.getEncoder().encodeToString(data));
                } else {
                    addData(json, name, null);
                }

                break;
            }
            case "OLE": {
                var fileValue = new FileValue(args, Globals.OUTPUT_MYSQL, this);

                try {
                    if (fileValue.handleOle(column, row, row.getBlob(name))) {
                        var jsonArrayBuilder = fileValue.getRecordsJsonArrayBuilder();
                        addData(json, name, jsonArrayBuilder);
                    } else {
                        addData(json, name, null);
                    }
                } catch (IOException e) {
                    addData(json, name, null);
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
                                var jsonArrayBuilder = fileValue.getRecordsJsonArrayBuilder();
                                addData(json, name, jsonArrayBuilder);
                            } else {
                                addData(json, name, null);
                            }
                        } else {
                            addData(json, name, null);
                        }
                    } catch (IOException ex) {
                        addData(json, name, null);
                        Error(
                            String.format(
                                "Count not fetch attachments for column '%s' (%s) in table '%s'",
                                name, row.getId().hashCode(), tableName
                            )
                        );
                    }
                } else {
                    addData(json, name, null);
                }

                break;
            }
            case "TEXT":
            case "MEMO":
            case "GUID": {
                try {
                    addData(json, name, row.getString(name));
                } catch (Exception e) {
                    addData(json, name, null);
                }

                break;
            }
            default: {
                addData(json, name, null);
            }
        }
    }
}
