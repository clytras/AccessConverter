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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArrayBuilder;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.complex.Attachment;
import com.healthmarketscience.jackcess.util.OleBlob;
import com.healthmarketscience.jackcess.util.OleBlob.SimplePackageContent;

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
 */
public class FileValue {
    public static final String SCOPE_ATTACHMENTS = "attachments";
    public static final String SCOPE_OLE = "ole";

    class FileAlreadyExistsException extends Exception {
        public FileAlreadyExistsException(String errorMessage) {
            super(errorMessage);
        }
    }

    static class ValueJsonRecord {
        public static final String MESSAGE_ERROR = "error";
        public static final String MESSAGE_WARNING = "warning";

        public String name;
        public String type;
        public String path;
        public Boolean isRelativePath;
        public byte[] data;
        public Long size;
        public String message;
        public String messageSeverity;

        public ValueJsonRecord() {}

        public ValueJsonRecord(String name, String type, String path, Boolean isRelativePath, Long size) {
            this.name = name;
            this.type = type;
            this.path = path;
            this.isRelativePath = isRelativePath;
            this.data = null;
            this.size = size;
            this.message = null;
            this.messageSeverity = null;
        }

        public ValueJsonRecord(String name, String type, byte[] data, Long size) {
            this.name = name;
            this.type = type;
            this.path = null;
            this.isRelativePath = null;
            this.data = data;
            this.size = size;
            this.message = null;
            this.messageSeverity = null;
        }

        public ValueJsonRecord(String name, byte[] data, Long size) {
            this.name = name;
            this.type = null;
            this.path = null;
            this.isRelativePath = null;
            this.data = data;
            this.size = size;
            this.message = null;
            this.messageSeverity = null;
        }

        public ValueJsonRecord(String name, String type, String message, String messageSeverity) {
            this.name = name;
            this.type = type;
            this.path = null;
            this.isRelativePath = null;
            this.data = null;
            this.message = message;
            this.messageSeverity = messageSeverity;
        }
    }

    public static class ValueJsonRecordAdapter implements JsonSerializer<ValueJsonRecord> {
        @Override
        public JsonElement serialize(ValueJsonRecord src, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject obj = new JsonObject();

            obj.addProperty("name", src.name);
            obj.addProperty("type", src.type);

            if (src.size != null) {
                obj.addProperty("size", src.size);
            }

            if (src.path != null) {
                obj.addProperty("path", src.path);
            }

            if (src.isRelativePath != null) {
                obj.addProperty("isRelativePath", src.isRelativePath);
            }

            if (src.message != null) {
                obj.addProperty(
                    src.messageSeverity != null ? src.messageSeverity : "info",
                    src.message
                );
            }

            if (src.data != null) {
                obj.addProperty("data", Base64.getEncoder().encodeToString(src.data));
            }

            return obj;
        }
    }

    public Args args;
    public String output;
    public Converter converter;
    public Boolean saveToFilesystem;
    public Boolean storeInline;
    public Boolean onlyReference;
    public Boolean saveToAbsolutePath;
    public Boolean overwriteExistingFiles;
    public List<ValueJsonRecord> records = new ArrayList<>();

    public FileValue(Args args, String output, Converter converter) {
        this.args = args;
        this.output = output;
        this.converter = converter;
        this.saveToFilesystem = args.GetOption("files-mode").startsWith("file");
        this.storeInline = args.GetOption("files-mode").equals("inline");
        this.onlyReference = args.GetOption("files-mode").equals("reference");
        this.saveToAbsolutePath = args.GetOption("files-mode").endsWith("absolute");
        this.overwriteExistingFiles = args.GetFlag("overwrite-existing-files");

        if (!saveToFilesystem && !storeInline && !onlyReference) {
            this.onlyReference = true;
        }
    }

    private String getGetAbsoluteRootPath() {
        File dbFile = new File(args.GetOption("access-file"));
        return Paths.get(dbFile.getAbsolutePath()).normalize().getParent().toString();
    }

    public Boolean handleAttachments(Column column, Row row, List<Attachment> attachments) {
        var tableName = column.getTable().getName();
        var columnName = column.getName();
        var rowId = row.getId();
        var baseName = FilenameUtils.getBaseName(args.GetOption("access-file")) + "-files";
        var basePath = Paths.get(baseName, tableName, columnName, SCOPE_ATTACHMENTS);
        var absoluteRootPath = getGetAbsoluteRootPath();

        records.clear();

        if (!createFilesPath(absoluteRootPath, basePath)) {
            return false;
        }

        for (Attachment attachment : attachments) {
            var name = attachment.getFileName();
            var fileBaseName = FilenameUtils.getBaseName(name);
            var fileExtension = FilenameUtils.getExtension(name);
            var type = attachment.getFileType();

            try {
                var data = attachment.getFileData();
                var filename = String.format("%s-[%s].%s", fileBaseName, rowId.hashCode(), fileExtension);
                var relativePath = Paths.get(basePath.toString(), filename);
                var absolutePath = Paths.get(absoluteRootPath.toString(), relativePath.toString());

                if (saveToFilesystem) {
                    saveFileData(absolutePath.toString(), relativePath.toString(), data);
                    records.add(
                        new ValueJsonRecord(
                            name,
                            type,
                            saveToAbsolutePath ? absolutePath.toString() : relativePath.toString(),
                            saveToAbsolutePath,
                            Long.valueOf(data.length)
                        )
                    );
                } else if (storeInline) {
                    records.add(
                        new ValueJsonRecord(
                            name,
                            type,
                            data,
                            Long.valueOf(data.length)
                        )
                    );
                } else {
                    records.add(
                        new ValueJsonRecord(
                            name,
                            type,
                            null,
                            Long.valueOf(data.length)
                        )
                    );
                }
            } catch (IOException | SecurityException e) {
                records.add(new ValueJsonRecord(name, type, e.getMessage(), ValueJsonRecord.MESSAGE_ERROR));
            } catch (FileAlreadyExistsException e) {
                records.add(new ValueJsonRecord(name, type, e.getMessage(), ValueJsonRecord.MESSAGE_WARNING));
            }
        }

        return true;
    }

    public Boolean handleOle(Column column, Row row, OleBlob oleBlob) {
        var tableName = column.getTable().getName();
        var columnName = column.getName();
        var rowId = row.getId();
        var baseName = FilenameUtils.getBaseName(args.GetOption("access-file")) + "-files";
        var basePath = Paths.get(baseName, tableName, columnName, SCOPE_OLE);
        var absoluteRootPath = getGetAbsoluteRootPath();
        String name = "<unprocessed>";

        records.clear();

        if (!createFilesPath(absoluteRootPath, basePath)) {
            return false;
        }

        try {
            var oleType = oleBlob.getContent().getType();

            if (oleType.name() != "SIMPLE_PACKAGE") {
                converter.Error(
                    String.format(
                        "Unsupported OLE type '%s' for column '%s' (%s) in table '%s'",
                        oleType.name(),
                        columnName,
                        rowId,
                        tableName
                    )
                );

                return false;
            }

            SimplePackageContent content = (SimplePackageContent) oleBlob.getContent();
            name = content.getFileName();
            var fileBaseName = FilenameUtils.getBaseName(name);
            var fileExtension = FilenameUtils.getExtension(name);

            var data = content.getStream();
            var filename = String.format("%s-[%s].%s", fileBaseName, rowId.hashCode(), fileExtension);
            var relativePath = Paths.get(basePath.toString(), filename);
            var absolutePath = Paths.get(absoluteRootPath.toString(), relativePath.toString());

            if (saveToFilesystem) {
                saveFileStream(absolutePath.toString(), relativePath.toString(), data);
                records.add(
                    new ValueJsonRecord(
                        name,
                        null,
                        saveToAbsolutePath ? absolutePath.toString() : relativePath.toString(),
                        saveToAbsolutePath,
                        content.length()
                    )
                );
            } else if (storeInline) {
                records.add(
                    new ValueJsonRecord(
                        name,
                        null,
                        data.readAllBytes(),
                        content.length()
                    )
                );
            } else {
                records.add(
                    new ValueJsonRecord(
                        name,
                        null,
                        content.length()
                    )
                );
            }
        } catch (IOException | SecurityException e) {
            records.add(new ValueJsonRecord(name, null, e.getMessage(), ValueJsonRecord.MESSAGE_ERROR));
        } catch (FileAlreadyExistsException e) {
            records.add(new ValueJsonRecord(name, null, e.getMessage(), ValueJsonRecord.MESSAGE_WARNING));
        }

        return true;
    }

    private Boolean createFilesPath(String rootPath, Path basePath) {
        if (saveToFilesystem) {
            try {
                Files.createDirectories(Paths.get(rootPath, basePath.toString()).normalize());
            } catch (IOException e) {
                converter.Error(String.format("Could not create directory '%s'", rootPath), e);
                return false;
            }
        }

        return true;
    }

    public String getRecordsJson() {
        return new GsonBuilder()
            // .setPrettyPrinting()
            .registerTypeAdapter(ValueJsonRecord.class, new ValueJsonRecordAdapter())
            .create()
            .toJson(records);
    }

    public JsonArrayBuilder getRecordsJsonArrayBuilder() {
        var json = new GsonBuilder()
            .registerTypeAdapter(ValueJsonRecord.class, new ValueJsonRecordAdapter())
            .create()
            .toJson(records);
        var jsonReader =  Json.createReader(new StringReader(json));
        var builder = Json.createArrayBuilder();
        jsonReader.readArray().forEach(builder::add);

        return builder;
    }

    private void saveFileData(String absolutePath, String relativePath, byte[] data)
        throws FileAlreadyExistsException, IOException, SecurityException {
        var file = new File(absolutePath);

        deleteExistingFile(file, relativePath);

        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(data);
        }
    }

    private void saveFileStream(String absolutePath, String relativePath, InputStream data)
        throws FileAlreadyExistsException, IOException, SecurityException {
        var file = new File(absolutePath);
        deleteExistingFile(file, relativePath);
        FileUtils.copyInputStreamToFile(data, file);
    }

    private void deleteExistingFile(File file, String path) throws FileAlreadyExistsException {
        if (file.exists()) {
            if (!overwriteExistingFiles) {
                throw new FileAlreadyExistsException(String.format("File already exists and it will not be overwritten '%s'", path));
            }

            file.delete();
        }
    }
}
