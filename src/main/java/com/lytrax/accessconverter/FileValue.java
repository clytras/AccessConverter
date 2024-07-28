package com.lytrax.accessconverter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.apache.commons.io.FilenameUtils;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.complex.Attachment;

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
        public String message;
        public String messageSeverity;

        public ValueJsonRecord() {}

        public ValueJsonRecord(String name, String type, String path, Boolean isRelativePath) {
            this.name = name;
            this.type = type;
            this.path = path;
            this.isRelativePath = isRelativePath;
            this.data = null;
            this.message = null;
            this.messageSeverity = null;
        }

        public ValueJsonRecord(String name, String type, byte[] data) {
            this.name = name;
            this.type = type;
            this.path = null;
            this.isRelativePath = null;
            this.data = data;
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

    public static class ValueJsonRecordAdapter implements JsonSerializer<ValueJsonRecord>, JsonDeserializer<ValueJsonRecord> {
        @Override
        public ValueJsonRecord deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
                throws JsonParseException {
            return new ValueJsonRecord();
        }
    
        @Override
        public JsonElement serialize(ValueJsonRecord src, Type typeOfSrc, JsonSerializationContext context) {
            JsonObject obj = new JsonObject();

            obj.addProperty("name", src.name);
            obj.addProperty("type", src.type);

            if (src.path != null) {
                obj.addProperty("path", src.path);
            }

            if (src.data != null) {
                obj.addProperty("data", Base64.getEncoder().encodeToString(src.data));
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
    
            return obj;
        }
    }

    public Args args;
    public String output;
    public Converter converter;
    public Boolean saveToFilesystem;
    public Boolean saveToAbsolutePath;
    public Boolean overwriteExistingFiles;
    public List<ValueJsonRecord> records = new ArrayList<>();

    public FileValue(Args args, String output, Converter converter) {
        this.args = args;
        this.output = output;
        this.converter = converter;
        this.saveToFilesystem = args.GetOption("files-mode").startsWith("file");
        this.saveToAbsolutePath = args.GetOption("files-mode").endsWith("absolute");
        this.overwriteExistingFiles = args.GetFlag("overwrite-existing-files");
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

        if (saveToFilesystem) {
            try {
                Files.createDirectories(Paths.get(absoluteRootPath, basePath.toString()).normalize());
            } catch (IOException e) {
                converter.Error(String.format("Could not create directory '%s'", absoluteRootPath), e);
                return false;
            }
        }

        for (Attachment attachment : attachments) {
            var name = attachment.getFileName();
            var fileBaseName = FilenameUtils.getBaseName(name);
            var fileExtension = FilenameUtils.getExtension(name);
            var type = attachment.getFileType();

            try {
                var data = attachment.getFileData();
                var filename = String.format("%s-[%s].%s", fileBaseName, rowId.hashCode(), fileExtension);
                // System.out.println(
                //     String.format(
                //         "Attachment: %s, type %s, baseName %s, tableName %s, columnName %s, SCOPE_ATTACHMENTS %s, rowId %s, rowId hash %s",
                //         filename, type, fileBaseName, tableName, columnName, SCOPE_ATTACHMENTS, rowId, rowId.hashCode()
                //     )
                // );
                var relativePath = Paths.get(baseName, tableName, columnName, SCOPE_ATTACHMENTS, filename);
                var absolutePath = Paths.get(absoluteRootPath.toString(), relativePath.toString());

                if (saveToFilesystem) {
                    saveFile(absolutePath.toString(), relativePath.toString(), data);
                    records.add(
                        new ValueJsonRecord(
                            name,
                            type,
                            saveToAbsolutePath ? absolutePath.toString() : relativePath.toString(),
                            saveToAbsolutePath
                        )
                    );
                } else {
                    records.add(
                        new ValueJsonRecord(
                            name,
                            type,
                            data
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

    public String getRecordsJson() {
        return new GsonBuilder()
            .setPrettyPrinting()
            .registerTypeAdapter(ValueJsonRecord.class, new ValueJsonRecordAdapter())
            .create()
            .toJson(records);
    }

    private void saveFile(String absolutePath, String relativePath, byte[] data)
        throws FileAlreadyExistsException, IOException, SecurityException {
        var file = new File(absolutePath);

        if (file.exists()) {
            if (!overwriteExistingFiles) {
                throw new FileAlreadyExistsException(String.format("File already exists and it won't be overwritten '%s'", relativePath));
            }

            file.delete();
        }

        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            outputStream.write(data);
        }
    }
}
