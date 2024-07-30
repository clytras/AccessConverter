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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;
import javax.json.stream.JsonGenerator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author Christos Lytras {@literal <christos.lytras@gmail.com>}
 */
public class AccessConverter {

    public static Args args;
    public static List<LogRecord> logs = new ArrayList<>();
    public static List<ErrorRecord> errors = new ArrayList<>();
    public static String result;
    public static String outputFilename = null;
    public static File outputFile = null;
    public static String logFilename = null;
    public static File logFile = null;
    public static String zipFilename = null;
    public static File zipFile = null;
    public static ProgressStatus progressStatus = null;

    static {
        System.setProperty("org.apache.commons.logging.Log",
                           "org.apache.commons.logging.impl.NoOpLog");
    }

    /**
     * @param cmdArgs the command line arguments
     */
    public static void main(String[] cmdArgs) throws InterruptedException {
        args = new Args(cmdArgs);

        if(CheckCommandArguments()) {
            Run();
        }

        CreateLog();
        Exit();
    }

    public static void Print(String str) {
        System.out.println(str);
    }

    public static void Debug(String str) {
        if (args.GetFlag("debug", false)) {
            System.out.println("DEBUG: " + str);
        }
    }

    public static void Log(String str) {
        logs.add(new LogRecord(str));
        Debug(str);
    }

    public static void Log(String str, String source) {
        logs.add(new LogRecord(str, source));
        Debug(str + " -> " + source);
    }

    public static void Error(String error) {
        errors.add(new ErrorRecord(error));
        Debug(error);
    }

    public static void Error(String error, Exception exception) {
        errors.add(new ErrorRecord(error, exception));
        Debug(error + " -> " + exception.toString());
    }

    public static void Error(String error, Exception exception, String source) {
        errors.add(new ErrorRecord(error, exception, source));
        Debug(error + " : " + exception.toString() + " -> " + source);
    }

    public static void Error(String error, Exception exception, String source, String sql) {
        errors.add(new ErrorRecord(error, exception, source, sql));
        Debug(error + " : " + exception.toString() + " -> " + source + " ~ " + sql);
    }

    public static boolean CheckCommandArguments() {
        if (!args.HasOption("access-file")) {
            Error("No input Access file specified");
            return false;
        }

        if (!args.HasOption("task")) {
            Error("No task specified");
            return false;
        }

        return true;
    }

    public static void Run() {
        result = "";

        try {
            File dbFile = new File(args.GetOption("access-file"));
            Database db = new DatabaseBuilder(dbFile).setReadOnly(true).open();

            progressStatus = new ProgressStatus(db);
            progressStatus.calculateAllRows();

            switch (args.GetOption("task")) {
                case "convert-json": {
                    File tempFile = getTempFile(Globals.OUTPUT_MYSQL);
                    outputFile = getOutputFile("json");

                    try (
                        FileWriter writer = new FileWriter(tempFile)
                    ) {
                        JSONConverter jsonConverter = new JSONConverter(args, db, writer);

                        if (jsonConverter.toJson()) {
                            if (outputFile != null) {
                                jsonConverter.writeJson();
                                writer.close();
                                tempFile.renameTo(outputFile);
                                Log(String.format("JSON file '%s' created successfully", outputFilename));
                                result = "success";
                            }
                        } else {
                            Log(String.format("Could not convert '%s' to JSON", args.GetOption("access-file")));
                        }
                    } catch (IOException ex) {
                        Error(String.format("Could not create JSON data file '%s'", tempFile.getAbsolutePath()), ex);
                    } finally {
                        if (tempFile.exists()) {
                            tempFile.delete();
                        }
                    }

                    break;
                }
                case "convert-mysql-dump": {
                    File tempFile = getTempFile(Globals.OUTPUT_MYSQL);
                    outputFile = getOutputFile("sql");

                    try (
                        SqlFileWriter writer = new SqlFileWriter(tempFile)
                    ) {
                        MySQLConverter mysqlConverter = new MySQLConverter(args, db, writer);

                        if (mysqlConverter.toMySQLDump()) {
                            if (outputFile != null) {
                                writer.close();
                                tempFile.renameTo(outputFile);
                                Log(String.format("MySQL dump file '%s' created successfully", outputFilename));
                                result = "success";
                            }
                        } else {
                            Error(String.format("Could not convert '%s' to MySQL dump file", args.GetOption("access-file")));
                        }
                    } catch (IOException ex) {
                        Error(String.format("Could not create MySQL dump file '%s'", tempFile.getAbsolutePath()), ex);
                    } finally {
                        if (tempFile.exists()) {
                            tempFile.delete();
                        }
                    }

                    break;
                }
                case "convert-sqlite": {
                    File sqliteFile = getOutputFile("sqlite3");

                    if (sqliteFile != null) {
                        SQLiteConverter sqliteConverter = new SQLiteConverter(args, db, sqliteFile);

                        if (sqliteConverter.toSQLiteFile()) {
                            Log(String.format("SQLite database '%s' created successfully", outputFilename));
                            outputFile = sqliteConverter.sqliteFile;
                            result = "success";
                        } else {
                            Error(String.format("Error while creating SQLite database '%s'", outputFilename));
                        }
                    }

                    break;
                }
                default: {
                    Error(String.format("No valid task given '%s'", args.GetOption("task")));
                    break;
                }
            }

            if (
                "success".equals(result) &&
                args.HasFlag("compress") &&
                outputFile != null
            ) {
                Compress();
            }
        } catch (IOException e) {
            Error("Can't open Access file", e);
        }
    }

    private static File getOutputFile(String extension) {
        outputFilename = args.HasOption("output-file")
            ? args.GetOption("output-file")
            : FilenameUtils.getBaseName(args.GetOption("access-file")) + "." + extension;

        File outFile = new File(FilenameUtils.concat(FilenameUtils.getFullPath(args.GetOption("access-file")), outputFilename));

        if (outFile.exists()) {
            try {
                outFile.delete();
            } catch (SecurityException e) {
                Error(String.format("Could not delete existing output file '%s'", outputFilename));
                return null;
            }
        }

        return outFile;
    }

    private static File getTempFile(String scope) {
        try {
            return File.createTempFile("access-converter-" + scope, ".tmp");
        } catch (IOException e) {
            Error("Could not create temporary file", e);
            return null;
        }
    }

    public static void CreateLog() {
        if (args.HasFlag("no-log")) {
            return;
        }

        logFilename = args.HasOption("log-file")
            ? args.GetOption("log-file")
            : FilenameUtils.getBaseName(args.GetOption("access-file")) + ".log.json";
        logFile = new File(FilenameUtils.concat(FilenameUtils.getFullPath(args.GetOption("access-file")), logFilename));

        if (logFile.exists()) {
            try {
                logFile.delete();
            } catch (SecurityException e) {
                Error(String.format("Could not delete existing log file '%s'", logFilename), e);
                logFile = null;
                return;
            }
        }

        JsonObjectBuilder json = Json.createObjectBuilder();
        json.add("generator", Application.Title);
        json.add("version", Application.Version);

        LocalDateTime datetime = LocalDateTime.now();
        Locale locale = new Locale("en", "US");

        json.add("datetime", datetime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", locale)));

        JsonObjectBuilder jsonArguments = Json.createObjectBuilder();
        boolean hasOptions = false;
        boolean hasFlags = false;

        if (!args.options.isEmpty()) {
            hasOptions = true;
            JsonObjectBuilder jsonOptions = Json.createObjectBuilder();
            args.options.keySet().forEach((key) -> {
                jsonOptions.add(key, args.options.get(key));
            });
            jsonArguments.add("options", jsonOptions);
        }

        if (!args.flags.isEmpty()) {
            hasFlags = true;
            JsonArrayBuilder jsonFlags = Json.createArrayBuilder();
            args.flags.keySet().forEach((key) -> {
                jsonFlags.add(key);
            });
            jsonArguments.add("flags", jsonFlags);
        }

        if (hasOptions || hasFlags) {
            json.add("arguments", jsonArguments);
        }

        json.add("result", result);

        if (outputFile != null) {
            json.add("outputFile", outputFile.getAbsolutePath());
        }

        if (zipFile != null) {
            json.add("zipFile", zipFile.getAbsolutePath());
        }

        if (!logs.isEmpty()) {
            JsonArrayBuilder jsonLogs = Json.createArrayBuilder();
            logs.forEach((log) -> {
                jsonLogs.add(log.toJsonObject());
            });
            json.add("logs", jsonLogs);
        }

        if (!errors.isEmpty()) {
            JsonArrayBuilder jsonErrors = Json.createArrayBuilder();
            errors.forEach((error) -> {
                jsonErrors.add(error.toJsonObject());
            });
            json.add("errors", jsonErrors);
        }

        try {
            Map<String, Object> properties = new HashMap<>(1);
            properties.put(JsonGenerator.PRETTY_PRINTING, true);
            StringWriter stringWriter = new StringWriter();

            try (JsonWriter jsonWriter = Json.createWriterFactory(properties).createWriter(stringWriter)) {
                jsonWriter.write(json.build());
            }

            FileUtils.write(logFile, stringWriter.toString(), "UTF-8");
        } catch (IOException e) {
            logFilename = "<error>";
            logFile = null;
        }
    }

    public static void Compress() {
        zipFilename = args.HasOption("zip-file")
            ? args.GetOption("zip-file")
            : FilenameUtils.getBaseName(args.GetOption("access-file")) + ".zip";
        zipFile = new File(FilenameUtils.concat(FilenameUtils.getFullPath(args.GetOption("access-file")), zipFilename));

        if (zipFile.exists()) {
            try {
                zipFile.delete();
            } catch (SecurityException ex) {
                Error(String.format("Could not delete existing zip file '%s'", logFilename), ex);
                return;
            }
        }

        Map<String, String> env = new HashMap<>() {{
            put("create", "true");
        }};
        URI uri = URI.create(String.format("jar:%s", zipFile.toURI().toString()));

        try (FileSystem zipfs = FileSystems.newFileSystem(uri, env)) {
            Path externalTxtFile = Paths.get(outputFile.getAbsolutePath());
            Path pathInZipfile = zipfs.getPath(String.format("/%s", outputFilename));
            // copy a file into the zip file
            Files.copy(externalTxtFile, pathInZipfile, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            Error(String.format("Cannot create ZIP file '%s'", zipFilename), ex);
            zipFile = null;
        }
    }

    public static void Exit() {
        switch (args.GetOption("output-result", "normal")) {
            case "json": {
                ExitJson(false);
                break;
            }
            case "json-pretty": {
                ExitJson(true);
                break;
            }
            default: {
                ExitNormal();
            }
        }
    }

    public static void ExitNormal() {
        System.out.println(String.format("Result: %s", "success".equals(result) ? "Success" : "Failure"));

        if (outputFile != null) {
            System.out.println(String.format("Output file: %s", outputFile.getAbsolutePath()));
        }

        if (logFile != null) {
            System.out.println(String.format("Log file: %s", logFile.getAbsolutePath()));
        }

        if (zipFile != null && zipFile.exists()) {
            System.out.println(String.format("Zip file: %s", zipFile.getAbsolutePath()));
        }
    }

    public static void ExitJson(boolean pretyPrint) {
        JsonObjectBuilder json = Json.createObjectBuilder();

        if (result.length() == 0) {
            result = "fail";
        }

        json.add("result", result);

        if (outputFile != null) {
            json.add("outputFile", outputFile.getAbsolutePath());
        }

        if (logFile != null) {
            json.add("logFile", logFile.getAbsolutePath());
        }

        if (zipFile != null && zipFile.exists()) {
            json.add("zipFile", zipFile.getAbsolutePath());
        }

        if (pretyPrint) {
            Map<String, Object> properties = new HashMap<>(1);
            properties.put(JsonGenerator.PRETTY_PRINTING, true);
            StringWriter stringWriter = new StringWriter();

            try (JsonWriter jsonWriter = Json.createWriterFactory(properties).createWriter(stringWriter)) {
                jsonWriter.write(json.build());
            }

            System.out.println(stringWriter.toString());
        } else {
            System.out.print(json.build().toString());
        }
    }

    public static void debugArgs(String[] args) {
        for (String arg : args) {
            System.out.println(arg);
        }
    }

    public static class LogRecord {
        public String text;
        public String source = "AccessConverter";
        public String sql = "";

        public LogRecord(String text) {
            this.text = text;
        }

        public LogRecord(String text, String source) {
            this.text = text;
            this.source += ":" + source;
        }

        public LogRecord(String text, String source, String sql) {
            this.text = text;
            this.source += ":" + source;
            this.sql = sql;
        }

        public JsonObjectBuilder toJsonObject() {
            JsonObjectBuilder json = Json.createObjectBuilder();
            json.add("text", text);
            json.add("source", source);

            if (!sql.isEmpty()) {
                json.add("sql", sql);
            }

            return json;
        }
    }

    public static class ErrorRecord extends LogRecord {
        public Exception exception = null;

        public ErrorRecord(String text) {
            super(text);
        }

        public ErrorRecord(String text, Exception exception) {
            super(text);
            this.exception = exception;
        }

        public ErrorRecord(String text, Exception exception, String source) {
            super(text, source);
            this.exception = exception;
        }

        public ErrorRecord(String text, Exception exception, String source, String sql) {
            super(text, source);
            this.exception = exception;
            this.sql = sql;
        }

        @Override
        public JsonObjectBuilder toJsonObject() {
            JsonObjectBuilder json = super.toJsonObject();

            if (exception != null) {
                json.add("exception", exception.toString());
            }

            return json;
        }
    }
}
