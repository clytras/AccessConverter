/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package accessconverter;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.FileSystem;
import java.util.*;
import com.healthmarketscience.jackcess.*;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;
import javax.json.stream.JsonGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * @author Christos Lytras <christos.lytras@gmail.com>
 */
public class AccessConverter {

    public static Args args;
    public static List<LogRecord> logs = new ArrayList<>();
    public static List<ErrorRecord> errors = new ArrayList<>();
    public static String lastError;
    public static String result;
    public static String outputFilename = null;
    public static File outputFile = null;
    public static String logFilename = null;
    public static File logFile = null;
    public static String zipFilename = null;
    public static File zipFile = null;
    
    /**
     * @param cmdArgs the command line arguments
     */
    public static void main(String[] cmdArgs) {

        args = new Args(cmdArgs);
        
        //debugTestDb();
        
        
        //debugArgs(args.options.keySet().toArray(new String[0]));
        //debugArgs(args.flags.keySet().toArray(new String[0]));
        
        if(CheckCommandArguments())
            Run();

        CreateLog();
        
        Exit();
    }
    
    public static void Print(String str) {
        //if(args.GetOption("result", "std").equals("std"))
            System.out.println(str);
    }
    
    public static void Log(String str) {
        logs.add(new LogRecord(str));
    }
    
    public static void Log(String str, String source) {
        logs.add(new LogRecord(str, source));
    }
    
    public static void Error(String error) {
        //lastError = error;
        //Log("ERROR: " + error);
        errors.add(new ErrorRecord(error));
    }
    
    public static void Error(String error, Exception exception) {
        errors.add(new ErrorRecord(error, exception));
    }
    
    public static void Error(String error, Exception exception, String source) {
        errors.add(new ErrorRecord(error, exception, source));
    }
    
    public static void Error(String error, Exception exception, String source, String sql) {
        errors.add(new ErrorRecord(error, exception, source, sql));
    }
    
    public static boolean CheckCommandArguments() {
        if(!args.HasOption("access-file")) {
            Error("No input Access file specified");
            return false;
        }
        
        if(!args.HasOption("task")) {
            Error("No task specified");
            return false;
        }
        
        /*switch(args.GetOption("task")) {
            case "convert-json":
                if(!args.HasFlag(""))
        }*/
        
        return true;
    }
    
    public static void Run() {
        result = "";
        
        try {
            File dbFile = new File(args.GetOption("access-file"));
            Database db = DatabaseBuilder.open(dbFile);
            
            switch(args.GetOption("task")) {
                case "convert-json":
                    JSONConverter jsonConverter = new JSONConverter(args, db);
                    
                    if(jsonConverter.toJson()) {
                        outputFile = getOutputFile("json");

                        if(outputFile != null) {
                            FileUtils.write(outputFile, jsonConverter.getPrettyPrinted(), "UTF-8");
                            Log(String.format("JSON file '%s' created successfully", outputFilename));
                            result = "success";
                        }
                    } else {
                        Log(String.format("Could not convert '%s' to JSON", args.GetOption("access-file")));
                    }
                    break;
                case "convert-mysql-dump":
                    MySQLConverter mysqlConverter = new MySQLConverter(args, db);
                    
                    if(mysqlConverter.toMySQLDump()) {
                        outputFile = getOutputFile("sql");

                        if(outputFile != null) {
                            FileUtils.write(outputFile, mysqlConverter.sqlDump.build(), "UTF-8");
                            Log(String.format("MySQL dump file '%s' created successfully", outputFilename));
                            result = "success";
                        }
                    } else {
                        Error(String.format("Could not convert '%s' to MySQL dump file", args.GetOption("access-file")));
                    }
                    break;
                case "convert-sqlite":
                    File sqliteFile = getOutputFile("sqlite3");
                    if(sqliteFile != null) {
                        SQLiteConverter sqliteConverter = new SQLiteConverter(args, db, sqliteFile);
                        if(sqliteConverter.toSQLiteFile()) {
                            Log(String.format("SQLite database '%s' created successfully", outputFilename));
                            outputFile = sqliteConverter.sqliteFile;
                            result = "success";
                        } else {
                            Error(String.format("Error while creating SQLite database '%s'", outputFilename));
                        }
                    }
                    break;
                default:
                    Error(String.format("No valid task given '%s'", args.GetOption("task")));
                    break;
            }
            
            if("success".equals(result) && args.HasFlag("compress") && outputFile != null)
                Compress();
        } catch(IOException e) {
            Error("Can't open Access file");
        }
    }
    
    private static File getOutputFile(String extension) {
        outputFilename = args.HasOption("output-file") ? 
                args.GetOption("output-file") : 
                FilenameUtils.getBaseName(args.GetOption("access-file")) + "." + extension;
        
        File outFile = new File(FilenameUtils.concat(FilenameUtils.getFullPath(args.GetOption("access-file")), outputFilename));
        if(outFile.exists()) {
            try {
                outFile.delete();
            } catch(SecurityException e) {
                Error(String.format("Could not delete existing output file '%s'", outputFilename));
                return null;
            }
        }
        
        return outFile;
    }
    
    public static void CreateLog() {
        if(args.HasFlag("no-log"))
            return;
        
        logFilename = args.HasOption("log-file") ? 
                args.GetOption("log-file") : 
                FilenameUtils.getBaseName(args.GetOption("access-file")) + ".log.json";

        logFile = new File(FilenameUtils.concat(FilenameUtils.getFullPath(args.GetOption("access-file")), logFilename));
        if(logFile.exists()) {
            try {
                logFile.delete();
            } catch(SecurityException e) {
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
        
        if(!args.options.isEmpty()) {
            hasOptions = true;
            JsonObjectBuilder jsonOptions = Json.createObjectBuilder();
            args.options.keySet().forEach((key) -> {
                jsonOptions.add(key, args.options.get(key));
            });
            jsonArguments.add("options", jsonOptions);
        }
        
        if(!args.flags.isEmpty()) {
            hasFlags = true;
            JsonArrayBuilder jsonFlags = Json.createArrayBuilder();
            args.flags.keySet().forEach((key) -> {
                jsonFlags.add(key);
            });
            jsonArguments.add("flags", jsonFlags);
        }
        
        if(hasOptions || hasFlags)
            json.add("arguments", jsonArguments);
        
        json.add("result", result);
        if(outputFile != null)
            json.add("outputFile", outputFile.getAbsolutePath());
        
        if(zipFile != null)
            json.add("zipFile", zipFile.getAbsolutePath());
        
        if(!logs.isEmpty()) {
            JsonArrayBuilder jsonLogs = Json.createArrayBuilder();
            logs.forEach((log) -> {
                jsonLogs.add(log.toJsonObject());
            });
            json.add("logs", jsonLogs);
        }
        
        if(!errors.isEmpty()) {
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
        zipFilename = args.HasOption("zip-file") ? 
                args.GetOption("zip-file") : 
                FilenameUtils.getBaseName(args.GetOption("access-file")) + ".zip";

        zipFile = new File(FilenameUtils.concat(FilenameUtils.getFullPath(args.GetOption("access-file")), zipFilename));
        if(zipFile.exists()) {
            try {
                zipFile.delete();
            } catch(SecurityException ex) {
                Error(String.format("Could not delete existing zip file '%s'", logFilename), ex);
                return;
            }
        }
        
        Map<String, String> env = new HashMap<>(); 
        env.put("create", "true");

        //URI uri = URI.create(String.format("jar:file:/%s", zipFile.getAbsolutePath()));
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
        switch(args.GetOption("output-result", "normal")) {
            case "json":
                ExitJson(false);
                break;
            case "json-pretty":
                ExitJson(true);
                break;
            default:
                ExitNormal();
        }
    }
    
    public static void ExitNormal() {
        System.out.println(String.format("Result: %s", "success".equals(result) ? "Success" : "Failure"));
        
        if(outputFile != null)
            System.out.println(String.format("Output file: %s", outputFile.getAbsolutePath()));
        
        if(logFile != null)
            System.out.println(String.format("Log file: %s", logFile.getAbsolutePath()));
        
        if(zipFile != null && zipFile.exists())
            System.out.println(String.format("Zip file: %s", zipFile.getAbsolutePath()));
    }
    
    public static void ExitJson(boolean pretyPrint) {
        JsonObjectBuilder json = Json.createObjectBuilder();
        
        if(result.length() == 0)
            result = "fail";
        
        json.add("result", result);
        if(outputFile != null)
            json.add("outputFile", outputFile.getAbsolutePath());
        
        if(logFile != null)
            json.add("logFile", logFile.getAbsolutePath());
        
        if(zipFile != null && zipFile.exists())
            json.add("zipFile", zipFile.getAbsolutePath());
        
        if(pretyPrint) {
            Map<String, Object> properties = new HashMap<>(1);
            properties.put(JsonGenerator.PRETTY_PRINTING, true);
            StringWriter stringWriter = new StringWriter();
            try (JsonWriter jsonWriter = Json.createWriterFactory(properties).createWriter(stringWriter)) {
                jsonWriter.write(json.build());
            }

            System.out.println(stringWriter.toString());
        } else         
            System.out.print(json.build().toString());
    }
    
    public static void debugArgs(String[] args) {
        for(String arg : args) {
            System.out.println(arg);
        }
    }
    
    public static void debugTestDb() {
        Log(String.format("Access File %s", args.GetOption("access-file")));
        
        Database db = null;
        Table tbl;
        
        try {
            File dbFile = new File(args.GetOption("access-file"));
            db = DatabaseBuilder.open(dbFile);
            
            try {
                Table table = db.getTable("testTable");
                
                for(Row row : table) {
                    for(Column column : table.getColumns()) {
                        String columnName = column.getName();
                        Object value = row.get(columnName);
                        
                        String cls = "";
                        try {
                            cls = value.getClass().toString();
                        } catch(NullPointerException e) {
                            
                        }
                        
                        

                        System.out.println("Column " + columnName + " = '" + value + "' (" + column.getType() + ") (" + column.getLength() + " : p - "+ column.getPrecision() + ") (" + cls + ")");
                        
                        //System.out.println("Column " + columnName + "(" + column.getType() + "): "
                        //                   + value + " (" + value.getClass() + ")");
                    }
                }
                
            } catch(IOException e) {
                System.out.println("Error: " + e.toString());
            }
        } catch(IOException e) {
            Error("Can't open Access file");
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
            if(!sql.isEmpty())
                json.add("sql", sql);
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
            if(exception != null)
                json.add("exception", exception.toString());
            return json;
        }
    }
}
