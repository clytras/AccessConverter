package com.lytrax.accessconverter.cli;

import com.lytrax.accessconverter.extract.SchemaExtractor;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.profile.DataProfile;
import com.lytrax.accessconverter.profile.DataProfiler;
import com.lytrax.accessconverter.report.Issue;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.report.Severity;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.target.ComplexTables;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.ConvertOptions.OnTableError;
import com.lytrax.accessconverter.target.json.JsonPlan;
import com.lytrax.accessconverter.target.json.JsonPlanner;
import com.lytrax.accessconverter.target.json.JsonVerifier;
import com.lytrax.accessconverter.target.mysql.MySqlDialect;
import com.lytrax.accessconverter.target.mysql.MySqlOptions;
import com.lytrax.accessconverter.target.mysql.MySqlPlan;
import com.lytrax.accessconverter.target.mysql.MySqlPlanner;
import com.lytrax.accessconverter.target.mysql.MySqlVerifier;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan;
import com.lytrax.accessconverter.target.sqlite.SqlitePlanner;
import com.lytrax.accessconverter.target.sqlite.SqliteVerifier;
import com.lytrax.accessconverter.verify.SourceSnapshot;
import com.lytrax.accessconverter.verify.SourceSnapshot.TableSnapshot;
import com.lytrax.accessconverter.verify.VerifyResult;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(
        name = "verify",
        mixinStandardHelpOptions = true,
        sortOptions = false,
        description = {
            "Compare a converted output with its Access source (09): the schema it should have, and every value.",
            "The conversion is planned again from the source, so pass the same options the conversion used.",
            "A MySQL or MariaDB output is the database the dump was imported into: give --jdbc-url (and"
                    + " --jdbc-driver, since no JDBC driver is bundled) instead of <output>.",
            "Without <output>, print the source side: each table's row count and a digest of its canonical rows."
        })
final class VerifyCommand implements Callable<Integer> {

    /** Every SQLite file starts with this, including the trailing NUL. */
    private static final byte[] SQLITE_HEADER = "SQLite format 3\0".getBytes(StandardCharsets.US_ASCII);

    @Parameters(index = "0", paramLabel = "<input>", description = "Access database (.mdb, .accdb)")
    Path input;

    @Parameters(index = "1", arity = "0..1", paramLabel = "<output>", description = "Converted output to compare")
    Path output;

    @Option(
            names = "--to",
            paramLabel = "<target>",
            description = "MySQL/MariaDB: the dialect the dump was written for, mysql or mariadb (default: the"
                    + " server's own).")
    Target to;

    @Mixin
    JdbcOptions jdbc;

    @Mixin
    PlanOptions plan;

    @Mixin
    SourceOptions source;

    @Spec
    CommandSpec spec;

    @Override
    public Integer call() throws IOException {
        Main.requireFile(input);
        if (jdbc.given()) {
            if (output != null) {
                throw new ParameterException(spec.commandLine(), "give either <output> or --jdbc-url, not both");
            }
            return compareDatabase();
        }
        return output == null ? snapshot() : compare();
    }

    /** Compares a SQLite file or a JSON export with the source. */
    private Integer compare() throws IOException {
        if (Files.isDirectory(output)) {
            if (!JsonVerifier.isNdjson(output)) {
                spec.commandLine()
                        .getErr()
                        .println("error: " + output + " is a directory without the schema.json of an ndjson export");
                return ExitCodes.FAILED;
            }
            return compareJson();
        }
        Main.requireFile(output);
        if (JsonVerifier.isJson(output)) {
            return compareJson();
        }
        if (!isSqlite(output)) {
            spec.commandLine()
                    .getErr()
                    .println("error: " + output + " is neither a SQLite file nor a JSON export; for a MySQL or"
                            + " MariaDB dump, import it and pass --jdbc-url instead");
            return ExitCodes.FAILED;
        }
        OpenOptions options = source.toOpenOptions();
        Issues issues = new Issues();
        ConvertOptions convert = plan.convertOptions(OnTableError.FAIL, ConvertOptions.DEFAULT_BATCH_ROWS);
        VerifyResult result;
        SchemaModel model;
        try (AccessSource db = AccessSource.open(input, options, issues)) {
            model = SchemaExtractor.extract(db, plan.extractOptions(), issues);
            SchemaModel expanded = ComplexTables.expand(model, convert);
            DataProfile profile = convert.profile() ? DataProfiler.profile(db, expanded) : null;
            // The issues of planning again are the conversion's, not the verification's: they go nowhere
            SqlitePlan planned =
                    SqlitePlanner.plan(expanded, profile, convert, plan.sqliteOptions(false), new Issues());
            result = SqliteVerifier.verify(db, planned, output);
        }
        return print(model, output + " (SQLite)", result, issues);
    }

    /** Compares a JSON document or ndjson directory with the source; the file says how its values are spelled. */
    private Integer compareJson() throws IOException {
        plan.check(Target.json, spec.commandLine());
        OpenOptions options = source.toOpenOptions();
        Issues issues = new Issues();
        ConvertOptions convert = plan.convertOptions(OnTableError.FAIL, ConvertOptions.DEFAULT_BATCH_ROWS);
        VerifyResult result;
        SchemaModel model;
        try (AccessSource db = AccessSource.open(input, options, issues)) {
            model = SchemaExtractor.extract(db, plan.extractOptions(), issues);
            DataProfile profile = convert.profile() ? DataProfiler.profile(db, model) : null;
            // The issues of planning again are the conversion's, not the verification's: they go nowhere
            JsonPlan planned = JsonPlanner.plan(model, profile, convert, new Issues());
            result = JsonVerifier.verify(db, planned, output);
        }
        return print(model, output + " (JSON)", result, issues);
    }

    /** Compares a MySQL or MariaDB database, loaded from a dump, with the source (09). */
    private Integer compareDatabase() throws IOException {
        OpenOptions options = source.toOpenOptions();
        Issues issues = new Issues();
        ConvertOptions convert = plan.convertOptions(OnTableError.FAIL, ConvertOptions.DEFAULT_BATCH_ROWS);
        VerifyResult result;
        SchemaModel model;
        String server;
        try (Connection db = jdbc.connect();
                AccessSource access = AccessSource.open(input, options, issues)) {
            server = version(db);
            MySqlDialect dialect = to != null ? to.dialect() : MySqlDialect.ofVersion(server);
            if (dialect == null) {
                throw new ParameterException(spec.commandLine(), "--to must be mysql or mariadb with --jdbc-url");
            }
            plan.check(Target.of(dialect), spec.commandLine());
            model = SchemaExtractor.extract(access, plan.extractOptions(), issues);
            SchemaModel expanded = ComplexTables.expand(model, convert);
            DataProfile profile = convert.profile() ? DataProfiler.profile(access, expanded) : null;
            MySqlOptions mysql = plan.mysqlOptions(dialect, false, null, MySqlOptions.DEFAULT_BATCH_BYTES, false);
            // The issues of planning again are the conversion's, not the verification's: they go nowhere
            MySqlPlan planned = MySqlPlanner.plan(expanded, profile, convert, mysql, new Issues());
            result = MySqlVerifier.verify(
                    access, planned, db, jdbc.dumpDirectory == null ? Path.of("") : jdbc.dumpDirectory);
        } catch (SQLException e) {
            throw new IOException("reading the database failed: " + e.getMessage(), e);
        }
        return print(model, jdbc.url + " (" + server + ")", result, issues);
    }

    private static String version(Connection db) throws SQLException {
        try (Statement statement = db.createStatement();
                ResultSet rows = statement.executeQuery("SELECT VERSION()")) {
            return rows.next() ? rows.getString(1) : "";
        }
    }

    private Integer print(SchemaModel model, String outputName, VerifyResult result, Issues issues) {
        PrintWriter out = spec.commandLine().getOut();
        StringBuilder text = new StringBuilder();
        text.append("Source: ")
                .append(model.source().fileName())
                .append(" (")
                .append(model.source().fileFormat())
                .append(")\nOutput: ")
                .append(outputName)
                .append('\n');
        for (VerifyResult.TableRows table : result.tables()) {
            text.append("  ")
                    .append(table.table())
                    .append(": ")
                    .append(table.actual())
                    .append(" rows\n");
        }
        if (result.matches()) {
            // Exit 1 (02: success with warnings) when reading the source warns, as the conversion did; say so
            List<Issue> warnings = issues.list().stream()
                    .filter(i -> i.severity() == Severity.WARNING)
                    .toList();
            if (warnings.isEmpty()) {
                text.append("verify: the output matches the source\n");
            } else {
                text.append("verify: the output matches the source, with ")
                        .append(warnings.size())
                        .append(warnings.size() == 1 ? " warning" : " warnings")
                        .append(" carried over from reading the source, as in the conversion:\n");
                for (Issue warning : warnings) {
                    text.append("  warning ")
                            .append(warning.code())
                            .append(warning.table() == null ? "" : " " + warning.table())
                            .append(": ")
                            .append(warning.message())
                            .append('\n');
                }
            }
        } else {
            text.append("verify: ").append(result.differenceCount()).append(" differences\n");
            result.differences().forEach(d -> text.append("  ").append(d).append('\n'));
        }
        out.print(text);
        out.flush();
        return result.matches() ? ExitCodes.of(issues) : ExitCodes.FAILED;
    }

    /** The source side alone: row counts and digests, for comparing two readings of the same database. */
    private Integer snapshot() throws IOException {
        OpenOptions options = source.toOpenOptions();
        Issues issues = new Issues();
        SourceSnapshot snapshot;
        try (AccessSource db = AccessSource.open(input, options, issues)) {
            SchemaModel model = SchemaExtractor.extract(db, plan.extractOptions(), issues);
            snapshot = SourceSnapshot.capture(db, model);
        }
        PrintWriter out = spec.commandLine().getOut();
        StringBuilder text = new StringBuilder();
        text.append("Source: ")
                .append(snapshot.source().fileName())
                .append(" (")
                .append(snapshot.source().fileFormat())
                .append(")\n");
        for (TableSnapshot table : snapshot.tables()) {
            text.append("  ")
                    .append(table.table())
                    .append(": ")
                    .append(table.rows())
                    .append(" rows, sha256 ")
                    .append(table.sha256())
                    .append('\n');
        }
        out.print(text);
        out.flush();
        return ExitCodes.of(issues);
    }

    private static boolean isSqlite(Path file) throws IOException {
        byte[] header = new byte[SQLITE_HEADER.length];
        try (var in = Files.newInputStream(file)) {
            return in.readNBytes(header, 0, header.length) == header.length && Arrays.equals(header, SQLITE_HEADER);
        }
    }
}
