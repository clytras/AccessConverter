package com.lytrax.accessconverter.cli;

import com.lytrax.accessconverter.extract.SchemaExtractor;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.profile.DataProfile;
import com.lytrax.accessconverter.profile.DataProfiler;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.ConvertOptions.OnTableError;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan;
import com.lytrax.accessconverter.target.sqlite.SqlitePlanner;
import com.lytrax.accessconverter.target.sqlite.SqliteVerifier;
import com.lytrax.accessconverter.verify.SourceSnapshot;
import com.lytrax.accessconverter.verify.SourceSnapshot.TableSnapshot;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(
        name = "verify",
        mixinStandardHelpOptions = true,
        sortOptions = false,
        description = {
            "Compare a converted output with its Access source (09): the schema it should have, and every value.",
            "The conversion is planned again from the source, so pass the same options the conversion used.",
            "Without <output>, print the source side: each table's row count and a digest of its canonical rows."
        })
final class VerifyCommand implements Callable<Integer> {

    /** Every SQLite file starts with this, including the trailing NUL. */
    private static final byte[] SQLITE_HEADER = "SQLite format 3\0".getBytes(StandardCharsets.US_ASCII);

    @Parameters(index = "0", paramLabel = "<input>", description = "Access database (.mdb, .accdb)")
    Path input;

    @Parameters(index = "1", arity = "0..1", paramLabel = "<output>", description = "Converted output to compare")
    Path output;

    @Mixin
    PlanOptions plan;

    @Mixin
    SourceOptions source;

    @Spec
    CommandSpec spec;

    @Override
    public Integer call() throws IOException {
        Main.requireFile(input);
        return output == null ? snapshot() : compare();
    }

    /** Compares a SQLite output with the source. Other targets arrive with their phases. */
    private Integer compare() throws IOException {
        Main.requireFile(output);
        if (!isSqlite(output)) {
            spec.commandLine()
                    .getErr()
                    .println("error: " + output + " is not a SQLite file; verify supports SQLite outputs");
            return ExitCodes.FAILED;
        }
        OpenOptions options = source.toOpenOptions();
        Issues issues = new Issues();
        ConvertOptions convert = plan.convertOptions(OnTableError.FAIL, ConvertOptions.DEFAULT_BATCH_ROWS);
        SqliteVerifier.Result result;
        SchemaModel model;
        try (AccessSource db = AccessSource.open(input, options, issues)) {
            model = SchemaExtractor.extract(db, plan.extractOptions(), issues);
            DataProfile profile = convert.profile() ? DataProfiler.profile(db, model) : null;
            // The issues of planning again are the conversion's, not the verification's: they go nowhere
            SqlitePlan planned = SqlitePlanner.plan(model, profile, convert, plan.sqliteOptions(false), new Issues());
            result = SqliteVerifier.verify(db, planned, output);
        }
        PrintWriter out = spec.commandLine().getOut();
        StringBuilder text = new StringBuilder();
        text.append("Source: ")
                .append(model.source().fileName())
                .append(" (")
                .append(model.source().fileFormat())
                .append(")\nOutput: ")
                .append(output)
                .append(" (SQLite)\n");
        for (SqliteVerifier.TableRows table : result.tables()) {
            text.append("  ")
                    .append(table.table())
                    .append(": ")
                    .append(table.actual())
                    .append(" rows\n");
        }
        if (result.matches()) {
            text.append("verify: the output matches the source\n");
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
