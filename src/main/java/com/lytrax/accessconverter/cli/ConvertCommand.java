package com.lytrax.accessconverter.cli;

import com.lytrax.accessconverter.extract.SchemaExtractor;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.profile.DataProfile;
import com.lytrax.accessconverter.profile.DataProfiler;
import com.lytrax.accessconverter.report.ConversionReport;
import com.lytrax.accessconverter.report.ConversionReport.TableResult;
import com.lytrax.accessconverter.report.Issue;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.report.Severity;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.ConvertOptions.OnTableError;
import com.lytrax.accessconverter.target.sqlite.SqliteOptions;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan;
import com.lytrax.accessconverter.target.sqlite.SqlitePlanner;
import com.lytrax.accessconverter.target.sqlite.SqliteVerifier;
import com.lytrax.accessconverter.target.sqlite.SqliteWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
        name = "convert",
        mixinStandardHelpOptions = true,
        sortOptions = false,
        description = {
            "Convert an Access database. The output carries the Access keys, indexes, foreign keys with their"
                    + " cascade actions, NOT NULL, defaults and CHECK constraints, as far as the data allows and the"
                    + " target can express them.",
            "Every value is exported exactly or reported in the conversion report."
        })
final class ConvertCommand implements Callable<Integer> {

    enum Target {
        /** A {@code .sqlite3} database file (06). */
        sqlite;

        String extension() {
            return ".sqlite3";
        }
    }

    enum ResultFormat {
        text,
        json
    }

    @Parameters(index = "0", paramLabel = "<input>", description = "Access database (.mdb, .accdb)")
    Path input;

    @Option(
            names = "--to",
            required = true,
            paramLabel = "<target>",
            description = "Output format: ${COMPLETION-CANDIDATES}.")
    Target to;

    @Option(
            names = {"-o", "--output"},
            paramLabel = "<file>",
            description = "Output file (default: the input's name with the target's extension, next to the input).")
    Path output;

    @Option(names = "--overwrite", description = "Replace an existing output file instead of failing.")
    boolean overwrite;

    @Option(
            names = "--report",
            paramLabel = "<file>",
            description = "Write the conversion report here (default: <output>.report.json).")
    Path report;

    @Option(
            names = "--no-report",
            description = "Don't write the conversion report file; the issues are still printed.")
    boolean noReport;

    @Option(
            names = "--format-result",
            defaultValue = "text",
            description = "How to print the result: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).")
    ResultFormat formatResult;

    @Option(
            names = "--on-table-error",
            defaultValue = "fail",
            description = "A table that can't be read or written: fail, continue (default: ${DEFAULT-VALUE})."
                    + " fail deletes the output; continue finishes the other tables and still exits 2.")
    OnTableError onTableError;

    @Option(
            names = "--verify",
            description = "After writing, check the output's integrity and compare its schema and every value with"
                    + " the source.")
    boolean verify;

    @Option(names = "--analyze", description = "Run ANALYZE on the finished output, not only PRAGMA optimize.")
    boolean analyze;

    @Option(
            names = "--batch-rows",
            paramLabel = "<n>",
            description = "Rows per insert batch (default: " + ConvertOptions.DEFAULT_BATCH_ROWS + ").")
    int batchRows = ConvertOptions.DEFAULT_BATCH_ROWS;

    @Mixin
    PlanOptions plan;

    @Mixin
    SourceOptions source;

    @Spec
    CommandSpec spec;

    @ParentCommand
    Main main;

    @Override
    public Integer call() throws IOException {
        Main.requireFile(input);
        Path out = output != null ? output : input.resolveSibling(baseName(input) + to.extension());
        if (Files.exists(out) && !overwrite) {
            spec.commandLine().getErr().println("error: " + out + " exists; pass --overwrite to replace it");
            return ExitCodes.FAILED;
        }
        OpenOptions openOptions = source.toOpenOptions();
        ConvertOptions options = plan.convertOptions(onTableError, batchRows);
        SqliteOptions sqlite = plan.sqliteOptions(analyze);
        Issues issues = new Issues();
        Map<String, Duration> timings = new LinkedHashMap<>();
        SchemaModel model;
        List<TableResult> tables;
        try (AccessSource db = AccessSource.open(input, openOptions, issues)) {
            long started = System.nanoTime();
            model = SchemaExtractor.extract(db, plan.extractOptions(), issues);
            timings.put("extract", since(started));

            started = System.nanoTime();
            DataProfile profile = options.profile() ? DataProfiler.profile(db, model) : null;
            timings.put("profile", since(started));

            started = System.nanoTime();
            SqlitePlan planned = SqlitePlanner.plan(model, profile, options, sqlite, issues);
            timings.put("plan", since(started));

            started = System.nanoTime();
            SqliteWriter.Outcome outcome = SqliteWriter.write(db, planned, out, options, sqlite, verify, issues);
            timings.put("write", since(started));
            tables = outcome.tables();

            if (verify) {
                started = System.nanoTime();
                SqliteVerifier.Result result = SqliteVerifier.verify(db, planned, out);
                timings.put("verify", since(started));
                result.report(issues);
            }
        }
        ConversionReport conversionReport = new ConversionReport(
                Main.Version.version(),
                "convert",
                effectiveOptions(out),
                model.source(),
                tables,
                issues.list(),
                outcome(issues),
                timings);
        Path reportFile = noReport ? null : report != null ? report : Path.of(out + ".report.json");
        if (reportFile != null) {
            try (Writer writer = Files.newBufferedWriter(reportFile, StandardCharsets.UTF_8)) {
                conversionReport.write(writer);
                writer.write('\n');
            }
        }
        print(conversionReport, out, reportFile, tables);
        return ExitCodes.of(issues);
    }

    private SortedMap<String, String> effectiveOptions(Path out) {
        SortedMap<String, String> options = plan.describe();
        options.put("to", to.name());
        options.put("output", out.toString());
        options.put("onTableError", PlanOptions.lower(onTableError));
        options.put("batchRows", String.valueOf(batchRows));
        options.put("verify", String.valueOf(verify));
        options.put("analyze", String.valueOf(analyze));
        if (source.charset != null) {
            options.put("charset", source.charset);
        }
        return options;
    }

    private static String outcome(Issues issues) {
        Severity highest = issues.highestSeverity();
        if (highest == Severity.ERROR) {
            return "failed";
        }
        return highest == Severity.WARNING ? "success-with-warnings" : "success";
    }

    private void print(ConversionReport report, Path out, Path reportFile, List<TableResult> tables)
            throws IOException {
        if (formatResult == ResultFormat.json) {
            Writer writer = main.utf8Out();
            report.write(writer);
            writer.write('\n');
            writer.flush();
            return;
        }
        long rows = tables.stream()
                .mapToLong(t -> t.rowsWritten() == null ? 0 : t.rowsWritten())
                .sum();
        StringBuilder text = new StringBuilder();
        text.append(report.source().fileName())
                .append(" (")
                .append(report.source().fileFormat())
                .append(") -> ")
                .append(out)
                .append('\n');
        text.append(String.format(Locale.ROOT, "  %,d tables, %,d rows", tables.size(), rows))
                .append(" in ")
                .append(String.format(Locale.ROOT, "%.1f s", total(report) / 1000.0))
                .append('\n');
        Map<Severity, Long> bySeverity = new TreeMap<>();
        for (Issue issue : report.issues()) {
            bySeverity.merge(issue.severity(), 1L, Long::sum);
        }
        if (bySeverity.isEmpty()) {
            text.append("  no issues\n");
        } else {
            List<String> parts = new ArrayList<>();
            bySeverity.forEach((severity, count) -> parts.add(count + " " + severity.label()));
            text.append("  ").append(String.join(", ", parts));
            text.append(reportFile == null ? "" : " (see " + reportFile + ")").append('\n');
        }
        for (Issue issue : report.issues()) {
            if (issue.severity() == Severity.ERROR) {
                text.append("  error ")
                        .append(issue.code())
                        .append(issue.table() == null ? "" : " " + issue.table())
                        .append(": ")
                        .append(issue.message())
                        .append('\n');
            }
        }
        if (report.issues().stream().anyMatch(i -> i.code() == IssueCode.FK_SKIPPED_ORPHANS)) {
            text.append("  some foreign keys were left out; the report says which and why\n");
        }
        if (report.issues().stream().anyMatch(i -> i.code() == IssueCode.FOREIGN_KEYS_NEED_PRAGMA)) {
            text.append("  SQLite enforces the foreign keys only for a connection that runs"
                    + " PRAGMA foreign_keys = ON\n");
        }
        spec.commandLine().getOut().print(text);
        spec.commandLine().getOut().flush();
    }

    private static long total(ConversionReport report) {
        return report.timings().values().stream().mapToLong(Duration::toMillis).sum();
    }

    private static Duration since(long startedNanos) {
        return Duration.ofNanos(System.nanoTime() - startedNanos);
    }

    private static String baseName(Path file) {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        return dot <= 0 ? name : name.substring(0, dot);
    }
}
