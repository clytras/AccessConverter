package com.lytrax.accessconverter.target.sqlite;

import com.lytrax.accessconverter.extract.ExtractOptions;
import com.lytrax.accessconverter.extract.SchemaExtractor;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.profile.DataProfile;
import com.lytrax.accessconverter.profile.DataProfiler;
import com.lytrax.accessconverter.report.Issue;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.verify.VerifyResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

/** Test helper: converts an Access database to SQLite in process, keeping the plan and the issues for assertions. */
public final class SqliteFixture {

    private SqliteFixture() {}

    public static Converted convert(Path source, Path output) {
        return convert(source, output, OpenOptions.DEFAULT, ConvertOptions.DEFAULT, SqliteOptions.DEFAULT);
    }

    public static Converted convert(Path source, Path output, SqliteOptions sqlite) {
        return convert(source, output, OpenOptions.DEFAULT, ConvertOptions.DEFAULT, sqlite);
    }

    public static Converted convert(
            Path source, Path output, OpenOptions open, ConvertOptions options, SqliteOptions sqlite) {
        Issues issues = new Issues();
        try (AccessSource db = AccessSource.open(source, open, issues)) {
            SchemaModel model = SchemaExtractor.extract(db, ExtractOptions.ALL, issues);
            DataProfile profile = options.profile() ? DataProfiler.profile(db, model) : null;
            SqlitePlan plan = SqlitePlanner.plan(model, profile, options, sqlite, issues);
            SqliteWriter.write(db, plan, output, options, sqlite, true, issues);
            VerifyResult verified = SqliteVerifier.verify(db, plan, output);
            return new Converted(output, plan, model, profile, issues, verified);
        } catch (IOException e) {
            throw new UncheckedIOException("converting " + source, e);
        }
    }

    /** A finished conversion: the file, what the planner decided, and everything reported on the way. */
    public record Converted(
            Path file, SqlitePlan plan, SchemaModel model, DataProfile profile, Issues issues, VerifyResult verified) {

        public Sqlite open() {
            return Sqlite.open(file);
        }

        public String sql(String object) {
            try (Sqlite sqlite = open()) {
                return sqlite.sql(object);
            }
        }

        public List<Issue> issues(IssueCode code) {
            return issues.list().stream().filter(i -> i.code() == code).toList();
        }

        public SqlitePlan.PlannedTable table(String name) {
            return plan.table(name).orElseThrow(() -> new AssertionError("no planned table " + name));
        }

        public SqlitePlan.PlannedColumn column(String table, String column) {
            return table(table)
                    .column(column)
                    .orElseThrow(() -> new AssertionError("no planned column " + table + "." + column));
        }
    }
}
