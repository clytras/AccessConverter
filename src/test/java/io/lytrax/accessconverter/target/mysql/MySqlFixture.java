package io.lytrax.accessconverter.target.mysql;

import io.lytrax.accessconverter.extract.ExtractOptions;
import io.lytrax.accessconverter.extract.SchemaExtractor;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.profile.DataProfile;
import io.lytrax.accessconverter.profile.DataProfiler;
import io.lytrax.accessconverter.report.Issue;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.OpenOptions;
import io.lytrax.accessconverter.target.ComplexTables;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.GeneratedKeys;
import io.lytrax.accessconverter.verify.VerifyResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.List;

/** Test helper: converts an Access database to a MySQL/MariaDB dump in process, keeping the plan and the issues. */
public final class MySqlFixture {

    /** The header's producer in tests: no version, so golden dumps don't change with it. */
    public static final String PRODUCER = "AccessConverter";

    private MySqlFixture() {}

    public static Converted convert(Path source, Path output, MySqlDialect dialect) {
        return convert(source, output, OpenOptions.DEFAULT, ConvertOptions.DEFAULT, MySqlOptions.of(dialect));
    }

    public static Converted convert(
            Path source, Path output, OpenOptions open, ConvertOptions options, MySqlOptions mysql) {
        return convert(source, output, open, options, mysql, null);
    }

    /** @param addPrimaryKey {@code --add-primary-key}'s column name, or null without the option */
    public static Converted convert(
            Path source,
            Path output,
            OpenOptions open,
            ConvertOptions options,
            MySqlOptions mysql,
            String addPrimaryKey) {
        Issues issues = new Issues();
        try (AccessSource db = AccessSource.open(source, open, issues)) {
            SchemaModel model = ComplexTables.expand(SchemaExtractor.extract(db, ExtractOptions.ALL, issues), options);
            if (addPrimaryKey != null) {
                model = GeneratedKeys.add(model, addPrimaryKey, issues);
            }
            DataProfile profile = options.profile() ? DataProfiler.profile(db, model) : null;
            MySqlPlan plan = MySqlPlanner.plan(model, profile, options, mysql, issues);
            MySqlDumpWriter.write(db, plan, output, options, mysql, PRODUCER, issues);
            return new Converted(source, open, output, plan, model, profile, issues);
        } catch (IOException e) {
            throw new UncheckedIOException("converting " + source, e);
        }
    }

    /** A finished conversion: the dump, what the planner decided, and everything reported on the way. */
    public record Converted(
            Path source,
            OpenOptions open,
            Path file,
            MySqlPlan plan,
            SchemaModel model,
            DataProfile profile,
            Issues issues) {

        public List<Issue> issues(IssueCode code) {
            return issues.list().stream().filter(i -> i.code() == code).toList();
        }

        public MySqlPlan.PlannedTable table(String name) {
            return plan.table(name).orElseThrow(() -> new AssertionError("no planned table " + name));
        }

        public MySqlPlan.PlannedColumn column(String table, String column) {
            return table(table)
                    .column(column)
                    .orElseThrow(() -> new AssertionError("no planned column " + table + "." + column));
        }

        /** Compares a database the dump was loaded into with the source, as {@code verify --jdbc-url} does. */
        public VerifyResult verify(Connection db) {
            return verify(db, file.toAbsolutePath().getParent());
        }

        /** As {@link #verify(Connection)}, finding {@code --binary files} paths under {@code dumpDirectory}. */
        public VerifyResult verify(Connection db, Path dumpDirectory) {
            try (AccessSource access = AccessSource.open(source, open, new Issues())) {
                return MySqlVerifier.verify(access, plan, db, dumpDirectory);
            } catch (IOException e) {
                throw new UncheckedIOException("verifying " + source, e);
            }
        }
    }
}
