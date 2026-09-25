package io.lytrax.accessconverter.target.json;

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
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.WriteOutcome;
import io.lytrax.accessconverter.verify.VerifyResult;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

/** Test helper: converts an Access database to JSON in process, keeping the plan and the issues. */
public final class JsonFixture {

    /** The producer in tests: no version, so golden files don't change with it. */
    public static final String PRODUCER = "AccessConverter";

    private JsonFixture() {}

    public static Converted convert(Path source, Path output) {
        return convert(source, output, OpenOptions.DEFAULT, ConvertOptions.DEFAULT, JsonOptions.DEFAULT);
    }

    public static Converted convert(Path source, Path output, JsonOptions json) {
        return convert(source, output, OpenOptions.DEFAULT, ConvertOptions.DEFAULT, json);
    }

    public static Converted convert(
            Path source, Path output, OpenOptions open, ConvertOptions options, JsonOptions json) {
        Issues issues = new Issues();
        try (AccessSource db = AccessSource.open(source, open, issues)) {
            SchemaModel model = SchemaExtractor.extract(db, ExtractOptions.ALL, issues);
            DataProfile profile = options.profile() ? DataProfiler.profile(db, model) : null;
            JsonPlan plan = JsonPlanner.plan(model, profile, options, issues);
            WriteOutcome outcome = JsonWriter.write(db, plan, output, options, json, PRODUCER, issues);
            return new Converted(source, open, output, plan, outcome, issues);
        } catch (IOException e) {
            throw new UncheckedIOException("converting " + source, e);
        }
    }

    /** A finished conversion: the output, what the planner decided, and everything reported on the way. */
    public record Converted(
            Path source, OpenOptions open, Path output, JsonPlan plan, WriteOutcome outcome, Issues issues) {

        public List<Issue> issues(IssueCode code) {
            return issues.list().stream().filter(i -> i.code() == code).toList();
        }

        public JsonPlan.PlannedTable table(String name) {
            return plan.table(name).orElseThrow(() -> new AssertionError("no planned table " + name));
        }

        /** Compares the output with the source, as {@code verify} does. */
        public VerifyResult verify() {
            return verify(output);
        }

        /** Compares another output (an edited copy, say) with the source under this conversion's plan. */
        public VerifyResult verify(Path other) {
            try (AccessSource access = AccessSource.open(source, open, new Issues())) {
                return JsonVerifier.verify(access, plan, other);
            } catch (IOException e) {
                throw new UncheckedIOException("verifying " + source, e);
            }
        }
    }
}
