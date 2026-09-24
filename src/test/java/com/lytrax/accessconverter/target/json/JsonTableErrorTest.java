package com.lytrax.accessconverter.target.json;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.lytrax.accessconverter.extract.ExtractOptions;
import com.lytrax.accessconverter.extract.SchemaExtractor;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.profile.DataProfiler;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.report.Severity;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.RowStream;
import com.lytrax.accessconverter.source.SourceException;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.ConvertOptions.OnTableError;
import com.lytrax.accessconverter.target.RowSource;
import com.lytrax.accessconverter.target.WriteOutcome;
import com.lytrax.accessconverter.target.json.JsonOptions.Layout;
import com.lytrax.accessconverter.target.json.JsonPlan.PlannedColumn;
import com.lytrax.accessconverter.target.json.JsonPlan.PlannedTable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * What {@code --on-table-error} promises (03, Error handling), as the SQL targets keep it: {@code fail} leaves no
 * output; {@code continue} finishes the other tables and leaves the failed one empty, never half-written, with the
 * failure reported and exit 2.
 *
 * <p>Two failures, both injected: a table that can't be read at all (at the writer's row source, as in the SQLite
 * test), and one that fails after two of its three rows were written. The second is what cutting the file back is for;
 * it is provoked by planning {@code Order Details.UnitPrice} as not nullable, which its third row contradicts.
 */
class JsonTableErrorTest {

    @TempDir
    Path dir;

    /** A row source that reads everything from the real database except one table, which fails as a damaged one. */
    private record FailingTable(AccessSource source, String table) implements RowSource {
        @Override
        public RowStream of(TableModel model) throws IOException {
            if (model.name().equalsIgnoreCase(table)) {
                throw SourceException.readFailed(
                        source.file(), "table " + model.name(), new IOException("page 42 is damaged"));
            }
            return source.rows(model);
        }
    }

    @ParameterizedTest
    @EnumSource(Layout.class)
    void continueLeavesAnUnreadableTableEmpty(Layout layout) throws IOException {
        Path output = dir.resolve("unreadable" + extension(layout));
        Issues issues = new Issues();
        WriteOutcome outcome = convert(output, layout, OnTableError.CONTINUE, "Customers", false, issues);

        assertThat(outcome.tableFailed()).isTrue();
        assertThat(rows(output, layout, "Customers")).isEmpty();
        assertThat(rows(output, layout, "Orders")).hasSize(2);
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.TABLE_READ_FAILED)
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("page 42 is damaged"));
        assertThat(issues.highestSeverity()).isEqualTo(Severity.ERROR);
        assertThat(JsonSchemaCheck.errors(output)).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(Layout.class)
    void continueCutsATableThatFailsHalfwayBackToEmpty(Layout layout) throws IOException {
        Path output = dir.resolve("halfway" + extension(layout));
        Issues issues = new Issues();
        WriteOutcome outcome = convert(output, layout, OnTableError.CONTINUE, "no such table", true, issues);

        assertThat(outcome.tableFailed()).isTrue();
        assertThat(outcome.tables())
                .filteredOn(t -> t.table().equals("Order Details"))
                .singleElement()
                .satisfies(t -> {
                    assertThat(t.rowsRead()).isEqualTo(3);
                    assertThat(t.rowsWritten()).isZero();
                });
        // The two rows written before the failure are gone, and the document around them is intact
        assertThat(rows(output, layout, "Order Details")).isEmpty();
        assertThat(rows(output, layout, "Orders")).hasSize(2);
        assertThat(rows(output, layout, "Products")).hasSize(2);
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.TABLE_WRITE_FAILED)
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("UnitPrice"));
        assertThat(JsonSchemaCheck.errors(output)).isEmpty();
        if (layout == Layout.DOCUMENT) {
            assertThat(Files.readString(output)).contains("\"Order Details\": [],\n    \"Orders\": [\n");
        }
    }

    @ParameterizedTest
    @EnumSource(Layout.class)
    void failStopsAndLeavesNoOutputBehind(Layout layout) {
        Path output = dir.resolve("fail" + extension(layout));
        Issues issues = new Issues();

        assertThatThrownBy(() -> convert(output, layout, OnTableError.FAIL, "Customers", false, issues))
                .isInstanceOf(SourceException.class)
                .hasMessageContaining("reading table Customers failed");

        assertThat(output).doesNotExist();
        assertThat(output.resolveSibling(output.getFileName() + ".partial")).doesNotExist();
    }

    @Test
    void anExistingNdjsonExportIsReplacedButAForeignDirectoryIsNot() throws IOException {
        Path output = dir.resolve("replace");
        convert(output, Layout.NDJSON, OnTableError.FAIL, "no such table", false, new Issues());
        Files.writeString(output.resolve("Shippers.ndjson"), "stale\n");

        convert(output, Layout.NDJSON, OnTableError.FAIL, "no such table", false, new Issues());
        assertThat(rows(output, Layout.NDJSON, "Shippers")).hasSize(2);
        assertThat(output.resolveSibling("replace.old")).doesNotExist();

        Path foreign = Files.createDirectories(dir.resolve("foreign"));
        Files.writeString(foreign.resolve("notes.txt"), "mine");
        assertThatThrownBy(() -> convert(foreign, Layout.NDJSON, OnTableError.FAIL, "none", false, new Issues()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("notes.txt");
        assertThat(foreign.resolve("notes.txt")).hasContent("mine");
    }

    private static String extension(Layout layout) {
        return layout == Layout.DOCUMENT ? ".json" : "";
    }

    /** A table's rows in the output, one string per row. */
    private static List<String> rows(Path output, Layout layout, String table) throws IOException {
        if (layout == Layout.NDJSON) {
            return Files.readAllLines(output.resolve(table + ".ndjson"), StandardCharsets.UTF_8);
        }
        String document = Files.readString(output, StandardCharsets.UTF_8);
        int start = document.indexOf("\n    \"" + table + "\": [");
        int end = document.indexOf(']', document.indexOf('[', start));
        return document.substring(document.indexOf('[', start) + 1, end)
                .lines()
                .filter(l -> !l.isBlank())
                .toList();
    }

    private WriteOutcome convert(
            Path output, Layout layout, OnTableError onError, String failing, boolean halfway, Issues issues)
            throws IOException {
        ConvertOptions options = new ConvertOptions(true, false, onError, ConvertOptions.DEFAULT_BATCH_ROWS);
        try (AccessSource db = AccessSource.open(GeneratedFixture.SCHEMA_FIDELITY.path())) {
            SchemaModel model = SchemaExtractor.extract(db, ExtractOptions.ALL, issues);
            JsonPlan plan = JsonPlanner.plan(model, DataProfiler.profile(db, model), options, issues);
            if (halfway) {
                plan = withUnitPriceRequired(plan);
            }
            return JsonWriter.write(
                    new FailingTable(db, failing),
                    plan,
                    output,
                    options,
                    JsonOptions.DEFAULT.withLayout(layout),
                    JsonFixture.PRODUCER,
                    issues);
        }
    }

    private static JsonPlan withUnitPriceRequired(JsonPlan plan) {
        List<PlannedTable> tables = plan.tables().stream()
                .map(t -> !t.name().equals("Order Details")
                        ? t
                        : new PlannedTable(
                                t.source(),
                                t.columns().stream()
                                        .map(c -> c.name().equals("UnitPrice")
                                                ? new PlannedColumn(c.source(), c.sourceIndex(), c.type(), false)
                                                : c)
                                        .toList(),
                                t.primaryKey(),
                                t.indexes(),
                                t.file()))
                .toList();
        return new JsonPlan(plan.model(), tables, plan.relationships(), plan.linked(), plan.profiled());
    }
}
