package io.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lytrax.accessconverter.extract.ExtractOptions;
import io.lytrax.accessconverter.extract.SchemaExtractor;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.profile.DataProfiler;
import io.lytrax.accessconverter.report.Issue;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.report.Severity;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.RowStream;
import io.lytrax.accessconverter.source.SourceException;
import io.lytrax.accessconverter.target.BinaryMode;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.ConvertOptions.OnTableError;
import io.lytrax.accessconverter.target.RowSource;
import io.lytrax.accessconverter.target.WriteOutcome;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What {@code --on-table-error} promises (03, Error handling): {@code fail} stops the conversion and leaves no
 * output, {@code continue} finishes the other tables and keeps what it wrote, and both exit 2.
 *
 * <p>The failure is injected at the writer's row source rather than faked with a damaged database: no corpus file
 * has a table that can't be read, and a fixture that only pretends to be broken would test the pretence.
 */
class SqliteTableErrorTest {

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

    @Test
    void continueFinishesTheOtherTablesAndKeepsTheOutput() throws IOException {
        Path output = dir.resolve("continue.sqlite3");
        Issues issues = new Issues();
        WriteOutcome outcome = convert(output, OnTableError.CONTINUE, "Customers", issues);

        assertThat(output).exists();
        assertThat(outcome.tableFailed()).isTrue();
        // The failed table is the only one without rows, and it is still in the schema
        assertThat(outcome.tables())
                .filteredOn(t -> t.table().equals("Customers"))
                .singleElement()
                .satisfies(t -> assertThat(t.rowsWritten()).isZero());
        try (Sqlite sqlite = Sqlite.open(output)) {
            assertThat(sqlite.value("SELECT count(*) FROM Customers")).isEqualTo(0);
            assertThat(sqlite.value("SELECT count(*) FROM Orders")).isEqualTo(2);
            assertThat(sqlite.value("SELECT count(*) FROM \"Order Details\"")).isEqualTo(3);
            assertThat(sqlite.strings("PRAGMA integrity_check")).containsExactly("ok");
        }
        List<Issue> reported = issues.list();
        assertThat(reported)
                .filteredOn(i -> i.code() == IssueCode.TABLE_READ_FAILED)
                .singleElement()
                .satisfies(i -> {
                    assertThat(i.table()).isEqualTo("Customers");
                    assertThat(i.message()).contains("reading the table failed after 0 rows", "page 42 is damaged");
                });
        // The orders whose customer is missing now break their foreign key: that is reported, not hidden
        assertThat(reported)
                .filteredOn(i -> i.code() == IssueCode.FOREIGN_KEY_CHECK_FAILED)
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("CustomersOrders"));
        assertThat(issues.highestSeverity()).isEqualTo(Severity.ERROR);
    }

    @Test
    void failStopsAndLeavesNoOutputBehind() throws IOException {
        Path output = dir.resolve("fail.sqlite3");
        Issues issues = new Issues();

        assertThatThrownBy(() -> convert(output, OnTableError.FAIL, "Customers", issues))
                .isInstanceOf(SourceException.class)
                .hasMessageContaining("reading table Customers failed");

        assertThat(output).doesNotExist();
        assertThat(output.resolveSibling(output.getFileName() + ".partial")).doesNotExist();
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.TABLE_READ_FAILED)
                .hasSize(1);
    }

    @Test
    void aTableThatReadsFineIsUnaffected() throws IOException {
        Path output = dir.resolve("ok.sqlite3");
        Issues issues = new Issues();
        // The same path with nothing failing: the seam itself changes nothing
        WriteOutcome outcome = convert(output, OnTableError.FAIL, "no such table", issues);

        assertThat(outcome.tableFailed()).isFalse();
        assertThat(outcome.rowsWritten()).isEqualTo(19);
        assertThat(issues.list()).noneMatch(i -> i.severity() == Severity.ERROR);
    }

    /**
     * A failed table is empty, and so are its files (08, --binary files): {@code Files} fails at its second row after
     * the first wrote its files, because {@code Raw} is planned NOT NULL and that row holds NULL.
     */
    @Test
    void aTableThatFailsLeavesNoFilesBehind() throws IOException {
        Path output = dir.resolve("files.sqlite3");
        Issues issues = new Issues();
        ConvertOptions options =
                new ConvertOptions(true, false, OnTableError.CONTINUE, 1).withBinary(BinaryMode.FILES, true, false);
        try (AccessSource db = AccessSource.open(GeneratedFixture.SCHEMA_FIDELITY.path())) {
            SchemaModel model = SchemaExtractor.extract(db, ExtractOptions.ALL, issues);
            SqlitePlan plan =
                    SqlitePlanner.plan(model, DataProfiler.profile(db, model), options, SqliteOptions.DEFAULT, issues);
            WriteOutcome outcome = SqliteWriter.write(
                    db::rows, required(plan), output, options, SqliteOptions.DEFAULT, "AccessConverter", false, issues);
            assertThat(outcome.tableFailed()).isTrue();
        }

        try (Sqlite sqlite = Sqlite.open(output)) {
            assertThat(sqlite.value("SELECT count(*) FROM Files")).isEqualTo(0);
        }
        assertThat(dir.resolve("files.sqlite3-files/Files")).doesNotExist();
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.TABLE_WRITE_FAILED)
                .singleElement()
                .satisfies(i -> assertThat(i.table()).isEqualTo("Files"));
        assertThat(issues.list()).noneMatch(i -> i.code() == IssueCode.BINARY_FILES_NOT_REMOVED);
    }

    /** The plan with {@code Files.Raw} NOT NULL, which the second row contradicts. */
    private static SqlitePlan required(SqlitePlan plan) {
        List<SqlitePlan.PlannedTable> tables = plan.tables().stream()
                .map(t -> !t.name().equals("Files")
                        ? t
                        : new SqlitePlan.PlannedTable(
                                t.source(),
                                t.name(),
                                t.columns().stream()
                                        .map(c -> !c.name().equals("Raw")
                                                ? c
                                                : new SqlitePlan.PlannedColumn(
                                                        c.source(),
                                                        c.sourceIndex(),
                                                        c.name(),
                                                        c.declaredType(),
                                                        true,
                                                        c.collateNocase(),
                                                        c.defaultSql(),
                                                        c.form(),
                                                        c.fractionDigits(),
                                                        c.olePart()))
                                        .toList(),
                                t.primaryKey(),
                                t.indexes(),
                                t.foreignKeys(),
                                t.autoIncrementColumn(),
                                t.sequenceSeed(),
                                t.createTableSql()))
                .toList();
        return new SqlitePlan(plan.model(), plan.strict(), tables, plan.metadata(), plan.profiled(), plan.options());
    }

    private WriteOutcome convert(Path output, OnTableError onError, String failing, Issues issues) throws IOException {
        ConvertOptions options = new ConvertOptions(true, false, onError, ConvertOptions.DEFAULT_BATCH_ROWS);
        try (AccessSource db = AccessSource.open(GeneratedFixture.SCHEMA_FIDELITY.path())) {
            SchemaModel model = SchemaExtractor.extract(db, ExtractOptions.ALL, issues);
            SqlitePlan plan =
                    SqlitePlanner.plan(model, DataProfiler.profile(db, model), options, SqliteOptions.DEFAULT, issues);
            return SqliteWriter.write(
                    new FailingTable(db, failing),
                    plan,
                    output,
                    options,
                    SqliteOptions.DEFAULT,
                    "AccessConverter",
                    true,
                    issues);
        } finally {
            Files.deleteIfExists(output.resolveSibling(output.getFileName() + ".partial"));
        }
    }
}
