package io.lytrax.accessconverter.target.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.lytrax.accessconverter.extract.ExtractOptions;
import io.lytrax.accessconverter.extract.SchemaExtractor;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.profile.DataProfiler;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code --on-table-error} in a dump (03, Error handling): {@code fail} leaves no file, {@code continue} writes the
 * failed table's rows away with a ROLLBACK, so the table loads empty rather than half-filled, and leaves out the
 * foreign keys that would reject rows pointing at it. The failure is injected at the writer's row source, as in
 * {@code SqliteTableErrorTest}.
 */
class MySqlTableErrorTest {

    @TempDir
    Path dir;

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
    void continueRollsTheTableBackAndLeavesOutTheForeignKeysThatNeedIt() throws IOException {
        Path output = dir.resolve("continue.sql");
        Issues issues = new Issues();
        WriteOutcome outcome = convert(output, OnTableError.CONTINUE, "Customers", issues);

        assertThat(outcome.tableFailed()).isTrue();
        String dump = Files.readString(output, StandardCharsets.UTF_8);
        assertThat(dump).doesNotContain("INSERT INTO `Customers`").contains("ROLLBACK;");
        assertThat(dump).doesNotContain("ADD CONSTRAINT `CustomersOrders`");
        assertThat(dump).contains("ADD CONSTRAINT `OrdersOrder Details`");
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.FOREIGN_KEY_CHECK_FAILED)
                .singleElement()
                .satisfies(i -> assertThat(i.object()).isEqualTo("CustomersOrders"));
        assertThat(issues.highestSeverity()).isEqualTo(Severity.ERROR);
    }

    @Test
    void failStopsAndLeavesNoOutputBehind() {
        Path output = dir.resolve("fail.sql");
        Issues issues = new Issues();

        assertThatThrownBy(() -> convert(output, OnTableError.FAIL, "Customers", issues))
                .isInstanceOf(SourceException.class)
                .hasMessageContaining("reading table Customers failed");
        assertThat(output).doesNotExist();
        assertThat(output.resolveSibling("fail.sql.partial")).doesNotExist();
    }

    /**
     * A failed table loads empty, and its files are gone too (08, --binary files): {@code Files} fails at its second row
     * after the first wrote its files, because {@code Raw} is planned NOT NULL and that row holds NULL.
     */
    @Test
    void aTableThatFailsLeavesNoFilesBehind() throws IOException {
        Path output = dir.resolve("files.sql");
        Issues issues = new Issues();
        ConvertOptions options =
                new ConvertOptions(true, false, OnTableError.CONTINUE, 1).withBinary(BinaryMode.FILES, true, false);
        MySqlOptions mysql = MySqlOptions.of(MySqlDialect.MARIADB);
        try (AccessSource db = AccessSource.open(GeneratedFixture.SCHEMA_FIDELITY.path())) {
            SchemaModel model = SchemaExtractor.extract(db, ExtractOptions.ALL, issues);
            MySqlPlan plan = MySqlPlanner.plan(model, DataProfiler.profile(db, model), options, mysql, issues);
            WriteOutcome outcome = MySqlDumpWriter.write(
                    db::rows, required(plan), output, options, mysql, MySqlFixture.PRODUCER, issues);
            assertThat(outcome.tableFailed()).isTrue();
        }

        assertThat(Files.readString(output, StandardCharsets.UTF_8)).contains("ROLLBACK;");
        assertThat(dir.resolve("files.sql-files/Files")).doesNotExist();
        assertThat(issues.list())
                .filteredOn(i -> i.code() == IssueCode.TABLE_WRITE_FAILED)
                .singleElement()
                .satisfies(i -> assertThat(i.table()).isEqualTo("Files"));
        assertThat(issues.list()).noneMatch(i -> i.code() == IssueCode.BINARY_FILES_NOT_REMOVED);
    }

    /** The plan with {@code Files.Raw} NOT NULL, which the second row contradicts. */
    private static MySqlPlan required(MySqlPlan plan) {
        List<MySqlPlan.PlannedTable> tables = plan.tables().stream()
                .map(t -> !t.name().equals("Files")
                        ? t
                        : new MySqlPlan.PlannedTable(
                                t.source(),
                                t.name(),
                                t.columns().stream()
                                        .map(c -> !c.name().equals("Raw")
                                                ? c
                                                : new MySqlPlan.PlannedColumn(
                                                        c.source(),
                                                        c.sourceIndex(),
                                                        c.name(),
                                                        c.type(),
                                                        c.collation(),
                                                        c.form(),
                                                        true,
                                                        c.autoIncrement(),
                                                        c.defaultSql(),
                                                        c.comment(),
                                                        c.fractionDigits(),
                                                        c.olePart()))
                                        .toList(),
                                t.primaryKey(),
                                t.inlineIndexes(),
                                t.indexes(),
                                t.checks(),
                                t.foreignKeys(),
                                t.comment(),
                                t.autoIncrementSeed(),
                                t.createTableSql(),
                                t.indexSql(),
                                t.checkSql(),
                                t.foreignKeySql()))
                .toList();
        return new MySqlPlan(plan.model(), plan.dialect(), plan.collation(), tables, plan.profiled(), plan.options());
    }

    private WriteOutcome convert(Path output, OnTableError onError, String failing, Issues issues) throws IOException {
        ConvertOptions options = new ConvertOptions(true, false, onError, ConvertOptions.DEFAULT_BATCH_ROWS);
        MySqlOptions mysql = MySqlOptions.of(MySqlDialect.MYSQL);
        try (AccessSource db = AccessSource.open(GeneratedFixture.SCHEMA_FIDELITY.path())) {
            SchemaModel model = SchemaExtractor.extract(db, ExtractOptions.ALL, issues);
            MySqlPlan plan = MySqlPlanner.plan(model, DataProfiler.profile(db, model), options, mysql, issues);
            return MySqlDumpWriter.write(
                    new FailingTable(db, failing), plan, output, options, mysql, MySqlFixture.PRODUCER, issues);
        }
    }
}
