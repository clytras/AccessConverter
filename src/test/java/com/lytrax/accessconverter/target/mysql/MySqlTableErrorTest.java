package com.lytrax.accessconverter.target.mysql;

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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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
