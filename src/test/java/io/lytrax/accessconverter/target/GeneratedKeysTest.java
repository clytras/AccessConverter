package io.lytrax.accessconverter.target;

import static io.lytrax.accessconverter.model.Models.column;
import static io.lytrax.accessconverter.model.Models.primaryKey;
import static io.lytrax.accessconverter.model.Models.schema;
import static io.lytrax.accessconverter.model.Models.table;
import static org.assertj.core.api.Assertions.assertThat;

import io.lytrax.accessconverter.model.AccessType;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.target.mysql.MySqlDialect;
import io.lytrax.accessconverter.target.mysql.MySqlOptions;
import io.lytrax.accessconverter.target.mysql.MySqlPlan;
import io.lytrax.accessconverter.target.mysql.MySqlPlanner;
import io.lytrax.accessconverter.target.sqlite.SqliteOptions;
import io.lytrax.accessconverter.target.sqlite.SqlitePlan;
import io.lytrax.accessconverter.target.sqlite.SqlitePlanner;
import org.junit.jupiter.api.Test;

/**
 * {@code --add-primary-key} (issue #1): a key for each table Access keeps without one, and nothing else. The
 * conversion itself, rows numbered 1 to n and verified, is tested over the whole corpus (SqliteCorpusTest,
 * MySqlCorpusIT) and on the servers (MySqlBehaviorIT).
 */
class GeneratedKeysTest {

    private final Issues issues = new Issues();

    @Test
    void aKeylessTableGetsAnAutoNumberKeyFirstAndTheRestIsUntouched() {
        SchemaModel model = schema(
                table("Log", null, column("Message", AccessType.TEXT)),
                table("Customers", primaryKey("CustomerID"), column("CustomerID", AccessType.LONG)));
        issues.add(IssueCode.NO_PRIMARY_KEY, "Log", null, "table has no primary key; targets create none");

        SchemaModel keyed = GeneratedKeys.add(model, "id", issues);

        TableModel log = keyed.table("Log").orElseThrow();
        assertThat(log.columns()).extracting(ColumnModel::name).containsExactly("id", "Message");
        assertThat(log.columns().get(0).type()).isEqualTo(AccessType.AUTONUMBER_LONG);
        assertThat(log.primaryKey().origin()).isEqualTo(IndexModel.Origin.GENERATED);
        assertThat(log.generatedKeyColumn()).isEqualTo("id");
        assertThat(keyed.table("Customers").orElseThrow())
                .isEqualTo(model.table("Customers").orElseThrow());
        assertThat(keyed.table("Customers").orElseThrow().generatedKeyColumn()).isNull();

        // The extractor's "targets create none" is no longer true, and the report says what was added instead
        assertThat(issues.list()).singleElement().satisfies(i -> {
            assertThat(i.code()).isEqualTo(IssueCode.PRIMARY_KEY_ADDED);
            assertThat(i.table()).isEqualTo("Log");
            assertThat(i.object()).isEqualTo("id");
            assertThat(i.message()).contains("--add-primary-key", "1, 2, 3");
        });
    }

    @Test
    void aTakenNameGetsANumberAndTheReportSaysWhy() {
        SchemaModel model = schema(table("Log", null, column("ID", AccessType.TEXT), column("id_2", AccessType.TEXT)));

        TableModel log = GeneratedKeys.add(model, "id", issues).table("Log").orElseThrow();

        assertThat(log.generatedKeyColumn()).isEqualTo("id_3");
        assertThat(issues.list())
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("named id_3 because id is taken"));
    }

    @Test
    void theColumnCanBeNamed() {
        SchemaModel model = schema(table("Log", null, column("Message", AccessType.TEXT)));
        assertThat(GeneratedKeys.add(model, "row_id", issues)
                        .table("Log")
                        .orElseThrow()
                        .generatedKeyColumn())
                .isEqualTo("row_id");
    }

    /**
     * MySQL allows one AUTO_INCREMENT column per table: in a keyless table that already has an AutoNumber, the added
     * key takes it, and the AutoNumber keeps its values but stops generating them (maintainer decision), reported.
     * SQLite has no such limit on keys, but only its key column can be AUTOINCREMENT, so the same holds there.
     */
    @Test
    void theAddedKeyTakesAutoIncrementFromAnAutoNumberThatIsNotTheKey() {
        SchemaModel model = schema(
                table("Log", null, column("Counter", AccessType.AUTONUMBER_LONG), column("Message", AccessType.TEXT)));
        SchemaModel keyed = GeneratedKeys.add(model, "id", new Issues());

        Issues mysqlIssues = new Issues();
        MySqlPlan mysql = MySqlPlanner.plan(
                keyed, null, ConvertOptions.DEFAULT, MySqlOptions.of(MySqlDialect.MYSQL), mysqlIssues);
        MySqlPlan.PlannedTable log = mysql.table("Log").orElseThrow();
        assertThat(log.columns())
                .filteredOn(MySqlPlan.PlannedColumn::autoIncrement)
                .extracting(MySqlPlan.PlannedColumn::name)
                .containsExactly("id");
        assertThat(mysqlIssues.list()).anySatisfy(i -> {
            assertThat(i.code()).isEqualTo(IssueCode.AUTOINCREMENT_NOT_PRESERVED);
            assertThat(i.object()).isEqualTo("Counter");
        });

        Issues sqliteIssues = new Issues();
        SqlitePlan sqlite =
                SqlitePlanner.plan(keyed, null, ConvertOptions.DEFAULT, SqliteOptions.DEFAULT, sqliteIssues);
        assertThat(sqlite.table("Log").orElseThrow().autoIncrementColumn()).isEqualTo("id");
        assertThat(sqliteIssues.list())
                .anySatisfy(i -> assertThat(i.code()).isEqualTo(IssueCode.AUTOINCREMENT_NOT_PRESERVED));
    }
}
