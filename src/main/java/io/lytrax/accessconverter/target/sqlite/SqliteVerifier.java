package io.lytrax.accessconverter.target.sqlite;

import io.lytrax.accessconverter.model.ForeignKeyModel.Action;
import io.lytrax.accessconverter.model.IndexModel.IndexColumn;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.target.BinaryCells;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedColumn;
import io.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedForeignKey;
import io.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedIndex;
import io.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedTable;
import io.lytrax.accessconverter.verify.RowComparison;
import io.lytrax.accessconverter.verify.VerifyResult;
import io.lytrax.accessconverter.verify.VerifyResult.Difference;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Compares a finished SQLite file with the plan that describes it and with the source data (09, The verifier).
 *
 * <p>The plan is re-derived from the source, so a difference here means the output doesn't hold what the source
 * says it should: every documented downgrade is already part of the plan.
 */
public final class SqliteVerifier {

    private final SqlitePlan plan;
    private final AccessSource source;
    private Path outputDirectory;
    private final VerifyResult.Collector differences = new VerifyResult.Collector();

    private SqliteVerifier(AccessSource source, SqlitePlan plan) {
        this.source = source;
        this.plan = plan;
    }

    public static VerifyResult verify(AccessSource source, SqlitePlan plan, Path output) throws IOException {
        SqliteVerifier verifier = new SqliteVerifier(source, plan);
        verifier.outputDirectory = output.toAbsolutePath().getParent();
        try (Connection db = DriverManager.getConnection("jdbc:sqlite:" + output.toAbsolutePath())) {
            verifier.run(db);
        } catch (SQLException e) {
            throw new IOException("the output could not be read back: " + e.getMessage(), e);
        }
        return verifier.differences.result();
    }

    private void run(Connection db) throws SQLException, IOException {
        SqliteIntrospector.Schema schema = SqliteIntrospector.read(db);
        header(db, "application_id", SqliteWriter.APPLICATION_ID);
        header(db, "user_version", SqliteWriter.FORMAT_VERSION);
        tables(schema);
        for (PlannedTable table : plan.tables()) {
            Optional<SqliteIntrospector.Table> actual = schema.table(table.name());
            if (actual.isEmpty()) {
                continue;
            }
            columns(table, actual.get());
            primaryKey(table, actual.get());
            indexes(table, actual.get(), schema);
            foreignKeys(table, actual.get());
            statement(
                    table.name(), "table", table.createTableSql(), actual.get().sql());
            data(db, table);
        }
    }

    // ---------------------------------------------------------------- schema

    /** The header field that marks the file as AccessConverter's, and its layout version. */
    private void header(Connection db, String pragma, int expected) throws SQLException {
        try (Statement statement = db.createStatement();
                ResultSet rows = statement.executeQuery("PRAGMA " + pragma)) {
            int actual = rows.next() ? rows.getInt(1) : 0;
            if (actual != expected) {
                differences.add(new Difference(
                        null, null, "PRAGMA " + pragma, String.valueOf(expected), String.valueOf(actual)));
            }
        }
    }

    private void tables(SqliteIntrospector.Schema schema) {
        List<String> expected =
                new ArrayList<>(plan.tables().stream().map(PlannedTable::name).toList());
        if (plan.metadata() != null) {
            expected.add(plan.metadata().columnsTable());
            expected.add(plan.metadata().relationshipsTable());
            expected.add(plan.metadata().exportTable());
        }
        expected.sort(String.CASE_INSENSITIVE_ORDER);
        List<String> actual = new ArrayList<>(schema.tableNames());
        actual.sort(String.CASE_INSENSITIVE_ORDER);
        if (!expected.equals(actual)) {
            differences.add(new Difference(
                    null, null, "the set of tables", String.join(", ", expected), String.join(", ", actual)));
        }
    }

    private void columns(PlannedTable table, SqliteIntrospector.Table actual) {
        List<String> expectedNames =
                table.columns().stream().map(PlannedColumn::name).toList();
        List<String> actualNames =
                actual.columns().stream().map(SqliteIntrospector.Column::name).toList();
        if (!expectedNames.equals(actualNames)) {
            differences.add(new Difference(
                    table.name(),
                    null,
                    "the columns",
                    String.join(", ", expectedNames),
                    String.join(", ", actualNames)));
            return;
        }
        for (PlannedColumn column : table.columns()) {
            SqliteIntrospector.Column found = actual.column(column.name()).orElseThrow();
            if (!column.declaredType().equals(found.declaredType())) {
                differences.add(new Difference(
                        table.name(), column.name(), "the declared type", column.declaredType(), found.declaredType()));
            }
            if (column.notNull() != found.notNull()) {
                differences.add(new Difference(
                        table.name(),
                        column.name(),
                        "NOT NULL",
                        String.valueOf(column.notNull()),
                        String.valueOf(found.notNull())));
            }
            String expectedDefault = unwrap(column.defaultSql());
            String actualDefault = unwrap(found.defaultValue());
            if (!java.util.Objects.equals(expectedDefault, actualDefault)) {
                differences.add(new Difference(
                        table.name(),
                        column.name(),
                        "the default",
                        String.valueOf(expectedDefault),
                        String.valueOf(actualDefault)));
            }
        }
    }

    /** SQLite reports an expression default without the parentheses the DDL needs. */
    private static String unwrap(String sql) {
        if (sql == null) {
            return null;
        }
        String text = sql.strip();
        return text.startsWith("(") && text.endsWith(")")
                ? text.substring(1, text.length() - 1).strip()
                : text;
    }

    private void primaryKey(PlannedTable table, SqliteIntrospector.Table actual) {
        List<IndexColumn> expected =
                table.primaryKey() == null ? List.of() : table.primaryKey().columns();
        List<IndexColumn> found = actual.primaryKey();
        if (!sameColumns(expected, found)) {
            differences.add(new Difference(table.name(), null, "the primary key", describe(expected), describe(found)));
        }
    }

    private void indexes(PlannedTable table, SqliteIntrospector.Table actual, SqliteIntrospector.Schema schema) {
        List<PlannedIndex> expected = table.indexes();
        // Only indexes we created: SQLite adds its own for UNIQUE constraints, which this target doesn't use
        List<SqliteIntrospector.Index> found =
                actual.indexes().stream().filter(i -> "c".equals(i.origin())).toList();
        if (expected.size() != found.size()) {
            differences.add(new Difference(
                    table.name(),
                    null,
                    "the number of indexes",
                    expected.size() + " ("
                            + expected.stream().map(PlannedIndex::name).collect(Collectors.joining(", ")) + ")",
                    found.size() + " ("
                            + found.stream().map(SqliteIntrospector.Index::name).collect(Collectors.joining(", "))
                            + ")"));
            return;
        }
        for (PlannedIndex index : expected) {
            SqliteIntrospector.Index match = found.stream()
                    .filter(i -> i.name().equals(index.name()))
                    .findFirst()
                    .orElse(null);
            if (match == null) {
                differences.add(new Difference(table.name(), index.name(), "the index", "present", "missing"));
                continue;
            }
            if (index.unique() != match.unique()) {
                differences.add(new Difference(
                        table.name(),
                        index.name(),
                        "UNIQUE",
                        String.valueOf(index.unique()),
                        String.valueOf(match.unique())));
            }
            if (!sameColumns(index.columns(), match.columns())) {
                differences.add(new Difference(
                        table.name(),
                        index.name(),
                        "the index columns",
                        describe(index.columns()),
                        describe(match.columns())));
            }
            String expectedWhere = index.where();
            String actualWhere = match.where().orElse(null);
            if (!java.util.Objects.equals(expectedWhere, actualWhere)) {
                differences.add(new Difference(
                        table.name(),
                        index.name(),
                        "the partial-index predicate",
                        String.valueOf(expectedWhere),
                        String.valueOf(actualWhere)));
            }
            statement(
                    table.name(),
                    "index",
                    index.createIndexSql(),
                    schema.statements().get("index " + index.name()));
        }
    }

    private void foreignKeys(PlannedTable table, SqliteIntrospector.Table actual) {
        List<PlannedForeignKey> expected = table.foreignKeys();
        List<SqliteIntrospector.ForeignKey> found = actual.foreignKeys();
        if (expected.size() != found.size()) {
            differences.add(new Difference(
                    table.name(),
                    null,
                    "the number of foreign keys",
                    String.valueOf(expected.size()),
                    String.valueOf(found.size())));
            return;
        }
        for (PlannedForeignKey fk : expected) {
            SqliteIntrospector.ForeignKey match = found.stream()
                    .filter(f -> f.parentTable().equalsIgnoreCase(fk.parentTable())
                            && f.childColumns().equals(fk.childColumns())
                            && f.parentColumns().equals(fk.parentColumns()))
                    .findFirst()
                    .orElse(null);
            String name = fk.source().name();
            if (match == null) {
                differences.add(new Difference(
                        table.name(),
                        name,
                        "the foreign key",
                        fk.childColumns() + " -> " + fk.parentTable() + " " + fk.parentColumns(),
                        "missing"));
                continue;
            }
            if (!action(fk.onUpdate()).equals(match.onUpdate())) {
                differences.add(
                        new Difference(table.name(), name, "ON UPDATE", action(fk.onUpdate()), match.onUpdate()));
            }
            if (!action(fk.onDelete()).equals(match.onDelete())) {
                differences.add(
                        new Difference(table.name(), name, "ON DELETE", action(fk.onDelete()), match.onDelete()));
            }
        }
    }

    /** The whole statement as SQLite stored it must be the one the planner wrote. */
    private void statement(String table, String kind, String expected, String actual) {
        if (!expected.equals(actual)) {
            differences.add(new Difference(table, null, "the " + kind + "'s SQL", expected, String.valueOf(actual)));
        }
    }

    private static String action(Action action) {
        return switch (action) {
            case NO_ACTION -> "NO ACTION";
            case CASCADE -> "CASCADE";
            case SET_NULL -> "SET NULL";
        };
    }

    private static boolean sameColumns(List<IndexColumn> expected, List<IndexColumn> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (int i = 0; i < expected.size(); i++) {
            if (!expected.get(i).name().equals(actual.get(i).name())
                    || expected.get(i).ascending() != actual.get(i).ascending()) {
                return false;
            }
        }
        return true;
    }

    private static String describe(List<IndexColumn> columns) {
        return columns.isEmpty()
                ? "none"
                : columns.stream()
                        .map(c -> c.name() + (c.ascending() ? "" : " DESC"))
                        .collect(Collectors.joining(", "));
    }

    // ---------------------------------------------------------------- data

    /**
     * Compares every value of a table. The output is read in rowid order, which is the order the rows were written
     * in, so the two streams line up without sorting either side. A complex child table's rowid is Access's value id,
     * not the order its values were read in, so its rows are compared regardless of order.
     */
    private void data(Connection db, PlannedTable table) throws SQLException, IOException {
        List<PlannedColumn> columns = table.columns();
        List<String> names = columns.stream().map(PlannedColumn::name).toList();
        ConvertOptions options = plan.options();
        List<RowComparison.Column> compared = columns.stream()
                .map(c -> new RowComparison.Column(
                        c.name(),
                        BinaryCells.comparedType(c.source(), options),
                        c.sourceIndex(),
                        c.fractionDigits(),
                        value -> BinaryCells.expected(c.source(), c.olePart(), value, options),
                        value -> BinaryCells.readBack(value, c.source(), options, outputDirectory)))
                .toList();
        try (Statement statement = db.createStatement();
                ResultSet actual = statement.executeQuery(SqliteIntrospector.selectAll(table.name(), names))) {
            RowComparison.OutputRows rows = new RowComparison.OutputRows() {
                @Override
                public boolean next() throws SQLException {
                    return actual.next();
                }

                @Override
                public Object value(int column) throws SQLException {
                    return actual.getObject(column + 1);
                }
            };
            if (table.source().isComplexChild()) {
                int[] key = table.primaryKey() == null
                        ? java.util.stream.IntStream.range(0, columns.size()).toArray()
                        : new int[] {0};
                differences.rows(RowComparison.unordered(
                        table.name(), compared, key, source.rows(table.source()), rows, differences));
            } else {
                differences.rows(
                        RowComparison.ordered(table.name(), compared, source.rows(table.source()), rows, differences));
            }
        }
    }
}
