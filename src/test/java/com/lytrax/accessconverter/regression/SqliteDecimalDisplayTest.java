package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.Access97Fixture;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.source.RowStream;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.sqlite.Sqlite;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture.Converted;
import com.lytrax.accessconverter.target.sqlite.SqliteOptions;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedColumn;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedTable;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * F-06, pinned where a reader sees it: every Currency and Decimal value must read back through SQLite's own text
 * rendering exactly as Access has it, not only round-trip as a double.
 *
 * <p>SQLite renders a REAL with about 17 significant digits, so a decimal only survives its own display while the
 * double carries the decimal's text form. Measured on SQLite 3.53.4 over random decimals: 13 significant digits came
 * back unchanged 40 of 40 times, 14 failed 14 times and 15 failed 31 times. That is why the planner keeps at most 13
 * digits as a number and everything longer as exact text. A future change to the threshold or to the storage that
 * loses a digit fails here.
 */
class SqliteDecimalDisplayTest {

    @TempDir
    static Path dir;

    static Stream<Case> databases() {
        return Stream.of(
                new Case("schemaFidelity", GeneratedFixture.SCHEMA_FIDELITY.path(), OpenOptions.DEFAULT),
                corpus("jackcess/V2010/calcFieldV2010.accdb"),
                corpus("jackcess/V1997/common1V1997.mdb"),
                corpus("jackcess/V2007/fixedNumericV2007.accdb"),
                corpus("jackcess-encrypt/money2002.mny"),
                new Case("gr97", Access97Fixture.GR97.file(), Access97Fixture.GR97.openOptions()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("databases")
    void everyDecimalReadsBackAsAccessHasIt(Case database) throws IOException {
        Converted converted = SqliteFixture.convert(
                database.file(),
                dir.resolve(database.id() + ".sqlite3"),
                database.options(),
                ConvertOptions.DEFAULT,
                SqliteOptions.DEFAULT);
        long compared = 0;
        try (AccessSource source = AccessSource.open(database.file(), database.options(), new Issues());
                Sqlite sqlite = converted.open()) {
            for (PlannedTable table : converted.plan().tables()) {
                List<PlannedColumn> decimals = table.columns().stream()
                        .filter(c -> c.source().type().isExactNumeric())
                        .toList();
                if (decimals.isEmpty()) {
                    continue;
                }
                List<List<String>> expected = accessValues(source, table, decimals);
                List<List<String>> actual = sqliteText(sqlite, table, decimals);
                assertThat(actual)
                        .as(database.id() + "." + table.name() + " "
                                + decimals.stream().map(PlannedColumn::name).toList())
                        .isEqualTo(expected);
                compared += (long) expected.size() * decimals.size();
            }
        }
        assertThat(compared).as("values compared").isPositive();
    }

    /** The case the plan was written from: 15 significant digits, which a REAL cannot show unchanged. */
    @Test
    void theFifteenDigitCaseIsStoredAsTextAndDisplaysExactly() {
        Converted converted = SqliteFixture.convert(
                CorpusFile.get("jackcess/V2010/calcFieldV2010.accdb").file(), dir.resolve("weeklySalary.sqlite3"));
        try (Sqlite sqlite = converted.open()) {
            assertThat(sqlite.value("SELECT typeof(WeeklySalary) FROM Table1 WHERE ID = 1"))
                    .isEqualTo("text");
            // As a REAL this read back as 19230.769230769201
            assertThat(sqlite.value("SELECT CAST(WeeklySalary AS TEXT) FROM Table1 WHERE ID = 1"))
                    .isEqualTo("19230.7692307692");
            assertThat(sqlite.value("SELECT quote(WeeklySalary) FROM Table1 WHERE ID = 1"))
                    .isEqualTo("'19230.7692307692'");
            // Text sorts and aggregates as text; CAST makes it a number again (the report says so per column)
            assertThat(sqlite.value("SELECT CAST(WeeklySalary AS REAL) > 19000 FROM Table1 WHERE ID = 1"))
                    .isEqualTo(1);
        }
    }

    /** Access's own values, normalized so only the digits that matter are compared. */
    private static List<List<String>> accessValues(
            AccessSource source, PlannedTable table, List<PlannedColumn> decimals) throws IOException {
        List<List<String>> rows = new ArrayList<>();
        RowStream stream = source.rows(table.source());
        while (stream.hasNext()) {
            Object[] row = stream.next();
            List<String> values = new ArrayList<>();
            for (PlannedColumn column : decimals) {
                Object value = row[column.sourceIndex()];
                values.add(value == null ? null : normalize(((BigDecimal) value).toPlainString()));
            }
            rows.add(values);
        }
        return rows;
    }

    /** What SQLite itself shows: the same conversion the {@code sqlite3} shell, {@code quote()} and a dump use. */
    private static List<List<String>> sqliteText(Sqlite sqlite, PlannedTable table, List<PlannedColumn> decimals) {
        String columns = String.join(
                ", ",
                decimals.stream()
                        .map(c -> "CAST(\"" + c.name().replace("\"", "\"\"") + "\" AS TEXT)")
                        .toList());
        return sqlite
                .query("SELECT " + columns + " FROM \"" + table.name().replace("\"", "\"\"") + "\" ORDER BY _rowid_")
                .stream()
                .map(row -> row.stream()
                        .map(value -> value == null ? null : normalize(String.valueOf(value)))
                        .toList())
                .toList();
    }

    /** Trailing zeros and the decimal point carry no value; every other digit must match. */
    private static String normalize(String decimal) {
        return new BigDecimal(decimal).stripTrailingZeros().toPlainString();
    }

    record Case(String id, Path file, OpenOptions options) {
        @Override
        public String toString() {
            return id;
        }
    }

    private static Case corpus(String id) {
        CorpusFile file = CorpusFile.get(id);
        return new Case(file.fileName(), file.file(), file.openOptions());
    }
}
