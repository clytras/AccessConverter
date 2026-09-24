package com.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.extract.ExpressionTranslator;
import com.lytrax.accessconverter.model.CheckRule;
import com.lytrax.accessconverter.model.DefaultValue;
import com.lytrax.accessconverter.model.expr.Expr;
import com.lytrax.accessconverter.profile.RuleEvaluator;
import com.lytrax.accessconverter.profile.RuleEvaluator.TextComparison;
import com.lytrax.accessconverter.profile.RuleEvaluator.Truth;
import com.lytrax.accessconverter.target.Rendered;
import com.lytrax.accessconverter.target.sqlite.SqliteExpressions.Kind;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Access defaults and validation rules as SQLite expresses them (06, Constraints and attributes). */
class SqliteExpressionsTest {

    /** Stands for the SQL string quote in {@code @CsvSource} rows, where it is the quote character itself. */
    private static final char SINGLE_QUOTE = '\'';

    /** The columns of a table with one of each kind, for the CHECK renderer. */
    private static final SqliteExpressions.Columns COLUMNS = new SqliteExpressions.Columns() {
        @Override
        public Optional<String> name(String accessName) {
            return "Gone".equalsIgnoreCase(accessName) ? Optional.empty() : Optional.of(accessName);
        }

        @Override
        public Kind kind(String accessName) {
            return switch (accessName.toLowerCase(java.util.Locale.ROOT)) {
                case "name", "code" -> Kind.TEXT;
                case "created", "enddate", "startdate" -> Kind.DATE;
                default -> Kind.NUMERIC;
            };
        }

        @Override
        public int fractionDigits(String accessName) {
            return "Created".equalsIgnoreCase(accessName) ? 3 : 0;
        }
    };

    @ParameterizedTest(name = "{0} on a {1} column -> {2}")
    @CsvSource(
            delimiter = ';',
            value = {
                "0;NUMERIC;0",
                "=0;NUMERIC;0",
                "-1.5;NUMERIC;-1.5",
                "Yes;NUMERIC;1",
                "No;NUMERIC;0",
                "Null;NUMERIC;NULL",
                "42;TEXT;@42@",
                "\"Greece\";TEXT;@Greece@",
                "#1/31/2000#;DATE;@2000-01-31 00:00:00@"
            })
    void defaultsBecomeSqliteDefaults(String access, Kind kind, String expected) {
        DefaultValue value = ExpressionTranslator.translateDefault(access);
        assertThat(value.isTranslated()).as(access).isTrue();
        Rendered result = SqliteExpressions.defaultClause(value.expr(), kind, 0);
        // '@' stands for the SQL string quote, which @CsvSource reads as its own quote character
        assertThat(result.sql()).isEqualTo(expected.replace('@', SINGLE_QUOTE));
    }

    @Test
    void theCurrentDateAndTimeDefaultsAreLocalAsAccessEvaluatesThem() {
        assertThat(defaultOf("Now()", Kind.DATE)).isEqualTo("(datetime('now','localtime'))");
        assertThat(defaultOf("Date()", Kind.DATE)).isEqualTo("(date('now','localtime') || ' 00:00:00')");
        // Access's Time() is a time on its day zero, which is how a time-only value is stored
        assertThat(defaultOf("Time()", Kind.DATE)).isEqualTo("('1899-12-30 ' || time('now','localtime'))");
        assertThat(defaultOf("Now()", Kind.TEXT)).isEqualTo("(datetime('now','localtime'))");
    }

    private static String defaultOf(String access, Kind kind) {
        DefaultValue value = ExpressionTranslator.translateDefault(access);
        assertThat(value.isTranslated()).as(access).isTrue();
        return SqliteExpressions.defaultClause(value.expr(), kind, 0).sql();
    }

    @Test
    void aStringDefaultIsQuotedAndEscaped() {
        DefaultValue value = ExpressionTranslator.translateDefault("\"it's\"");
        assertThat(SqliteExpressions.defaultClause(value.expr(), Kind.TEXT, 0).sql())
                .isEqualTo("'it''s'");
    }

    @Test
    void aGuidAutonumberGetsARandomGuidOfAccessesShape() {
        DefaultValue value = ExpressionTranslator.translateDefault("GenGUID()");
        assertThat(SqliteExpressions.defaultClause(value.expr(), Kind.GUID, 0).sql())
                .isEqualTo(SqliteExpressions.RANDOM_GUID)
                .contains("randomblob(4)")
                .contains("'{'")
                .contains("'}'");
    }

    @Test
    void aDateDefaultUsesItsColumnsFractionalDigits() {
        DefaultValue value = ExpressionTranslator.translateDefault("#1/31/2000#");
        assertThat(SqliteExpressions.defaultClause(value.expr(), Kind.DATE, 3).sql())
                .isEqualTo("'2000-01-31 00:00:00.000'");
    }

    @ParameterizedTest(name = "{0} on a {1} column is dropped")
    @CsvSource(
            delimiter = ';',
            value = {
                "\"abc\";NUMERIC",
                "Yes;TEXT",
                "0;DATE",
                "\"abc\";DATE",
                "0;BLOB",
                "Now();NUMERIC",
                "#1/31/2000#;NUMERIC"
            })
    void aDefaultThatDoesNotFitItsColumnIsReportedInsteadOfGuessed(String access, Kind kind) {
        DefaultValue value = ExpressionTranslator.translateDefault(access);
        Rendered result = SqliteExpressions.defaultClause(value.expr(), kind, 0);
        assertThat(result.isPresent()).isFalse();
        assertThat(result.typeMismatch()).isTrue();
        assertThat(result.problem()).isNotBlank();
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource(
            delimiter = ';',
            value = {
                ">0;\"Qty\" > 0",
                ">=0 And <=100;\"Qty\" >= 0 AND \"Qty\" <= 100",
                ">0 And <10 Or =99;\"Qty\" > 0 AND \"Qty\" < 10 OR \"Qty\" = 99",
                "<>0;\"Qty\" <> 0",
                "Between 1 And 10;\"Qty\" BETWEEN 1 AND 10",
                "In (1,2,3);\"Qty\" IN (1, 2, 3)",
                "Is Not Null;\"Qty\" IS NOT NULL",
                "Not >5;NOT \"Qty\" > 5",
                ">0 Or Is Null;\"Qty\" > 0 OR \"Qty\" IS NULL"
            })
    void columnRulesBecomeChecks(String access, String expected) {
        CheckRule rule = ExpressionTranslator.translateColumnRule(access, null, "Qty");
        assertThat(rule.isTranslated()).as(access).isTrue();
        assertThat(SqliteExpressions.check(rule.expr(), COLUMNS).sql()).isEqualTo(expected);
    }

    @Test
    void textIsComparedCaseInsensitivelyAndWithoutTrailingSpacesAsAccessDoes() {
        CheckRule rule = ExpressionTranslator.translateColumnRule("<>\"\"", null, "Name");
        assertThat(SqliteExpressions.check(rule.expr(), COLUMNS).sql())
                .isEqualTo("rtrim(\"Name\") COLLATE NOCASE <> ''");
        CheckRule in = ExpressionTranslator.translateColumnRule("In (\"A  \",\"B\t\")", null, "Name");
        assertThat(SqliteExpressions.check(in.expr(), COLUMNS).sql())
                .isEqualTo("rtrim(\"Name\") COLLATE NOCASE IN ('A', 'B\t')");
    }

    /**
     * What Access (ACE 16, a General-sort .accdb) accepted and rejected for each rule, measured through DAO in phase 6
     * (Access 97 agrees except that it ignores accents in a rule, which only makes the evaluator drop a CHECK). The
     * evaluator reproduces Access; the SQLite CHECK does exactly what the evaluator's SQLite comparison predicts, so
     * a CHECK the planner emits never rejects a row the profile passed.
     */
    @ParameterizedTest(name = "{0} with [{1}]: Access {2}")
    @CsvSource(
            delimiter = ';',
            value = {
                "In (\"A\",\"B\");A;accept",
                "In (\"A\",\"B\");A_;accept",
                "In (\"A\",\"B\");A___;accept",
                "In (\"A\",\"B\");a_;accept",
                "In (\"A\",\"B\");A<TAB>;reject",
                "In (\"A\",\"B\");A<NBSP>;reject",
                "In (\"A\",\"B\");A<CRLF>;reject",
                "In (\"A\",\"B\");_A;reject",
                "=\"A\";A__;accept",
                "=\"A\";A<TAB>;reject",
                "<>\"A\";A_;reject",
                "<>\"A\";A<TAB>;accept",
                "<>\"A\";e;accept",
                "In (\"Α\",\"Β\");α;accept",
                "In (\"Α\",\"Β\");ά;reject",
                "In (\"Α\",\"Β\");a;reject",
                "=\"ΟΔΟΣ\";οδος;accept",
                "=\"ΟΔΟΣ\";οδοσ;accept",
                "=\"ЖУК\";жук;accept",
                "=\"I\";i;accept",
                "=\"I\";ı;reject",
                "=\"É\";é;accept",
                "=\"É\";e;reject",
                "Like \"A\";A;accept",
                "Like \"A\";A_;reject",
                "Like \"A*\";a<TAB>;accept",
                "Like \"A*\";_A;reject"
            })
    void theCheckNeverRejectsWhatTheProfilePassed(String access, String stored, String measured) throws Exception {
        String value = stored.replace("_", " ")
                .replace("<TAB>", "\t")
                .replace("<NBSP>", " ")
                .replace("<CRLF>", "\r\n");
        Expr rule =
                ExpressionTranslator.translateColumnRule(access, null, "Name").expr();
        Function<String, Object> row = column -> value;

        Truth asAccess = new RuleEvaluator(TextComparison.ACCESS).test(rule, row);
        assertThat(asAccess != Truth.FALSE).as("Access semantics").isEqualTo(measured.equals("accept"));

        Truth asSqlite = new RuleEvaluator(TextComparison.ASCII_NOCASE).test(rule, row);
        try (Connection db = DriverManager.getConnection("jdbc:sqlite::memory:");
                Statement s = db.createStatement()) {
            s.execute("CREATE TABLE t (\"Name\" TEXT CHECK ("
                    + SqliteExpressions.check(rule, COLUMNS).sql() + "))");
            boolean accepted;
            try (PreparedStatement insert = db.prepareStatement("INSERT INTO t VALUES (?)")) {
                insert.setString(1, value);
                insert.execute();
                accepted = true;
            } catch (SQLException e) {
                assertThat(e.getMessage()).contains("CHECK constraint failed");
                accepted = false;
            }
            assertThat(accepted).as("SQLite").isEqualTo(asSqlite != Truth.FALSE);
        }
    }

    @Test
    void aTableRuleComparesTwoColumns() {
        CheckRule rule = ExpressionTranslator.translateTableRule("[EndDate]>=[StartDate]", null);
        assertThat(rule.isTranslated()).isTrue();
        assertThat(SqliteExpressions.check(rule.expr(), COLUMNS).sql()).isEqualTo("\"EndDate\" >= \"StartDate\"");
    }

    @Test
    void aDateInARuleIsWrittenTheWayItsColumnIs() {
        CheckRule rule = ExpressionTranslator.translateColumnRule(">#1/31/2000#", null, "Created");
        assertThat(SqliteExpressions.check(rule.expr(), COLUMNS).sql())
                .isEqualTo("\"Created\" > '2000-01-31 00:00:00.000'");
    }

    @Test
    void likePatternsBecomeSqlLikeWithEscapedWildcards() {
        CheckRule starts = ExpressionTranslator.translateColumnRule("Like \"A*\"", null, "Name");
        assertThat(SqliteExpressions.check(starts.expr(), COLUMNS).sql()).isEqualTo("\"Name\" LIKE 'A%'");
        CheckRule percent = ExpressionTranslator.translateColumnRule("Like \"100%\"", null, "Name");
        assertThat(SqliteExpressions.check(percent.expr(), COLUMNS).sql())
                .isEqualTo("\"Name\" LIKE '100\\%' ESCAPE '\\'");
        CheckRule one = ExpressionTranslator.translateColumnRule("Like \"A?C\"", null, "Name");
        assertThat(SqliteExpressions.check(one.expr(), COLUMNS).sql()).isEqualTo("\"Name\" LIKE 'A_C'");
    }

    @Test
    void aRuleSqliteCannotExpressIsReportedNotGuessed() {
        CheckRule digits = ExpressionTranslator.translateColumnRule("Like \"##\"", null, "Code");
        Rendered result = SqliteExpressions.check(digits.expr(), COLUMNS);
        assertThat(result.isPresent()).isFalse();
        assertThat(result.problem()).contains("# wildcard");

        CheckRule missing = ExpressionTranslator.translateTableRule("[Gone]>0", null);
        assertThat(SqliteExpressions.check(missing.expr(), COLUMNS).problem())
                .isEqualTo("it refers to Gone, which is not written");
    }
}
