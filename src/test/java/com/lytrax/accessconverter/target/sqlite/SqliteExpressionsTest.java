package com.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.extract.ExpressionTranslator;
import com.lytrax.accessconverter.model.CheckRule;
import com.lytrax.accessconverter.model.DefaultValue;
import com.lytrax.accessconverter.target.sqlite.SqliteExpressions.Kind;
import java.util.Optional;
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
        SqliteExpressions.Result result = SqliteExpressions.defaultClause(value.expr(), kind, 0);
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
        SqliteExpressions.Result result = SqliteExpressions.defaultClause(value.expr(), kind, 0);
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
    void textIsComparedCaseInsensitivelyAsAccessDoes() {
        CheckRule rule = ExpressionTranslator.translateColumnRule("<>\"\"", null, "Name");
        assertThat(SqliteExpressions.check(rule.expr(), COLUMNS).sql()).isEqualTo("\"Name\" COLLATE NOCASE <> ''");
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
        assertThat(SqliteExpressions.check(starts.expr(), COLUMNS).sql())
                .isEqualTo("\"Name\" COLLATE NOCASE LIKE 'A%'");
        CheckRule percent = ExpressionTranslator.translateColumnRule("Like \"100%\"", null, "Name");
        assertThat(SqliteExpressions.check(percent.expr(), COLUMNS).sql())
                .isEqualTo("\"Name\" COLLATE NOCASE LIKE '100\\%' ESCAPE '\\'");
        CheckRule one = ExpressionTranslator.translateColumnRule("Like \"A?C\"", null, "Name");
        assertThat(SqliteExpressions.check(one.expr(), COLUMNS).sql()).isEqualTo("\"Name\" COLLATE NOCASE LIKE 'A_C'");
    }

    @Test
    void aRuleSqliteCannotExpressIsReportedNotGuessed() {
        CheckRule digits = ExpressionTranslator.translateColumnRule("Like \"##\"", null, "Code");
        SqliteExpressions.Result result = SqliteExpressions.check(digits.expr(), COLUMNS);
        assertThat(result.isPresent()).isFalse();
        assertThat(result.problem()).contains("# wildcard");

        CheckRule missing = ExpressionTranslator.translateTableRule("[Gone]>0", null);
        assertThat(SqliteExpressions.check(missing.expr(), COLUMNS).problem())
                .isEqualTo("it refers to Gone, which is not written");
    }
}
