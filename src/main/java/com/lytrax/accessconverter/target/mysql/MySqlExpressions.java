package com.lytrax.accessconverter.target.mysql;

import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.model.expr.Expr;
import com.lytrax.accessconverter.model.expr.LikePattern;
import com.lytrax.accessconverter.profile.RuleEvaluator;
import com.lytrax.accessconverter.target.CheckRenderer;
import com.lytrax.accessconverter.target.Rendered;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

/**
 * Renders the Access expression subset ({@link Expr}) as MySQL/MariaDB SQL, for {@code DEFAULT} clauses and
 * {@code CHECK} constraints (05, Constraints and attributes). A default that doesn't fit its column (text too long,
 * a number out of range, anything on a BLOB) comes back as a mismatch to report, never as SQL the server would
 * reject in strict mode.
 */
public final class MySqlExpressions {

    /** A new Replication ID in Access's <code>{XXXXXXXX-…}</code> form; verified on both dialects (05). */
    public static final String RANDOM_GUID = "(CONCAT('{', UPPER(UUID()), '}'))";

    /**
     * A Random autonumber's next value: a random signed 32-bit integer, as Access generates. {@code RAND()} has about
     * 2^30 distinct values on both servers, so two calls make the 16-bit halves. Measured on all five servers (05).
     */
    public static final String RANDOM_INT = "(FLOOR(RAND() * 65536) * 65536 + FLOOR(RAND() * 65536) - 2147483648)";

    /** Access's {@code Date()}: today at midnight. */
    public static final String TODAY = "(CURRENT_DATE)";

    /** Access's {@code Time()}: the time of day on Access's day zero, which is how a time-only value is stored. */
    public static final String TIME_OF_DAY = "(TIMESTAMP('1899-12-30', CURRENT_TIME))";

    private static final BigDecimal FLOAT_MAX = new BigDecimal(Float.MAX_VALUE);

    /**
     * What an expression needs to know about a column.
     *
     * @param sqlType the declared type, e.g. {@code VARCHAR(50)}
     * @param maxChars the most characters a {@code VARCHAR}/{@code CHAR} holds; null for other types
     * @param lob a {@code TEXT}/{@code LONGTEXT}/{@code LONGBLOB} column, whose default must be an expression
     */
    public record Column(
            String name,
            AccessType type,
            String sqlType,
            Integer maxChars,
            boolean lob,
            int fractionDigits,
            boolean notNull,
            Integer precision,
            Integer scale,
            boolean binary) {

        boolean isText() {
            return type.isText();
        }
    }

    /** Looks up the output columns an expression refers to. */
    public interface Columns {
        /** The column, or empty when it isn't written. */
        Optional<Column> column(String accessName);
    }

    private MySqlExpressions() {}

    // ---------------------------------------------------------------- defaults

    /** A {@code DEFAULT} value for this column, or why the Access default can't be one. */
    public static Rendered defaultClause(Expr expr, Column column) {
        if (expr instanceof Expr.NullLiteral) {
            // Omitting DEFAULT means DEFAULT NULL for a nullable column; a NOT NULL one can't take it
            return column.notNull()
                    ? Rendered.mismatch("the column is NOT NULL, so NULL can't be its default")
                    : Rendered.of(null);
        }
        return switch (column.type()) {
            case BOOLEAN -> booleanLiteral(expr);
            case BYTE -> integer(expr, 0, 255);
            case INT -> integer(expr, Short.MIN_VALUE, Short.MAX_VALUE);
            case LONG, AUTONUMBER_LONG, ATTACHMENT, MULTI_VALUE, VERSION_HISTORY, COMPLEX_UNSUPPORTED ->
                integer(expr, Integer.MIN_VALUE, Integer.MAX_VALUE);
            case BIG_INT -> integer(expr, Long.MIN_VALUE, Long.MAX_VALUE);
            case MONEY, NUMERIC -> decimal(expr, column.precision(), column.scale());
            case FLOAT, DOUBLE -> approximate(expr, column.type() == AccessType.FLOAT);
            case SHORT_DATE_TIME, EXT_DATE_TIME -> date(expr, column.fractionDigits());
            case TEXT, MEMO, HYPERLINK -> text(expr, column);
            case GUID, AUTONUMBER_GUID -> guid(expr);
            case BINARY, OLE, UNSUPPORTED -> Rendered.mismatch("a binary column takes no default value");
        };
    }

    private static Rendered booleanLiteral(Expr expr) {
        return switch (expr) {
            case Expr.BooleanLiteral b -> Rendered.of(b.value() ? "1" : "0");
            // Access stores Yes as -1; any non-zero number means Yes
            case Expr.NumberLiteral n -> Rendered.of(n.value().signum() == 0 ? "0" : "1");
            default -> Rendered.mismatch("a Yes/No column takes no " + describe(expr) + " value");
        };
    }

    private static Rendered integer(Expr expr, long min, long max) {
        Optional<BigDecimal> value = number(expr);
        if (value.isEmpty()) {
            return Rendered.mismatch("a number column takes no " + describe(expr) + " value");
        }
        BigDecimal v = value.get().stripTrailingZeros();
        if (v.scale() > 0) {
            return Rendered.mismatch("the value " + v.toPlainString() + " is not a whole number");
        }
        BigInteger whole = v.toBigIntegerExact();
        if (whole.compareTo(BigInteger.valueOf(min)) < 0 || whole.compareTo(BigInteger.valueOf(max)) > 0) {
            return Rendered.mismatch("the value " + whole + " is outside the column's range");
        }
        return Rendered.of(whole.toString());
    }

    private static Rendered decimal(Expr expr, Integer precision, Integer scale) {
        Optional<BigDecimal> value = number(expr);
        if (value.isEmpty()) {
            return Rendered.mismatch("a number column takes no " + describe(expr) + " value");
        }
        BigDecimal v = value.get().stripTrailingZeros();
        int p = precision == null ? 18 : precision;
        int s = scale == null ? 0 : scale;
        if (Math.max(v.scale(), 0) > s || v.precision() - v.scale() > p - s) {
            return Rendered.mismatch("the value " + v.toPlainString() + " doesn't fit DECIMAL(" + p + "," + s + ")");
        }
        return Rendered.of(v.toPlainString());
    }

    private static Rendered approximate(Expr expr, boolean single) {
        Optional<BigDecimal> value = number(expr);
        if (value.isEmpty()) {
            return Rendered.mismatch("a number column takes no " + describe(expr) + " value");
        }
        if (single && value.get().abs().compareTo(FLOAT_MAX) > 0) {
            return Rendered.mismatch("the value " + value.get().toPlainString() + " is outside a Single's range");
        }
        return Rendered.of(value.get().toPlainString());
    }

    /** A number an Access default stands for: a number, a Yes/No (-1/0 as Access stores it) or numeric text. */
    private static Optional<BigDecimal> number(Expr expr) {
        return switch (expr) {
            case Expr.NumberLiteral n -> Optional.of(n.value());
            case Expr.BooleanLiteral b -> Optional.of(b.value() ? BigDecimal.ONE.negate() : BigDecimal.ZERO);
            case Expr.StringLiteral s -> parse(s.value());
            default -> Optional.empty();
        };
    }

    private static Rendered date(Expr expr, int fractionDigits) {
        return switch (expr) {
            case Expr.DateTimeLiteral d ->
                Rendered.of(
                        MySqlLiterals.dateTime(MySqlLiterals.atPrecision(d.value(), fractionDigits), fractionDigits));
            case Expr.CurrentDateTime c ->
                Rendered.of(
                        switch (c.part()) {
                            // The precision must match the column's, or the server rejects the default
                            case NOW ->
                                fractionDigits == 0 ? "CURRENT_TIMESTAMP" : "CURRENT_TIMESTAMP(" + fractionDigits + ")";
                            case DATE -> TODAY;
                            case TIME -> TIME_OF_DAY;
                        });
            default -> Rendered.mismatch("a date column takes no " + describe(expr) + " value");
        };
    }

    private static Rendered text(Expr expr, Column column) {
        if (expr instanceof Expr.NewGuid) {
            if (column.maxChars() != null && column.maxChars() < 38) {
                return Rendered.mismatch("a new GUID has 38 characters, and the column holds " + column.maxChars());
            }
            return Rendered.of(RANDOM_GUID);
        }
        String text =
                switch (expr) {
                    case Expr.StringLiteral s -> s.value();
                    // Access stores a number default of a text column as its text
                    case Expr.NumberLiteral n -> n.value().toPlainString();
                    case Expr.DateTimeLiteral d -> MySqlLiterals.dateTimeText(d.value(), 0);
                    default -> null;
                };
        if (text == null) {
            return Rendered.mismatch("a text column takes no " + describe(expr) + " value");
        }
        if (MySqlLiterals.hasUnpairedSurrogate(text)) {
            return Rendered.mismatch("the text holds an unpaired surrogate, which UTF-8 can't encode");
        }
        if (column.maxChars() != null && text.codePointCount(0, text.length()) > column.maxChars()) {
            return Rendered.mismatch("the text is longer than the column's " + column.maxChars() + " characters");
        }
        String literal = MySqlLiterals.string(text);
        // MySQL rejects a literal default on TEXT/BLOB (error 1101); an expression is accepted on both dialects
        return Rendered.of(column.lob() ? "(" + literal + ")" : literal);
    }

    private static Rendered guid(Expr expr) {
        return switch (expr) {
            case Expr.NewGuid g -> Rendered.of(RANDOM_GUID);
            case Expr.StringLiteral s ->
                s.value().length() <= 38 && !MySqlLiterals.hasUnpairedSurrogate(s.value())
                        ? Rendered.of(MySqlLiterals.string(s.value()))
                        : Rendered.mismatch("the text doesn't fit a GUID column");
            default -> Rendered.mismatch("a GUID column takes no " + describe(expr) + " value");
        };
    }

    // ---------------------------------------------------------------- checks

    /**
     * A {@code CHECK} expression, or why MySQL can't express the rule. Text is compared the way Access compares it:
     * case-insensitively through the collation (or an explicit accent-sensitive, case-insensitive one when the
     * chosen collation isn't) and with trailing spaces ignored, which a NO PAD collation such as
     * {@code utf8mb4_0900_as_ci} doesn't do by itself. {@code Like} sees the value as it is, as in Access.
     *
     * @param caseInsensitiveCollation the name of an accent-sensitive, case-insensitive collation to compare text
     *     with, or null when the table collation already is one
     * @param defaultCollation the dialect's default collation, for a column that has a binary one of its own
     */
    public static Rendered check(Expr expr, Columns columns, String caseInsensitiveCollation, String defaultCollation) {
        return new Checks(columns, caseInsensitiveCollation, defaultCollation).check(expr);
    }

    private static final class Checks extends CheckRenderer {
        private final Columns columns;
        private final String collate;
        private final String binaryCollate;

        Checks(Columns columns, String caseInsensitiveCollation, String defaultCollation) {
            this.columns = columns;
            this.collate = caseInsensitiveCollation == null ? "" : " COLLATE " + caseInsensitiveCollation;
            this.binaryCollate = " COLLATE " + defaultCollation;
        }

        /** How a text column is compared in a CHECK: case-insensitively, as Access compares it. */
        private String collate(Column column) {
            return column.binary() ? binaryCollate : collate;
        }

        private Column column(Expr.ColumnRef ref) {
            return columns.column(ref.name())
                    .orElseThrow(() -> unrenderable("it refers to " + ref.name() + ", which is not written"));
        }

        @Override
        protected String operand(Expr.ColumnRef ref) {
            Column column = column(ref);
            String name = MySqlLiterals.identifier(column.name());
            return column.isText() ? "RTRIM(" + name + ")" + collate(column) : name;
        }

        @Override
        protected String value(Expr expr, Expr.ColumnRef beside) {
            if (expr instanceof Expr.NullLiteral) {
                return "NULL";
            }
            Column column = beside == null ? null : column(beside);
            if (column != null && column.isText()) {
                return switch (expr) {
                    // Access ignores trailing spaces when it compares text; the column side is RTRIM()med too
                    case Expr.StringLiteral s -> text(RuleEvaluator.withoutTrailingSpaces(s.value()));
                    case Expr.NumberLiteral n -> text(n.value().toPlainString());
                    default -> throw unrenderable("a text column is compared with a " + describe(expr) + " value");
                };
            }
            if (column != null && column.type().isDateTime()) {
                if (expr instanceof Expr.DateTimeLiteral d) {
                    int digits = column.fractionDigits();
                    return MySqlLiterals.dateTime(MySqlLiterals.atPrecision(d.value(), digits), digits);
                }
                throw unrenderable("a date column is compared with a " + describe(expr) + " value");
            }
            if (column != null && column.type() == AccessType.BOOLEAN) {
                // The output stores Yes as 1 where Access stores -1
                return switch (expr) {
                    case Expr.BooleanLiteral b -> b.value() ? "1" : "0";
                    case Expr.NumberLiteral n ->
                        n.value().compareTo(BigDecimal.ONE.negate()) == 0
                                ? "1"
                                : n.value().toPlainString();
                    default -> throw unrenderable("a Yes/No column is compared with a " + describe(expr) + " value");
                };
            }
            if (column != null && (column.type() == AccessType.GUID || column.type() == AccessType.AUTONUMBER_GUID)) {
                if (expr instanceof Expr.StringLiteral s) {
                    return text(s.value());
                }
                throw unrenderable("a GUID column is compared with a " + describe(expr) + " value");
            }
            return switch (expr) {
                case Expr.NumberLiteral n -> n.value().toPlainString();
                case Expr.BooleanLiteral b -> b.value() ? "-1" : "0";
                case Expr.StringLiteral s ->
                    column == null
                            ? text(s.value())
                            : parse(s.value())
                                    .map(BigDecimal::toPlainString)
                                    .orElseThrow(() -> unrenderable("a number column is compared with text"));
                case Expr.DateTimeLiteral d -> MySqlLiterals.dateTime(MySqlLiterals.atPrecision(d.value(), 0), 0);
                default -> throw unrenderable("MySQL can't compare with a " + describe(expr) + " value here");
            };
        }

        private String text(String value) {
            if (MySqlLiterals.hasUnpairedSurrogate(value)) {
                throw unrenderable("the text holds an unpaired surrogate, which UTF-8 can't encode");
            }
            return MySqlLiterals.string(value);
        }

        @Override
        protected String like(Expr.Like like, String operand) {
            LikePattern pattern = like.pattern();
            if (!pattern.mapsToSqlLike()) {
                throw unrenderable("MySQL's LIKE has no equivalent of the # wildcard in \"" + pattern.access() + "\"");
            }
            if (!(like.operand() instanceof Expr.ColumnRef ref) || !column(ref).isText()) {
                throw unrenderable("Like is applied to something other than a text column");
            }
            StringBuilder sql = new StringBuilder();
            boolean escaped = false;
            for (LikePattern.Element element : pattern.elements()) {
                switch (element) {
                    case LikePattern.Literal l -> {
                        for (char c : l.text().toCharArray()) {
                            if (c == '%' || c == '_' || c == '\\') {
                                sql.append('\\');
                                escaped = true;
                            }
                            sql.append(c);
                        }
                    }
                    case LikePattern.AnyString any -> sql.append('%');
                    case LikePattern.AnyChar one -> sql.append('_');
                    case LikePattern.AnyDigit d -> throw new IllegalStateException("excluded by mapsToSqlLike");
                }
            }
            // The value as it is, as Access matches it: no RTRIM() here
            return MySqlLiterals.identifier(column(ref).name()) + collate(column(ref)) + (like.negated() ? " NOT" : "")
                    + " LIKE " + text(sql.toString()) + (escaped ? " ESCAPE '\\\\'" : "");
        }
    }

    // ---------------------------------------------------------------- helpers

    private static Optional<BigDecimal> parse(String text) {
        try {
            return Optional.of(new BigDecimal(text.strip()));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private static String describe(Expr expr) {
        return switch (expr) {
            case Expr.StringLiteral s -> "text";
            case Expr.NumberLiteral n -> "number";
            case Expr.BooleanLiteral b -> "Yes/No";
            case Expr.DateTimeLiteral d -> "date";
            case Expr.CurrentDateTime c -> "current date";
            case Expr.NewGuid g -> "GenGUID()";
            default -> expr.getClass().getSimpleName();
        };
    }
}
