package io.lytrax.accessconverter.target.sqlite;

import io.lytrax.accessconverter.model.expr.Expr;
import io.lytrax.accessconverter.model.expr.LikePattern;
import io.lytrax.accessconverter.profile.RuleEvaluator;
import io.lytrax.accessconverter.target.CheckRenderer;
import io.lytrax.accessconverter.target.IdentifierPolicy;
import io.lytrax.accessconverter.target.Rendered;
import java.math.BigDecimal;
import java.util.Optional;

/**
 * Renders the Access expression subset ({@link Expr}) as SQLite SQL, for {@code DEFAULT} clauses and {@code CHECK}
 * constraints (06, Constraints and attributes). Anything the subset allows but SQLite can't express, and any
 * default that doesn't fit its column, comes back as a problem to report instead of a guess.
 *
 * <p>Text is compared as {@code rtrim(col) COLLATE NOCASE} against a literal without its trailing spaces, because
 * Access compares text case-insensitively and ignores trailing spaces (U+0020 only; measured in Access 97 and ACE 16,
 * phase 6): a rule Access accepted must never reject the same row here. NOCASE folds ASCII letters only, so the
 * planner emits a CHECK only when the data also satisfies the rule compared that way
 * ({@code RuleEvaluator.TextComparison.ASCII_NOCASE}). {@code Like} sees the value as it is, as in Access.
 */
public final class SqliteExpressions {

    /** A random GUID in Access's <code>{XXXXXXXX-…}</code> form, for a Replication ID autonumber. */
    public static final String RANDOM_GUID = "('{' || upper(hex(randomblob(4))) || '-' || upper(hex(randomblob(2)))"
            + " || '-' || upper(hex(randomblob(2))) || '-' || upper(hex(randomblob(2))) || '-'"
            + " || upper(hex(randomblob(6))) || '}')";

    /** Access's {@code Now()}: local time, in the format the date columns use. */
    public static final String NOW = "(datetime('now','localtime'))";

    /** Access's {@code Date()}: today at midnight. */
    public static final String TODAY = "(date('now','localtime') || ' 00:00:00')";

    /** Access's {@code Time()}: the time of day on Access's day zero, which is how time-only values are stored. */
    public static final String TIME_OF_DAY = "('1899-12-30 ' || time('now','localtime'))";

    /** What a column holds, which decides how a literal beside it is rendered. */
    public enum Kind {
        /** INTEGER, REAL or NUMERIC affinity: booleans, integers, floats and exact decimals kept as numbers. */
        NUMERIC,
        TEXT,
        /** ISO-8601 text. */
        DATE,
        GUID,
        BLOB
    }

    /** What the planner knows about the columns an expression refers to. */
    public interface Columns {
        /** The column's output name, or empty when it isn't written (a dropped column). */
        Optional<String> name(String accessName);

        Kind kind(String accessName);

        int fractionDigits(String accessName);
    }

    private SqliteExpressions() {}

    /** A {@code DEFAULT} clause for a column of this kind, or why the Access default can't become one. */
    public static Rendered defaultClause(Expr expr, Kind kind, int fractionDigits) {
        return switch (kind) {
            case NUMERIC -> numericLiteral(expr);
            case TEXT -> textLiteral(expr);
            case DATE -> dateLiteral(expr, fractionDigits);
            case GUID -> guidLiteral(expr);
            case BLOB ->
                expr instanceof Expr.NullLiteral
                        ? Rendered.of("NULL")
                        : Rendered.mismatch("a BLOB column takes no default value");
        };
    }

    /** A {@code CHECK} expression, or why SQLite can't express the rule. */
    public static Rendered check(Expr expr, Columns columns) {
        return new Renderer(columns).check(expr);
    }

    private static Rendered numericLiteral(Expr expr) {
        return switch (expr) {
            case Expr.NumberLiteral n -> Rendered.of(n.value().toPlainString());
            case Expr.BooleanLiteral b -> Rendered.of(b.value() ? "1" : "0");
            case Expr.NullLiteral n -> Rendered.of("NULL");
            case Expr.StringLiteral s ->
                number(s.value())
                        .map(v -> Rendered.of(v.toPlainString()))
                        .orElseGet(() -> Rendered.mismatch("the value " + quote(s.value()) + " is not a number"));
            default -> Rendered.mismatch("a number column takes no " + describe(expr) + " value");
        };
    }

    private static Rendered textLiteral(Expr expr) {
        return switch (expr) {
            case Expr.StringLiteral s -> Rendered.of(IdentifierPolicy.literal(s.value()));
            case Expr.NumberLiteral n ->
                Rendered.of(IdentifierPolicy.literal(n.value().toPlainString()));
            case Expr.NullLiteral n -> Rendered.of("NULL");
            case Expr.NewGuid g -> Rendered.of(RANDOM_GUID);
            case Expr.CurrentDateTime c -> Rendered.of(currentDateTime(c));
            case Expr.DateTimeLiteral d -> Rendered.of(IdentifierPolicy.literal(SqliteValues.dateTime(d.value(), 0)));
            default -> Rendered.mismatch("a text column takes no " + describe(expr) + " value");
        };
    }

    private static Rendered dateLiteral(Expr expr, int fractionDigits) {
        return switch (expr) {
            case Expr.DateTimeLiteral d ->
                Rendered.of(IdentifierPolicy.literal(SqliteValues.dateTime(d.value(), fractionDigits)));
            case Expr.CurrentDateTime c -> Rendered.of(currentDateTime(c));
            case Expr.NullLiteral n -> Rendered.of("NULL");
            default -> Rendered.mismatch("a date column takes no " + describe(expr) + " value");
        };
    }

    private static Rendered guidLiteral(Expr expr) {
        return switch (expr) {
            case Expr.NewGuid g -> Rendered.of(RANDOM_GUID);
            case Expr.StringLiteral s -> Rendered.of(IdentifierPolicy.literal(s.value()));
            case Expr.NullLiteral n -> Rendered.of("NULL");
            default -> Rendered.mismatch("a GUID column takes no " + describe(expr) + " value");
        };
    }

    private static String currentDateTime(Expr.CurrentDateTime c) {
        return switch (c.part()) {
            case NOW -> NOW;
            case DATE -> TODAY;
            case TIME -> TIME_OF_DAY;
        };
    }

    /** CHECK expressions: text is compared with {@code COLLATE NOCASE}, a date literal in its column's format. */
    private static final class Renderer extends CheckRenderer {
        private final Columns columns;

        Renderer(Columns columns) {
            this.columns = columns;
        }

        private String column(Expr.ColumnRef ref) {
            return IdentifierPolicy.quote(columns.name(ref.name())
                    .orElseThrow(() -> unrenderable("it refers to " + ref.name() + ", which is not written")));
        }

        @Override
        protected String operand(Expr.ColumnRef ref) {
            // Access compares text case-insensitively and without trailing spaces; BINARY would reject rows it accepted
            return columns.kind(ref.name()) == Kind.TEXT ? "rtrim(" + column(ref) + ") COLLATE NOCASE" : column(ref);
        }

        @Override
        protected String value(Expr expr, Expr.ColumnRef beside) {
            Kind kind = beside == null ? Kind.NUMERIC : columns.kind(beside.name());
            int fractionDigits = beside == null ? 0 : columns.fractionDigits(beside.name());
            if (kind == Kind.TEXT && expr instanceof Expr.StringLiteral s) {
                // The column side is rtrim()med too
                return IdentifierPolicy.literal(RuleEvaluator.withoutTrailingSpaces(s.value()));
            }
            Rendered result = defaultClause(expr, kind, fractionDigits);
            if (!result.isPresent()) {
                throw unrenderable(result.problem());
            }
            return result.sql();
        }

        @Override
        protected String like(Expr.Like like, String operand) {
            LikePattern pattern = like.pattern();
            if (!pattern.mapsToSqlLike()) {
                throw unrenderable("SQLite's LIKE has no equivalent of the # wildcard in " + quote(pattern.access()));
            }
            if (like.operand() instanceof Expr.ColumnRef ref) {
                operand = column(ref); // the value as it is, as Access matches it: no rtrim() here
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
            return operand + (like.negated() ? " NOT" : "") + " LIKE " + IdentifierPolicy.literal(sql.toString())
                    + (escaped ? " ESCAPE '\\'" : "");
        }
    }

    private static Optional<BigDecimal> number(String text) {
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

    private static String quote(String text) {
        return "\"" + text + "\"";
    }
}
