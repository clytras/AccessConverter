package com.lytrax.accessconverter.target.sqlite;

import com.lytrax.accessconverter.model.expr.Expr;
import com.lytrax.accessconverter.model.expr.LikePattern;
import com.lytrax.accessconverter.target.IdentifierPolicy;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Renders the Access expression subset ({@link Expr}) as SQLite SQL, for {@code DEFAULT} clauses and {@code CHECK}
 * constraints (06, Constraints and attributes). Anything the subset allows but SQLite can't express, and any
 * default that doesn't fit its column, comes back as a problem to report instead of a guess.
 *
 * <p>Text is compared with {@code COLLATE NOCASE}, because Access compares text case-insensitively: a rule Access
 * accepted must never reject the same row here.
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

    /** The rendered SQL, or why there is none. Exactly one of the two is set. */
    public record Result(String sql, String problem, boolean typeMismatch) {

        static Result of(String sql) {
            return new Result(sql, null, false);
        }

        static Result unsupported(String problem) {
            return new Result(null, problem, false);
        }

        static Result mismatch(String problem) {
            return new Result(null, problem, true);
        }

        public boolean isPresent() {
            return sql != null;
        }
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
    public static Result defaultClause(Expr expr, Kind kind, int fractionDigits) {
        return switch (kind) {
            case NUMERIC -> numericLiteral(expr);
            case TEXT -> textLiteral(expr);
            case DATE -> dateLiteral(expr, fractionDigits);
            case GUID -> guidLiteral(expr);
            case BLOB ->
                expr instanceof Expr.NullLiteral
                        ? Result.of("NULL")
                        : Result.mismatch("a BLOB column takes no default value");
        };
    }

    /** A {@code CHECK} expression, or why SQLite can't express the rule. */
    public static Result check(Expr expr, Columns columns) {
        try {
            return Result.of(render(expr, 0, columns));
        } catch (Unrenderable e) {
            return Result.unsupported(e.getMessage());
        }
    }

    private static Result numericLiteral(Expr expr) {
        return switch (expr) {
            case Expr.NumberLiteral n -> Result.of(n.value().toPlainString());
            case Expr.BooleanLiteral b -> Result.of(b.value() ? "1" : "0");
            case Expr.NullLiteral n -> Result.of("NULL");
            case Expr.StringLiteral s ->
                number(s.value())
                        .map(v -> Result.of(v.toPlainString()))
                        .orElseGet(() -> Result.mismatch("the value " + quote(s.value()) + " is not a number"));
            default -> Result.mismatch("a number column takes no " + describe(expr) + " value");
        };
    }

    private static Result textLiteral(Expr expr) {
        return switch (expr) {
            case Expr.StringLiteral s -> Result.of(IdentifierPolicy.literal(s.value()));
            case Expr.NumberLiteral n ->
                Result.of(IdentifierPolicy.literal(n.value().toPlainString()));
            case Expr.NullLiteral n -> Result.of("NULL");
            case Expr.NewGuid g -> Result.of(RANDOM_GUID);
            case Expr.CurrentDateTime c -> Result.of(currentDateTime(c));
            case Expr.DateTimeLiteral d -> Result.of(IdentifierPolicy.literal(SqliteValues.dateTime(d.value(), 0)));
            default -> Result.mismatch("a text column takes no " + describe(expr) + " value");
        };
    }

    private static Result dateLiteral(Expr expr, int fractionDigits) {
        return switch (expr) {
            case Expr.DateTimeLiteral d ->
                Result.of(IdentifierPolicy.literal(SqliteValues.dateTime(d.value(), fractionDigits)));
            case Expr.CurrentDateTime c -> Result.of(currentDateTime(c));
            case Expr.NullLiteral n -> Result.of("NULL");
            default -> Result.mismatch("a date column takes no " + describe(expr) + " value");
        };
    }

    private static Result guidLiteral(Expr expr) {
        return switch (expr) {
            case Expr.NewGuid g -> Result.of(RANDOM_GUID);
            case Expr.StringLiteral s -> Result.of(IdentifierPolicy.literal(s.value()));
            case Expr.NullLiteral n -> Result.of("NULL");
            default -> Result.mismatch("a GUID column takes no " + describe(expr) + " value");
        };
    }

    private static String currentDateTime(Expr.CurrentDateTime c) {
        return switch (c.part()) {
            case NOW -> NOW;
            case DATE -> TODAY;
            case TIME -> TIME_OF_DAY;
        };
    }

    /** Precedence: Or 1, And 2, Not 3, predicates 4. Mirrors {@code ExprPrinter}, so both read the same. */
    private static String render(Expr expr, int context, Columns columns) {
        return switch (expr) {
            case Expr.Or or ->
                wrap(1, context, render(or.left(), 1, columns) + " OR " + render(or.right(), 1, columns));
            case Expr.And and ->
                wrap(2, context, render(and.left(), 2, columns) + " AND " + render(and.right(), 2, columns));
            case Expr.Not not -> wrap(3, context, "NOT " + render(not.operand(), 3, columns));
            case Expr.Comparison c ->
                operand(c.left(), columns) + " " + c.operator().symbol() + " " + beside(c.left(), c.right(), columns);
            case Expr.Between b ->
                operand(b.operand(), columns) + (b.negated() ? " NOT" : "") + " BETWEEN "
                        + beside(b.operand(), b.low(), columns) + " AND " + beside(b.operand(), b.high(), columns);
            case Expr.In in ->
                operand(in.operand(), columns) + (in.negated() ? " NOT" : "") + " IN ("
                        + in.values().stream()
                                .map(v -> beside(in.operand(), v, columns))
                                .collect(Collectors.joining(", "))
                        + ")";
            case Expr.IsNull n -> operand(n.operand(), columns) + (n.negated() ? " IS NOT NULL" : " IS NULL");
            case Expr.Like l -> like(l, columns);
            default -> value(expr, Kind.NUMERIC, 0, columns);
        };
    }

    /** The left-hand side of a predicate: a column reference, compared the way Access compares it. */
    private static String operand(Expr expr, Columns columns) {
        if (expr instanceof Expr.ColumnRef ref) {
            String name = columns.name(ref.name())
                    .orElseThrow(() -> new Unrenderable("it refers to " + ref.name() + ", which is not written"));
            // Access compares text case-insensitively; BINARY would reject rows Access accepted
            return IdentifierPolicy.quote(name) + (columns.kind(ref.name()) == Kind.TEXT ? " COLLATE NOCASE" : "");
        }
        return value(expr, Kind.NUMERIC, 0, columns);
    }

    /** A value rendered for the column it stands beside, so a date literal matches that column's format. */
    private static String beside(Expr subject, Expr expr, Columns columns) {
        if (subject instanceof Expr.ColumnRef ref) {
            return value(expr, columns.kind(ref.name()), columns.fractionDigits(ref.name()), columns);
        }
        return value(expr, Kind.NUMERIC, 0, columns);
    }

    private static String value(Expr expr, Kind kind, int fractionDigits, Columns columns) {
        if (expr instanceof Expr.ColumnRef) {
            return operand(expr, columns);
        }
        Result result = defaultClause(expr, kind, fractionDigits);
        if (!result.isPresent()) {
            throw new Unrenderable(result.problem());
        }
        return result.sql();
    }

    private static String like(Expr.Like like, Columns columns) {
        LikePattern pattern = like.pattern();
        if (!pattern.mapsToSqlLike()) {
            throw new Unrenderable("SQLite's LIKE has no equivalent of the # wildcard in " + quote(pattern.access()));
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
                case LikePattern.AnyString s -> sql.append('%');
                case LikePattern.AnyChar c -> sql.append('_');
                case LikePattern.AnyDigit d -> throw new IllegalStateException("excluded by mapsToSqlLike");
            }
        }
        return operand(like.operand(), columns) + (like.negated() ? " NOT" : "") + " LIKE "
                + IdentifierPolicy.literal(sql.toString()) + (escaped ? " ESCAPE '\\'" : "");
    }

    private static String wrap(int precedence, int context, String text) {
        return precedence < context ? "(" + text + ")" : text;
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

    /** Thrown while rendering a CHECK that SQLite can't express; turned into a {@link Result} problem. */
    private static final class Unrenderable extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Unrenderable(String message) {
            super(message, null, false, false);
        }
    }
}
