package com.lytrax.accessconverter.profile;

import com.lytrax.accessconverter.model.expr.Expr;
import com.lytrax.accessconverter.model.expr.Expr.And;
import com.lytrax.accessconverter.model.expr.Expr.Between;
import com.lytrax.accessconverter.model.expr.Expr.BooleanLiteral;
import com.lytrax.accessconverter.model.expr.Expr.ColumnRef;
import com.lytrax.accessconverter.model.expr.Expr.Comparison;
import com.lytrax.accessconverter.model.expr.Expr.CurrentDateTime;
import com.lytrax.accessconverter.model.expr.Expr.DateTimeLiteral;
import com.lytrax.accessconverter.model.expr.Expr.In;
import com.lytrax.accessconverter.model.expr.Expr.IsNull;
import com.lytrax.accessconverter.model.expr.Expr.Like;
import com.lytrax.accessconverter.model.expr.Expr.NewGuid;
import com.lytrax.accessconverter.model.expr.Expr.NewRandomId;
import com.lytrax.accessconverter.model.expr.Expr.Not;
import com.lytrax.accessconverter.model.expr.Expr.NullLiteral;
import com.lytrax.accessconverter.model.expr.Expr.NumberLiteral;
import com.lytrax.accessconverter.model.expr.Expr.Or;
import com.lytrax.accessconverter.model.expr.Expr.StringLiteral;
import java.math.BigDecimal;
import java.text.Collator;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.IntPredicate;

/**
 * Evaluates a translated validation rule against one row of canonical values, with SQL's three-valued logic: a
 * rule rejects a row only when it is FALSE, so NULLs pass, as in both Access and a SQL CHECK.
 *
 * <p>Text compares as Access does by default ({@link TextComparison#ACCESS}): case-insensitive, accent-sensitive,
 * trailing spaces ignored. Measured in Access 97 and ACE 16 (phase 6): a rule ignores trailing U+0020 only, never a
 * trailing tab, line break or no-break space, and folds case in every script ({@code "α"} passes {@code In ("Α")},
 * {@code "οδος"} equals {@code "ΟΔΟΣ"}, Cyrillic too), but keeps the dotless {@code ı} apart from {@code I}. Java's
 * ROOT collator folds only Latin case, so text is {@linkplain #accessComparable folded} before it is collated.
 * {@link TextComparison#ASCII_NOCASE} is what a SQLite CHECK does with the same rule: trailing spaces trimmed
 * ({@code rtrim}), ASCII letters folded ({@code COLLATE NOCASE}), everything else compared by code point. Comparing
 * values of incompatible types throws {@link EvaluationException}: the profiler then treats the rule as unverifiable
 * rather than guessing.
 */
public final class RuleEvaluator {
    public enum Truth {
        TRUE,
        FALSE,
        UNKNOWN;

        static Truth of(boolean value) {
            return value ? TRUE : FALSE;
        }

        Truth not() {
            return this == UNKNOWN ? UNKNOWN : of(this == FALSE);
        }

        Truth and(Truth other) {
            if (this == FALSE || other == FALSE) {
                return FALSE;
            }
            return this == TRUE && other == TRUE ? TRUE : UNKNOWN;
        }

        Truth or(Truth other) {
            if (this == TRUE || other == TRUE) {
                return TRUE;
            }
            return this == FALSE && other == FALSE ? FALSE : UNKNOWN;
        }
    }

    public static final class EvaluationException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        EvaluationException(String message) {
            super(message);
        }
    }

    /** How text is compared. */
    public enum TextComparison {
        /** As Access compares it. */
        ACCESS,
        /** As a SQLite CHECK compares it: {@code rtrim(col) COLLATE NOCASE}, and {@code LIKE} folding ASCII only. */
        ASCII_NOCASE
    }

    private final TextComparison comparison;
    private final Collator text;

    public RuleEvaluator() {
        this(TextComparison.ACCESS);
    }

    public RuleEvaluator(TextComparison comparison) {
        this.comparison = comparison;
        text = Collator.getInstance(Locale.ROOT);
        text.setStrength(Collator.SECONDARY);
    }

    /**
     * Text as Access compares it, before collation: without trailing spaces and with each letter's case folded as
     * measured in ACE 16 (lower case per code point, final sigma as sigma).
     */
    public static String accessComparable(String text) {
        StringBuilder folded = new StringBuilder(text.length());
        withoutTrailingSpaces(text)
                .codePoints()
                .forEach(c -> folded.appendCodePoint(c == 'ς' ? 'σ' : Character.toLowerCase(c)));
        return folded.toString();
    }

    /** The text without its trailing spaces (U+0020 only), which Access ignores when it compares text. */
    public static String withoutTrailingSpaces(String text) {
        int end = text.length();
        while (end > 0 && text.charAt(end - 1) == ' ') {
            end--;
        }
        return text.substring(0, end);
    }

    /** @param row canonical value of a column by name; throws for an unknown column */
    public Truth test(Expr rule, Function<String, Object> row) {
        return switch (rule) {
            case And and -> test(and.left(), row).and(test(and.right(), row));
            case Or or -> test(or.left(), row).or(test(or.right(), row));
            case Not not -> test(not.operand(), row).not();
            case Comparison c -> compare(c, row);
            case Between b -> {
                Object value = value(b.operand(), row);
                Truth low = order(value, value(b.low(), row), r -> r >= 0);
                Truth high = order(value, value(b.high(), row), r -> r <= 0);
                Truth between = low.and(high);
                yield b.negated() ? between.not() : between;
            }
            case In in -> {
                Object value = value(in.operand(), row);
                Truth found = Truth.FALSE;
                for (Expr candidate : in.values()) {
                    found = found.or(order(value, value(candidate, row), r -> r == 0));
                }
                yield in.negated() ? found.not() : found;
            }
            case IsNull n -> Truth.of((value(n.operand(), row) == null) != n.negated());
            case Like l -> {
                Object value = value(l.operand(), row);
                if (value == null) {
                    yield Truth.UNKNOWN;
                }
                if (!(value instanceof String s)) {
                    throw new EvaluationException("Like on a non-text value");
                }
                Truth match = Truth.of(l.pattern()
                        .toRegex(comparison == TextComparison.ACCESS)
                        .matcher(s)
                        .matches());
                yield l.negated() ? match.not() : match;
            }
            default -> throw new EvaluationException("not a condition: " + rule);
        };
    }

    private Truth compare(Comparison c, Function<String, Object> row) {
        Object left = value(c.left(), row);
        Object right = value(c.right(), row);
        return switch (c.operator()) {
            case EQ -> order(left, right, r -> r == 0);
            case NE -> order(left, right, r -> r != 0);
            case LT -> order(left, right, r -> r < 0);
            case LE -> order(left, right, r -> r <= 0);
            case GT -> order(left, right, r -> r > 0);
            case GE -> order(left, right, r -> r >= 0);
        };
    }

    private Truth order(Object left, Object right, IntPredicate test) {
        if (left == null || right == null) {
            return Truth.UNKNOWN;
        }
        return Truth.of(test.test(compareValues(left, right)));
    }

    private int compareValues(Object left, Object right) {
        if (isNumeric(left) && isNumeric(right)) {
            return number(left).compareTo(number(right));
        }
        if (left instanceof String a && right instanceof String b) {
            return comparison == TextComparison.ACCESS
                    ? text.compare(accessComparable(a), accessComparable(b))
                    : asciiNocase(withoutTrailingSpaces(a), withoutTrailingSpaces(b));
        }
        if (left instanceof LocalDateTime a && right instanceof LocalDateTime b) {
            return a.compareTo(b);
        }
        throw new EvaluationException("cannot compare " + kind(left) + " with " + kind(right));
    }

    /** SQLite's NOCASE: ASCII letters folded, then byte order, which for UTF-8 is code-point order. */
    private static int asciiNocase(String a, String b) {
        int[] x = a.codePoints().map(RuleEvaluator::asciiLower).toArray();
        int[] y = b.codePoints().map(RuleEvaluator::asciiLower).toArray();
        return Arrays.compare(x, y);
    }

    private static int asciiLower(int c) {
        return c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c;
    }

    private static Object value(Expr operand, Function<String, Object> row) {
        return switch (operand) {
            case NullLiteral n -> null;
            case BooleanLiteral b -> b.value();
            case NumberLiteral n -> n.value();
            case StringLiteral s -> s.value();
            case DateTimeLiteral d -> d.value();
            case ColumnRef c -> row.apply(c.name());
            case CurrentDateTime c -> throw new EvaluationException("non-deterministic " + c.part());
            case NewGuid g -> throw new EvaluationException("non-deterministic GenGUID()");
            case NewRandomId r -> throw new EvaluationException("non-deterministic GenUniqueID()");
            default -> throw new EvaluationException("a condition used as a value");
        };
    }

    private static boolean isNumeric(Object value) {
        return value instanceof Number || value instanceof Boolean;
    }

    /** Access Yes/No is -1/0 in comparisons with numbers. */
    private static BigDecimal number(Object value) {
        return switch (value) {
            case Boolean b -> b ? BigDecimal.ONE.negate() : BigDecimal.ZERO;
            case BigDecimal d -> d;
            case Float f -> finite(f.doubleValue(), f.toString());
            case Double d -> finite(d, d.toString());
            case Number n -> BigDecimal.valueOf(n.longValue());
            default -> throw new EvaluationException("not a number: " + kind(value));
        };
    }

    private static BigDecimal finite(double value, String text) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new EvaluationException("cannot compare " + text);
        }
        return new BigDecimal(text);
    }

    private static String kind(Object value) {
        return switch (value) {
            case String s -> "text";
            case LocalDateTime t -> "a date";
            case Boolean b -> "Yes/No";
            case Number n -> "a number";
            default -> value.getClass().getSimpleName();
        };
    }
}
