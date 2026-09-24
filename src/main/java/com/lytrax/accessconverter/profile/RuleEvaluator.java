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
import java.util.Locale;
import java.util.function.Function;
import java.util.function.IntPredicate;

/**
 * Evaluates a translated validation rule against one row of canonical values, with SQL's three-valued logic: a
 * rule rejects a row only when it is FALSE, so NULLs pass, as in both Access and a SQL CHECK.
 *
 * <p>Text compares as Access does by default: case-insensitive, accent-sensitive, trailing spaces ignored (the
 * unique-index semantics verified in 01). Comparing values of incompatible types throws
 * {@link EvaluationException}: the profiler then treats the rule as unverifiable rather than guessing.
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

    private final Collator text;

    public RuleEvaluator() {
        text = Collator.getInstance(Locale.ROOT);
        text.setStrength(Collator.SECONDARY);
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
                Truth match = Truth.of(l.pattern().toRegex().matcher(s).matches());
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
            return text.compare(a.stripTrailing(), b.stripTrailing());
        }
        if (left instanceof LocalDateTime a && right instanceof LocalDateTime b) {
            return a.compareTo(b);
        }
        throw new EvaluationException("cannot compare " + kind(left) + " with " + kind(right));
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
