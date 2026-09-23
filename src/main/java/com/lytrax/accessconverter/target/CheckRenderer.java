package com.lytrax.accessconverter.target;

import com.lytrax.accessconverter.model.expr.Expr;
import java.util.stream.Collectors;

/**
 * Renders an Access validation rule ({@link Expr}) as a target's CHECK expression. The shape of the SQL is the same
 * everywhere; how a column is referred to, how a literal is written beside it and how {@code Like} works are the
 * target's.
 *
 * <p>Precedence: Or 1, And 2, Not 3, predicates 4. It mirrors {@code ExprPrinter}, so both read the same.
 */
public abstract class CheckRenderer {

    /** A CHECK expression, or why this target can't express the rule. */
    public final Rendered check(Expr expr) {
        try {
            return Rendered.of(render(expr, 0));
        } catch (Unrenderable e) {
            return Rendered.unsupported(e.getMessage());
        }
    }

    /** A column as the subject of a predicate, compared the way Access compares it. */
    protected abstract String operand(Expr.ColumnRef ref);

    /**
     * A value written beside a column, in that column's terms (a date literal in its format, text in its collation).
     *
     * @param beside the column it is compared with, or null when there is none
     */
    protected abstract String value(Expr expr, Expr.ColumnRef beside);

    protected abstract String like(Expr.Like like, String operand);

    /** Stops rendering: the rule has no CHECK in this target, for this reason. */
    protected static RuntimeException unrenderable(String problem) {
        return new Unrenderable(problem);
    }

    private String render(Expr expr, int context) {
        return switch (expr) {
            case Expr.Or or -> wrap(1, context, render(or.left(), 1) + " OR " + render(or.right(), 1));
            case Expr.And and -> wrap(2, context, render(and.left(), 2) + " AND " + render(and.right(), 2));
            case Expr.Not not -> wrap(3, context, "NOT " + render(not.operand(), 3));
            case Expr.Comparison c ->
                subject(c.left()) + " " + c.operator().symbol() + " " + beside(c.left(), c.right());
            case Expr.Between b ->
                subject(b.operand()) + (b.negated() ? " NOT" : "") + " BETWEEN " + beside(b.operand(), b.low())
                        + " AND " + beside(b.operand(), b.high());
            case Expr.In in ->
                subject(in.operand()) + (in.negated() ? " NOT" : "") + " IN ("
                        + in.values().stream().map(v -> beside(in.operand(), v)).collect(Collectors.joining(", "))
                        + ")";
            case Expr.IsNull n -> subject(n.operand()) + (n.negated() ? " IS NOT NULL" : " IS NULL");
            case Expr.Like l -> like(l, subject(l.operand()));
            default -> value(expr, null);
        };
    }

    private String subject(Expr expr) {
        return expr instanceof Expr.ColumnRef ref ? operand(ref) : value(expr, null);
    }

    private String beside(Expr subject, Expr expr) {
        if (expr instanceof Expr.ColumnRef ref) {
            return operand(ref);
        }
        return value(expr, subject instanceof Expr.ColumnRef ref ? ref : null);
    }

    private static String wrap(int precedence, int context, String text) {
        return precedence < context ? "(" + text + ")" : text;
    }

    /** Thrown while rendering a CHECK the target can't express; turned into a {@link Rendered} problem. */
    private static final class Unrenderable extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Unrenderable(String message) {
            super(message, null, false, false);
        }
    }
}
