package com.lytrax.accessconverter.model.expr;

import java.util.Set;
import java.util.TreeSet;

/** The columns an expression refers to. Used by the profiler (which rows to read) and by the targets. */
public final class ExprColumns {
    private ExprColumns() {}

    /** Column names, compared and ordered like Access object names: case-insensitively. */
    public static Set<String> referenced(Expr expr) {
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        collect(expr, names);
        return names;
    }

    private static void collect(Expr expr, Set<String> names) {
        switch (expr) {
            case Expr.ColumnRef c -> names.add(c.name());
            case Expr.And a -> {
                collect(a.left(), names);
                collect(a.right(), names);
            }
            case Expr.Or o -> {
                collect(o.left(), names);
                collect(o.right(), names);
            }
            case Expr.Not n -> collect(n.operand(), names);
            case Expr.Comparison c -> {
                collect(c.left(), names);
                collect(c.right(), names);
            }
            case Expr.Between b -> {
                collect(b.operand(), names);
                collect(b.low(), names);
                collect(b.high(), names);
            }
            case Expr.In in -> {
                collect(in.operand(), names);
                in.values().forEach(v -> collect(v, names));
            }
            case Expr.IsNull n -> collect(n.operand(), names);
            case Expr.Like l -> collect(l.operand(), names);
            default -> {}
        }
    }
}
