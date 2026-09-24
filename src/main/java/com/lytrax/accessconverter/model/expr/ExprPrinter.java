package com.lytrax.accessconverter.model.expr;

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
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Renders an expression in normalized Access syntax: bracketed columns, double-quoted strings, ISO dates and
 * explicit operands. Used by {@code inspect}, the report and golden tests.
 */
public final class ExprPrinter {
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("uuuu-MM-dd", Locale.ROOT);
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);

    private ExprPrinter() {}

    public static String print(Expr expr) {
        return print(expr, 0);
    }

    /** Precedence: Or 1, And 2, Not 3, predicates and operands 4. */
    private static String print(Expr expr, int context) {
        return switch (expr) {
            case Or or -> wrap(1, context, print(or.left(), 1) + " Or " + print(or.right(), 1));
            case And and -> wrap(2, context, print(and.left(), 2) + " And " + print(and.right(), 2));
            case Not not -> wrap(3, context, "Not " + print(not.operand(), 3));
            case Comparison c -> print(c.left(), 4) + " " + c.operator().symbol() + " " + print(c.right(), 4);
            case Between b ->
                print(b.operand(), 4) + (b.negated() ? " Not" : "") + " Between " + print(b.low(), 4) + " And "
                        + print(b.high(), 4);
            case In in ->
                print(in.operand(), 4) + (in.negated() ? " Not" : "") + " In ("
                        + in.values().stream().map(v -> print(v, 0)).collect(Collectors.joining(", ")) + ")";
            case IsNull n -> print(n.operand(), 4) + (n.negated() ? " Is Not Null" : " Is Null");
            case Like l ->
                print(l.operand(), 4) + (l.negated() ? " Not" : "") + " Like "
                        + quote(l.pattern().access());
            case NullLiteral n -> "Null";
            case BooleanLiteral b -> b.value() ? "True" : "False";
            case NumberLiteral n -> n.value().toPlainString();
            case StringLiteral s -> quote(s.value());
            case DateTimeLiteral d -> "#" + dateTime(d) + "#";
            case CurrentDateTime c ->
                switch (c.part()) {
                    case NOW -> "Now()";
                    case DATE -> "Date()";
                    case TIME -> "Time()";
                };
            case NewGuid g -> "GenGUID()";
            case NewRandomId r -> "GenUniqueID()";
            case ColumnRef c -> "[" + c.name() + "]";
        };
    }

    private static String wrap(int precedence, int context, String text) {
        return precedence < context ? "(" + text + ")" : text;
    }

    private static String quote(String s) {
        return "\"" + s.replace("\"", "\"\"") + "\"";
    }

    private static String dateTime(DateTimeLiteral d) {
        if (!d.hasTime()) {
            return DATE.format(d.value());
        }
        if (!d.hasDate()) {
            return TIME.format(d.value());
        }
        return DATE.format(d.value()) + " " + TIME.format(d.value());
    }
}
