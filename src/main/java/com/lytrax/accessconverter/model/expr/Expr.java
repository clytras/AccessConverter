package com.lytrax.accessconverter.model.expr;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

/**
 * The closed subset of Access expressions that defaults and validation rules translate to (04, Expressions).
 * Targets render it; the profiler evaluates it against existing rows.
 */
public sealed interface Expr {

    record NullLiteral() implements Expr {}

    /** {@code Yes}/{@code No}, {@code True}/{@code False}, {@code On}/{@code Off}. */
    record BooleanLiteral(boolean value) implements Expr {}

    record NumberLiteral(BigDecimal value) implements Expr {
        public NumberLiteral {
            Objects.requireNonNull(value, "value");
        }
    }

    record StringLiteral(String value) implements Expr {
        public StringLiteral {
            Objects.requireNonNull(value, "value");
        }
    }

    /**
     * A {@code #…#} literal. A time-only literal is on Access's day zero, 1899-12-30; a date-only literal is at
     * midnight.
     */
    record DateTimeLiteral(LocalDateTime value, boolean hasDate, boolean hasTime) implements Expr {
        public DateTimeLiteral {
            Objects.requireNonNull(value, "value");
        }
    }

    /** {@code Now()}, {@code Date()} or {@code Time()}: local time, as Access evaluates them. */
    record CurrentDateTime(Part part) implements Expr {
        public enum Part {
            NOW,
            DATE,
            TIME
        }
    }

    /** {@code GenGUID()}. */
    record NewGuid() implements Expr {}

    /**
     * {@code GenUniqueID()}: how Access stores New Values = Random on a Long autonumber (measured in Access 97's table
     * designer; Jet then generates random values, negative ones included). It is part of the autonumber, not a
     * default a target writes.
     */
    record NewRandomId() implements Expr {}

    record ColumnRef(String name) implements Expr {
        public ColumnRef {
            Objects.requireNonNull(name, "name");
        }
    }

    record Comparison(Operator operator, Expr left, Expr right) implements Expr {}

    enum Operator {
        EQ("="),
        NE("<>"),
        LT("<"),
        LE("<="),
        GT(">"),
        GE(">=");

        private final String symbol;

        Operator(String symbol) {
            this.symbol = symbol;
        }

        public String symbol() {
            return symbol;
        }
    }

    record Between(Expr operand, Expr low, Expr high, boolean negated) implements Expr {}

    record In(Expr operand, List<Expr> values, boolean negated) implements Expr {
        public In {
            values = List.copyOf(values);
        }
    }

    record IsNull(Expr operand, boolean negated) implements Expr {}

    record Like(Expr operand, LikePattern pattern, boolean negated) implements Expr {}

    record Not(Expr operand) implements Expr {}

    record And(Expr left, Expr right) implements Expr {}

    record Or(Expr left, Expr right) implements Expr {}
}
