package com.lytrax.accessconverter.model;

import com.lytrax.accessconverter.model.expr.Expr;
import java.util.Objects;

/**
 * An Access validation rule (column or table level): the raw text and message, plus either its parsed expression
 * or why it's outside the supported subset. A column rule's implicit operand (as in {@code >0}) is already
 * resolved to a reference to that column.
 */
public record CheckRule(String raw, String validationText, Expr expr, String unsupportedReason) {

    public CheckRule {
        Objects.requireNonNull(raw, "raw");
        if ((expr == null) == (unsupportedReason == null)) {
            throw new IllegalArgumentException("exactly one of expr and unsupportedReason is required");
        }
    }

    public boolean isTranslated() {
        return expr != null;
    }
}
