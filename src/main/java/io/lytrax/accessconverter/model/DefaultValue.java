package io.lytrax.accessconverter.model;

import io.lytrax.accessconverter.model.expr.Expr;
import java.util.Objects;

/**
 * A column's Access {@code DefaultValue}: the raw text, plus either its parsed expression or why it's outside the
 * supported subset (04, Expressions). Nothing is ever guessed.
 */
public record DefaultValue(String raw, Expr expr, String unsupportedReason) {

    public DefaultValue {
        Objects.requireNonNull(raw, "raw");
        if ((expr == null) == (unsupportedReason == null)) {
            throw new IllegalArgumentException("exactly one of expr and unsupportedReason is required");
        }
    }

    public boolean isTranslated() {
        return expr != null;
    }
}
