package com.lytrax.accessconverter.target;

/**
 * An Access expression rendered as target SQL, or why it can't be. Exactly one of {@code sql} and {@code problem} is
 * set.
 *
 * @param typeMismatch the expression is valid, but doesn't fit the column it is for (a text default on a number
 *     column): reported as {@code DEFAULT_DROPPED_TYPE_MISMATCH} rather than {@code DEFAULT_UNTRANSLATABLE}
 */
public record Rendered(String sql, String problem, boolean typeMismatch) {

    public static Rendered of(String sql) {
        return new Rendered(sql, null, false);
    }

    public static Rendered unsupported(String problem) {
        return new Rendered(null, problem, false);
    }

    public static Rendered mismatch(String problem) {
        return new Rendered(null, problem, true);
    }

    public boolean isPresent() {
        return sql != null;
    }
}
