package com.lytrax.accessconverter.model;

import java.util.Objects;

/**
 * One Access column with the properties targets need (04, Columns). Nullable fields are null when Access has no
 * value for them.
 *
 * @param ordinal Access column order, from 0
 * @param length characters for TEXT, bytes for BINARY, otherwise null
 * @param precision NUMERIC and MONEY only
 * @param scale NUMERIC and MONEY only
 * @param required the {@code Required} property. Tentative: the profiler decides whether the data allows NOT NULL.
 * @param allowZeroLength TEXT and MEMO: {@code false} means Access rejects {@code ""}. True when the property is
 *     absent, so a target is never stricter than Access.
 * @param calculatedExpression the Access expression of a calculated column; its values are exported as stored
 * @param appendOnly a memo with version history
 * @param hidden a system-maintained column, such as replication's {@code s_GUID}
 */
public record ColumnModel(
        String name,
        int ordinal,
        AccessType type,
        Integer length,
        Integer precision,
        Integer scale,
        boolean required,
        boolean allowZeroLength,
        DefaultValue defaultValue,
        CheckRule validation,
        String description,
        String format,
        Integer decimalPlaces,
        boolean richText,
        String calculatedExpression,
        boolean appendOnly,
        boolean hidden) {

    public ColumnModel {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(type, "type");
    }

    public boolean isCalculated() {
        return calculatedExpression != null;
    }
}
