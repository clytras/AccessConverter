package com.lytrax.accessconverter.profile;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * What the data allows (04, Profiling pass): the statistics that let a target be as strict as the data is, and no
 * stricter. Only the statistics a target decision depends on are collected; a table none of them applies to is
 * not read and has no entry.
 */
public record DataProfile(Map<String, TableProfile> tables, Map<String, RelationshipProfile> relationships) {

    public DataProfile {
        Map<String, TableProfile> t = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        t.putAll(tables);
        tables = Collections.unmodifiableMap(t);
        Map<String, RelationshipProfile> r = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        r.putAll(relationships);
        relationships = Collections.unmodifiableMap(r);
    }

    public Optional<TableProfile> table(String name) {
        return Optional.ofNullable(tables.get(name));
    }

    public Optional<RelationshipProfile> relationship(String name) {
        return Optional.ofNullable(relationships.get(name));
    }

    /** @param tableRule the table-level validation rule's result, or null when there is none to evaluate */
    public record TableProfile(String table, long rowsScanned, List<ColumnStats> columns, RuleStats tableRule) {
        public TableProfile {
            columns = List.copyOf(columns);
        }

        public Optional<ColumnStats> column(String name) {
            return columns.stream()
                    .filter(c -> c.column().equalsIgnoreCase(name))
                    .findFirst();
        }
    }

    /**
     * Per-column statistics. A field is null when it wasn't collected for this column.
     *
     * @param nulls NULL count, for Required columns and columns of required indexes: NOT NULL only when 0
     * @param emptyStrings {@code ""} count where AllowZeroLength is off: {@code CHECK (col <> '')} only when 0
     * @param maxSignificantDigits MONEY/NUMERIC: digits needed to hold every value exactly
     * @param maxScale MONEY/NUMERIC: the most fractional digits any value uses
     * @param maxFractionDigits date/time: fractional-second digits in use (0 = whole seconds, up to 7)
     * @param maxAutoNumber AUTONUMBER_LONG: the highest value, for the target's sequence seed
     * @param rule the column validation rule's result
     */
    public record ColumnStats(
            String column,
            Long nulls,
            Long emptyStrings,
            Integer maxSignificantDigits,
            Integer maxScale,
            Integer maxFractionDigits,
            Long maxAutoNumber,
            RuleStats rule) {}

    /**
     * @param violations rows for which the rule is FALSE
     * @param unevaluable rows the evaluator couldn't decide (incompatible types): the rule is then unverified
     * @param samples keys of violating rows
     */
    public record RuleStats(long violations, long unevaluable, List<String> samples, String firstError) {
        public RuleStats {
            samples = List.copyOf(samples);
        }

        public boolean holds() {
            return violations == 0 && unevaluable == 0;
        }
    }

    /**
     * Referential integrity of an enforced relationship, checked through the parent's Access index.
     *
     * @param checked child rows whose key columns are all non-NULL
     * @param orphans checked rows with no parent: a foreign key would reject them
     * @param inexact rows whose parent matches only under Access's case-insensitive comparison
     * @param inexactAsciiCaseOnly every inexact match differs only in ASCII letter case
     */
    public record RelationshipProfile(
            String relationship,
            long checked,
            long orphans,
            List<String> orphanSamples,
            long inexact,
            boolean inexactAsciiCaseOnly,
            List<String> inexactSamples) {
        public RelationshipProfile {
            orphanSamples = List.copyOf(orphanSamples);
            inexactSamples = List.copyOf(inexactSamples);
        }
    }
}
