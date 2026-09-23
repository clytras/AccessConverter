package com.lytrax.accessconverter.profile;

import com.lytrax.accessconverter.profile.DataProfile.ColumnStats;
import com.lytrax.accessconverter.profile.DataProfile.RelationshipProfile;
import com.lytrax.accessconverter.profile.DataProfile.RuleStats;
import com.lytrax.accessconverter.profile.DataProfile.TableProfile;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Test helper: a {@link DataProfile} written by hand, for target rules the corpus doesn't exercise. */
public final class Profiles {

    private final Map<String, TableProfile> tables = new LinkedHashMap<>();
    private final Map<String, RelationshipProfile> relationships = new LinkedHashMap<>();

    private Profiles() {}

    public static Profiles profile() {
        return new Profiles();
    }

    public static Stats stats(String column) {
        return new Stats(column);
    }

    public Profiles table(String name, long rows, Stats... columns) {
        List<ColumnStats> stats = new ArrayList<>();
        for (Stats column : columns) {
            stats.add(column.build());
        }
        tables.put(name, new TableProfile(name, rows, stats, null));
        return this;
    }

    public Profiles tableRule(String name, long violations, long unevaluable) {
        TableProfile existing = tables.get(name);
        tables.put(
                name,
                new TableProfile(
                        name,
                        existing == null ? 0 : existing.rowsScanned(),
                        existing == null ? List.of() : existing.columns(),
                        new RuleStats(violations, unevaluable, List.of(), unevaluable > 0 ? "no such column" : null)));
        return this;
    }

    /** An enforced relationship the profiler checked: {@code orphans} rows had no parent. */
    public Profiles relationship(String name, long checked, long orphans) {
        return relationship(name, checked, orphans, 0, true);
    }

    public Profiles relationship(String name, long checked, long orphans, long inexact, boolean asciiCaseOnly) {
        relationships.put(
                name,
                new RelationshipProfile(
                        name,
                        checked,
                        orphans,
                        orphans > 0 ? List.of("(1) -> (2)") : List.of(),
                        inexact,
                        inexact > 0 && asciiCaseOnly,
                        inexact > 0 ? List.of("(1): (\"abc\") matches (\"ABC\")") : List.of(),
                        null));
        return this;
    }

    public DataProfile build() {
        return new DataProfile(tables, relationships);
    }

    /** One column's statistics; a field left out was not collected, as in a real profile. */
    public static final class Stats {
        private final String column;
        private Long nulls;
        private Long emptyStrings;
        private Integer maxSignificantDigits;
        private Integer maxScale;
        private Integer maxFractionDigits;
        private Long maxAutoNumber;
        private RuleStats rule;
        private Long unsafeKeyText;

        private Stats(String column) {
            this.column = column;
        }

        public Stats nulls(long count) {
            this.nulls = count;
            return this;
        }

        public Stats emptyStrings(long count) {
            this.emptyStrings = count;
            return this;
        }

        public Stats digits(int significant, int scale) {
            this.maxSignificantDigits = significant;
            this.maxScale = scale;
            return this;
        }

        public Stats fractionDigits(int digits) {
            this.maxFractionDigits = digits;
            return this;
        }

        public Stats maxAutoNumber(long value) {
            this.maxAutoNumber = value;
            return this;
        }

        public Stats unsafeKeyText(long count) {
            this.unsafeKeyText = count;
            return this;
        }

        public Stats rule(long violations, long unevaluable) {
            this.rule =
                    new RuleStats(violations, unevaluable, List.of(), unevaluable > 0 ? "incompatible types" : null);
            return this;
        }

        private ColumnStats build() {
            return new ColumnStats(
                    column,
                    nulls,
                    emptyStrings,
                    null,
                    maxSignificantDigits,
                    maxScale,
                    maxFractionDigits,
                    maxAutoNumber,
                    rule,
                    unsafeKeyText);
        }
    }
}
