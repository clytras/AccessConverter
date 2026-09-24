package com.lytrax.accessconverter.profile;

import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.model.CheckRule;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.model.expr.Expr;
import com.lytrax.accessconverter.model.expr.ExprColumns;
import com.lytrax.accessconverter.profile.DataProfile.ColumnStats;
import com.lytrax.accessconverter.profile.DataProfile.RelationshipProfile;
import com.lytrax.accessconverter.profile.DataProfile.RuleStats;
import com.lytrax.accessconverter.profile.DataProfile.TableProfile;
import com.lytrax.accessconverter.profile.RuleEvaluator.EvaluationException;
import com.lytrax.accessconverter.profile.RuleEvaluator.Truth;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.RowStream;
import com.lytrax.accessconverter.value.CanonicalText;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * The targeted profiling pass (04): one physical scan per table, reading only the columns whose statistics decide
 * a target feature, plus an index lookup per child row of each enforced relationship.
 */
public final class DataProfiler {
    /** U+FFFD, what a byte the code page doesn't define decodes to. */
    private static final char REPLACEMENT = (char) 0xFFFD;

    private DataProfiler() {}

    public static DataProfile profile(AccessSource source, SchemaModel model) throws IOException {
        Map<String, TableProfile> tables = new LinkedHashMap<>();
        Map<String, RelationshipProfile> relationships = new LinkedHashMap<>();
        RuleEvaluator evaluator = new RuleEvaluator();
        for (TableModel table : model.tables()) {
            if (!table.isLinked()) {
                new TablePass(source, model, table, evaluator).run(tables, relationships);
            }
        }
        return new DataProfile(tables, relationships);
    }

    private static final class TablePass {
        private final AccessSource source;
        private final SchemaModel model;
        private final TableModel table;
        private final RuleEvaluator evaluator;
        private final Map<String, Integer> position = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        private final List<ColumnModel> scanned = new ArrayList<>();
        private final List<ColumnAccumulator> columns = new ArrayList<>();
        private final List<ForeignKeyCheck> foreignKeys = new ArrayList<>();
        private RuleAccumulator tableRule;
        private long rows;

        TablePass(AccessSource source, SchemaModel model, TableModel table, RuleEvaluator evaluator) {
            this.source = source;
            this.model = model;
            this.table = table;
            this.evaluator = evaluator;
        }

        void run(Map<String, TableProfile> tables, Map<String, RelationshipProfile> relationships) throws IOException {
            plan();
            if (scanned.isEmpty()) {
                return; // no statistic decides anything for this table
            }
            RowStream stream = source.scan(table, scanned);
            while (stream.hasNext()) {
                Object[] row = stream.next();
                rows++;
                Function<String, Object> byName = name -> {
                    Integer at = position.get(name);
                    if (at == null) {
                        throw new EvaluationException("no column " + name);
                    }
                    return row[at];
                };
                for (ColumnAccumulator column : columns) {
                    column.accept(row, byName);
                }
                if (tableRule != null) {
                    tableRule.accept(byName, () -> key(row));
                }
                for (ForeignKeyCheck fk : foreignKeys) {
                    fk.accept(row, () -> key(row));
                }
            }
            tables.put(
                    table.name(),
                    new TableProfile(
                            table.name(),
                            rows,
                            columns.stream()
                                    .filter(ColumnAccumulator::reports)
                                    .map(ColumnAccumulator::stats)
                                    .toList(),
                            tableRule == null ? null : tableRule.stats()));
            for (ForeignKeyCheck fk : foreignKeys) {
                relationships.put(fk.relationship.name(), fk.profile());
            }
        }

        private void plan() throws IOException {
            Set<String> requiredByIndex = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            Set<String> uniqueKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (IndexModel index : table.allIndexes()) {
                if (index.required()) {
                    requiredByIndex.addAll(index.columnNames());
                }
                if (index.unique()) {
                    uniqueKeys.addAll(index.columnNames());
                }
            }
            boolean needsKeys = false;
            for (ColumnModel column : table.columns()) {
                ColumnAccumulator acc = new ColumnAccumulator(column);
                acc.nulls = column.required() || requiredByIndex.contains(column.name());
                acc.emptyStrings = column.type().isText() && !column.allowZeroLength();
                // Access 97 text is in a code page, whose undefined bytes can't be decoded; Unicode text always can
                acc.undecodable = column.type().isText() && model.source().codePage() != null;
                acc.digits = column.type().isExactNumeric();
                acc.fraction = column.type().isDateTime();
                acc.autoNumber = column.type() == AccessType.AUTONUMBER_LONG;
                // Whether a target's collation can keep a key Access holds unique (05, Collation)
                acc.keyText = column.type().isText() && uniqueKeys.contains(column.name());
                if (translated(column.validation())) {
                    acc.rule = new RuleAccumulator(column.validation().expr());
                    ExprColumns.referenced(column.validation().expr()).forEach(this::scan);
                    needsKeys = true;
                }
                if (acc.collects()) {
                    scan(column.name());
                    columns.add(acc);
                }
            }
            if (translated(table.validation())) {
                tableRule = new RuleAccumulator(table.validation().expr());
                ExprColumns.referenced(table.validation().expr()).forEach(this::scan);
                needsKeys = true;
            }
            for (ForeignKeyModel fk : model.relationships()) {
                // A complex child table's rows are read from its parent's cells, so none can be an orphan
                if (fk.status() == ForeignKeyModel.Status.EMIT
                        && fk.childTable().equalsIgnoreCase(table.name())
                        && !table.isComplexChild()) {
                    fk.childColumns().forEach(this::scan);
                    foreignKeys.add(new ForeignKeyCheck(fk));
                    needsKeys = true;
                }
            }
            if (needsKeys && table.primaryKey() != null) {
                table.primaryKey().columnNames().forEach(this::scan);
            }
            for (ColumnAccumulator acc : columns) {
                acc.at = position.get(acc.column.name());
            }
        }

        private void scan(String name) {
            if (!position.containsKey(name)) {
                Optional<ColumnModel> column = table.column(name);
                if (column.isPresent()) {
                    position.put(column.get().name(), scanned.size());
                    scanned.add(column.get());
                }
            }
        }

        private String key(Object[] row) {
            if (table.primaryKey() == null) {
                return "row " + rows;
            }
            return table.primaryKey().columnNames().stream()
                    .map(name -> CanonicalText.of(row[position.get(name)]))
                    .collect(Collectors.joining(", ", "(", ")"));
        }

        private final class ForeignKeyCheck {
            final ForeignKeyModel relationship;
            final ParentKeys lookup;
            final int[] childPositions;
            long checked;
            long orphans;
            long inexact;
            boolean asciiCaseOnly = true;
            final List<String> orphanSamples = new ArrayList<>();
            final List<String> inexactSamples = new ArrayList<>();

            ForeignKeyCheck(ForeignKeyModel fk) throws IOException {
                this.relationship = fk;
                TableModel parent = model.table(fk.parentTable()).orElseThrow();
                IndexModel key = parent.allIndexes().stream()
                        .filter(i -> i.unique() && i.name().equals(fk.parentKey()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("relationship " + fk.name() + ": no key "
                                + fk.parentKey() + " on " + fk.parentTable()));
                this.lookup = ParentKeys.of(source, parent, key);
                // The lookup wants the key in index order; map each index column to its child column
                this.childPositions = new int[key.columns().size()];
                for (int j = 0; j < childPositions.length; j++) {
                    int pair = indexOfIgnoreCase(
                            fk.parentColumns(), key.columnNames().get(j));
                    childPositions[j] = position.get(fk.childColumns().get(pair));
                }
            }

            void accept(Object[] row, Supplier<String> key) {
                Object[] childKey = new Object[childPositions.length];
                for (int j = 0; j < childKey.length; j++) {
                    childKey[j] = row[childPositions[j]];
                    if (childKey[j] == null) {
                        return; // MATCH SIMPLE: a key with a NULL part is never checked, in Access or in SQL
                    }
                }
                checked++;
                Optional<Object[]> parentKey;
                try {
                    parentKey = lookup.find(childKey);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                if (parentKey.isEmpty()) {
                    orphans++;
                    sample(orphanSamples, key.get() + " -> " + text(childKey));
                    return;
                }
                if (!exactlyEqual(childKey, parentKey.get())) {
                    inexact++;
                    asciiCaseOnly &= asciiCaseOnly(childKey, parentKey.get());
                    sample(inexactSamples, key.get() + ": " + text(childKey) + " matches " + text(parentKey.get()));
                }
            }

            RelationshipProfile profile() {
                return new RelationshipProfile(
                        relationship.name(),
                        checked,
                        orphans,
                        orphanSamples,
                        inexact,
                        inexact > 0 && asciiCaseOnly,
                        inexactSamples,
                        lookup.fallbackReason());
            }
        }

        private final class RuleAccumulator {
            final Expr rule;
            long violations;
            long unevaluable;
            String firstError;
            final List<String> samples = new ArrayList<>();

            RuleAccumulator(Expr rule) {
                this.rule = rule;
            }

            void accept(Function<String, Object> row, Supplier<String> key) {
                try {
                    if (evaluator.test(rule, row) == Truth.FALSE) {
                        violations++;
                        sample(samples, key.get());
                    }
                } catch (EvaluationException e) {
                    unevaluable++;
                    if (firstError == null) {
                        firstError = e.getMessage();
                    }
                }
            }

            RuleStats stats() {
                return new RuleStats(violations, unevaluable, samples, firstError);
            }
        }

        private final class ColumnAccumulator {
            final ColumnModel column;
            boolean nulls;
            boolean emptyStrings;
            boolean undecodable;
            boolean digits;
            boolean fraction;
            boolean autoNumber;
            boolean keyText;
            RuleAccumulator rule;
            Integer at;
            long nullCount;
            long emptyCount;
            long undecodableCount;
            long unsafeKeyTextCount;
            int maxDigits;
            int maxScale;
            int maxFraction;
            Long maxAuto;

            ColumnAccumulator(ColumnModel column) {
                this.column = column;
            }

            boolean collects() {
                return nulls
                        || emptyStrings
                        || undecodable
                        || digits
                        || fraction
                        || autoNumber
                        || keyText
                        || rule != null;
            }

            /** Whether there is a statistic to show: undecodable text is only reported when there is some. */
            boolean reports() {
                return nulls
                        || emptyStrings
                        || undecodableCount > 0
                        || unsafeKeyTextCount > 0
                        || digits
                        || fraction
                        || autoNumber
                        || rule != null;
            }

            void accept(Object[] row, Function<String, Object> byName) {
                Object value = row[at];
                if (value == null) {
                    nullCount++;
                } else {
                    if (undecodable && ((String) value).indexOf(REPLACEMENT) >= 0) {
                        undecodableCount++;
                    }
                    if (keyText && !KeyText.isSafe((String) value)) {
                        unsafeKeyTextCount++;
                    }
                    switch (value) {
                        case String s when emptyStrings && s.isEmpty() -> emptyCount++;
                        case BigDecimal d
                        when digits -> {
                            BigDecimal stripped = d.stripTrailingZeros();
                            maxDigits = Math.max(maxDigits, stripped.precision());
                            maxScale = Math.max(maxScale, Math.max(0, stripped.scale()));
                        }
                        case LocalDateTime t when fraction -> maxFraction = Math.max(maxFraction, fractionDigits(t));
                        case Integer i when autoNumber -> maxAuto = maxAuto == null ? i : Math.max(maxAuto, i);
                        default -> {}
                    }
                }
                if (rule != null) {
                    rule.accept(byName, () -> key(row));
                }
            }

            ColumnStats stats() {
                return new ColumnStats(
                        column.name(),
                        nulls ? nullCount : null,
                        emptyStrings ? emptyCount : null,
                        undecodableCount > 0 ? undecodableCount : null,
                        digits ? maxDigits : null,
                        digits ? maxScale : null,
                        fraction ? maxFraction : null,
                        autoNumber ? maxAuto : null,
                        rule == null ? null : rule.stats(),
                        unsafeKeyTextCount > 0 ? unsafeKeyTextCount : null);
            }
        }
    }

    private static boolean translated(CheckRule rule) {
        return rule != null && rule.isTranslated();
    }

    /** 0 for whole seconds, else the fractional digits used, up to 9 (Access stores at most 7). */
    static int fractionDigits(LocalDateTime t) {
        int nanos = t.getNano();
        if (nanos == 0) {
            return 0;
        }
        String digits = String.format(Locale.ROOT, "%09d", nanos).replaceFirst("0+$", "");
        return digits.length();
    }

    private static int indexOfIgnoreCase(List<String> names, String name) {
        for (int i = 0; i < names.size(); i++) {
            if (names.get(i).equalsIgnoreCase(name)) {
                return i;
            }
        }
        throw new IllegalStateException("column " + name + " is not part of the relationship");
    }

    private static boolean exactlyEqual(Object[] a, Object[] b) {
        for (int i = 0; i < a.length; i++) {
            if (a[i] instanceof Number x && b[i] instanceof Number y) {
                if (new BigDecimal(x.toString()).compareTo(new BigDecimal(y.toString())) != 0) {
                    return false;
                }
            } else if (!Objects.equals(a[i], b[i])) {
                return false;
            }
        }
        return true;
    }

    /** Every difference is a letter that differs only in ASCII case. */
    private static boolean asciiCaseOnly(Object[] child, Object[] parent) {
        for (int i = 0; i < child.length; i++) {
            if (Objects.equals(child[i], parent[i])) {
                continue;
            }
            if (!(child[i] instanceof String a) || !(parent[i] instanceof String b) || a.length() != b.length()) {
                return false;
            }
            for (int k = 0; k < a.length(); k++) {
                char x = a.charAt(k);
                char y = b.charAt(k);
                if (x != y && !(x < 128 && y < 128 && Character.toLowerCase(x) == Character.toLowerCase(y))) {
                    return false;
                }
            }
        }
        return true;
    }

    private static String text(Object[] values) {
        return Arrays.stream(values).map(CanonicalText::of).collect(Collectors.joining(", ", "(", ")"));
    }

    private static void sample(List<String> samples, String sample) {
        if (samples.size() < Issues.MAX_SAMPLES) {
            samples.add(sample);
        }
    }
}
