package io.lytrax.accessconverter.profile;

import io.lytrax.accessconverter.model.AccessType;
import io.lytrax.accessconverter.model.CheckRule;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.ForeignKeyModel;
import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.model.expr.Expr;
import io.lytrax.accessconverter.model.expr.ExprColumns;
import io.lytrax.accessconverter.profile.DataProfile.ColumnStats;
import io.lytrax.accessconverter.profile.DataProfile.RelationshipProfile;
import io.lytrax.accessconverter.profile.DataProfile.RuleStats;
import io.lytrax.accessconverter.profile.DataProfile.TableProfile;
import io.lytrax.accessconverter.profile.RuleEvaluator.EvaluationException;
import io.lytrax.accessconverter.profile.RuleEvaluator.TextComparison;
import io.lytrax.accessconverter.profile.RuleEvaluator.Truth;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.RowStream;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.TableFailure;
import io.lytrax.accessconverter.value.CanonicalText;
import java.io.IOException;
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

    /**
     * Access 97 text the code page doesn't fully cover (3.0.1): a byte Windows decodes to its private use area is
     * written as that character, exactly as Access shows it; a byte nothing decodes is written as U+FFFD, and the byte
     * is lost. Both are reported per column; the profile counts them.
     */
    public static void reportCodePageText(DataProfile profile, SchemaModel.Source source, Issues issues) {
        if (source.codePage() == null) {
            return;
        }
        for (var table : profile.tables().values()) {
            for (var column : table.columns()) {
                if (column.privateUse() != null) {
                    issues.add(
                            IssueCode.TEXT_PRIVATE_USE,
                            table.table(),
                            column.column(),
                            column.privateUse() + " values hold a byte code page " + source.codePage()
                                    + " leaves undefined, written as Windows and Access decode it: a private-use"
                                    + " character (U+E000 to U+F8FF), which maps back to the same byte but which most"
                                    + " fonts show as blank");
                }
                if (column.undecodable() != null) {
                    issues.add(
                            IssueCode.TEXT_UNDECODABLE,
                            table.table(),
                            column.column(),
                            column.undecodable() + " values hold a byte " + source.charset()
                                    + " can't decode; it is written as U+FFFD, and the byte is lost");
                }
            }
        }
    }

    /** A character of the BMP's private use area, where Windows decodes the bytes some code pages leave undefined. */
    private static boolean hasPrivateUse(String text) {
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c >= '\uE000' && c <= '\uF8FF') {
                return true;
            }
        }
        return false;
    }

    private DataProfiler() {}

    /** Profiles every local table; one that can't be read stops it, as {@code --on-table-error fail} does. */
    public static DataProfile profile(AccessSource source, SchemaModel model) throws IOException {
        return profile(source, model, ConvertOptions.DEFAULT, new Issues());
    }

    /** As {@link #profile(ProfileSource, SchemaModel, ConvertOptions, Issues)}, reading {@code source}. */
    public static DataProfile profile(AccessSource source, SchemaModel model, ConvertOptions options, Issues issues)
            throws IOException {
        return profile(ProfileSource.of(source), model, options, issues);
    }

    /**
     * Profiles every local table. A table that can't be read, here or as the parent of a relationship being checked,
     * is reported as {@code TABLE_READ_FAILED} and handled as {@code options.onTableError()} says: {@code fail}
     * throws, {@code continue} goes on and leaves the table without statistics, in {@link DataProfile#failedTables}.
     */
    public static DataProfile profile(ProfileSource source, SchemaModel model, ConvertOptions options, Issues issues)
            throws IOException {
        Profiling run = new Profiling(source, model, options, issues);
        for (TableModel table : model.tables()) {
            if (!table.isLinked() && !run.failed.contains(table.name())) {
                new TablePass(run, table).run();
            }
        }
        return new DataProfile(run.tables, run.relationships, run.failed);
    }

    /** Rules are evaluated as Access compares text, and as a SQLite CHECK does. */
    private record Evaluators(RuleEvaluator access, RuleEvaluator asciiNocase) {}

    /** What the passes share: the source, what they found so far and the tables that failed. */
    private static final class Profiling {
        final ProfileSource source;
        final SchemaModel model;
        final ConvertOptions options;
        final Issues issues;
        final Evaluators evaluators = new Evaluators(
                new RuleEvaluator(TextComparison.ACCESS), new RuleEvaluator(TextComparison.ASCII_NOCASE));
        final Map<String, TableProfile> tables = new LinkedHashMap<>();
        final Map<String, RelationshipProfile> relationships = new LinkedHashMap<>();
        final Set<String> failed = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        Profiling(ProfileSource source, SchemaModel model, ConvertOptions options, Issues issues) {
            this.source = source;
            this.model = model;
            this.options = options;
            this.issues = issues;
        }

        /** Reports a table that couldn't be read and drops its statistics; under {@code fail}, throws. */
        void failed(String table, Exception e) throws IOException {
            if (failed.add(table)) {
                tables.keySet().removeIf(table::equalsIgnoreCase);
                relationships
                        .values()
                        .removeIf(r -> model.relationships().stream()
                                .anyMatch(fk -> fk.name().equals(r.relationship())
                                        && (fk.parentTable().equalsIgnoreCase(table)
                                                || fk.childTable().equalsIgnoreCase(table))));
                TableFailure.readFailed(issues, options, table, e);
            }
        }
    }

    /** A relationship's parent table couldn't be read while a child row was checked: the parent's failure. */
    private static final class ParentFailed extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final String parent;

        ParentFailed(String parent, Exception cause) {
            super(cause);
            this.parent = parent;
        }
    }

    private static final class TablePass {
        private final Profiling run;
        private final ProfileSource source;
        private final SchemaModel model;
        private final TableModel table;
        private final Evaluators evaluators;
        private final Map<String, Integer> position = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        private final List<ColumnModel> scanned = new ArrayList<>();
        private final List<ColumnAccumulator> columns = new ArrayList<>();
        private final List<ForeignKeyCheck> foreignKeys = new ArrayList<>();
        private RuleAccumulator tableRule;
        private long rows;

        TablePass(Profiling run, TableModel table) {
            this.run = run;
            this.source = run.source;
            this.model = run.model;
            this.table = table;
            this.evaluators = run.evaluators;
        }

        void run() throws IOException {
            try {
                plan();
                if (scanned.isEmpty()) {
                    return; // no statistic decides anything for this table
                }
                scan();
            } catch (ParentFailed e) {
                run.failed(e.parent, (Exception) e.getCause());
                return;
            } catch (IOException | RuntimeException e) {
                run.failed(table.name(), e);
                return;
            }
            if (run.failed.contains(table.name())) {
                return; // it is its own parent, and reading its keys failed
            }
            run.tables.put(
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
                if (!run.failed.contains(fk.relationship.parentTable())) {
                    run.relationships.put(fk.relationship.name(), fk.profile());
                }
            }
        }

        private void scan() throws IOException {
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
                // A parent that couldn't be read leaves the relationship unchecked, as without profiling
                if (fk.status() == ForeignKeyModel.Status.EMIT
                        && fk.childTable().equalsIgnoreCase(table.name())
                        && !table.isComplexChild()
                        && !run.failed.contains(fk.parentTable())) {
                    fk.childColumns().forEach(this::scan);
                    try {
                        foreignKeys.add(new ForeignKeyCheck(fk));
                    } catch (ParentFailed e) {
                        parentFailed(e);
                    }
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

        /**
         * Under {@code continue} the parent is reported and the child goes on without that relationship's check; under
         * {@code fail} the pass stops, and the failure is the parent's.
         */
        private void parentFailed(ParentFailed e) throws IOException {
            if (run.options.onTableError() == ConvertOptions.OnTableError.FAIL) {
                throw e;
            }
            run.failed(e.parent, (Exception) e.getCause());
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
                try {
                    this.lookup = ParentKeys.of(source, parent, key);
                } catch (IOException | RuntimeException e) {
                    throw new ParentFailed(parent.name(), e);
                }
                // The lookup wants the key in index order; map each index column to its child column
                this.childPositions = new int[key.columns().size()];
                for (int j = 0; j < childPositions.length; j++) {
                    int pair = indexOfIgnoreCase(
                            fk.parentColumns(), key.columnNames().get(j));
                    childPositions[j] = position.get(fk.childColumns().get(pair));
                }
            }

            void accept(Object[] row, Supplier<String> key) throws IOException {
                if (run.failed.contains(relationship.parentTable())) {
                    return; // the parent failed at an earlier row, under continue
                }
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
                } catch (IOException | RuntimeException e) {
                    parentFailed(new ParentFailed(relationship.parentTable(), e));
                    return;
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
            long asciiNocaseViolations;
            String firstError;
            final List<String> samples = new ArrayList<>();
            final List<String> asciiNocaseSamples = new ArrayList<>();

            RuleAccumulator(Expr rule) {
                this.rule = rule;
            }

            void accept(Function<String, Object> row, Supplier<String> key) {
                try {
                    if (evaluators.access().test(rule, row) == Truth.FALSE) {
                        violations++;
                        sample(samples, key.get());
                    }
                    if (evaluators.asciiNocase().test(rule, row) == Truth.FALSE) {
                        asciiNocaseViolations++;
                        sample(asciiNocaseSamples, key.get());
                    }
                } catch (EvaluationException e) {
                    unevaluable++;
                    if (firstError == null) {
                        firstError = e.getMessage();
                    }
                }
            }

            RuleStats stats() {
                return new RuleStats(
                        violations, unevaluable, samples, firstError, asciiNocaseViolations, asciiNocaseSamples);
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
            long privateUseCount;
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
                        || privateUseCount > 0
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
                    if (undecodable && hasPrivateUse((String) value)) {
                        privateUseCount++;
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
                        privateUseCount > 0 ? privateUseCount : null,
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
