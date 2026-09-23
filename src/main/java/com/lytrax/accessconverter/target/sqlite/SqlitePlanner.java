package com.lytrax.accessconverter.target.sqlite;

import static com.lytrax.accessconverter.target.IdentifierPolicy.quote;

import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.model.CheckRule;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.DefaultValue;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.ForeignKeyModel.Action;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.IndexModel.IndexColumn;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.model.expr.ExprColumns;
import com.lytrax.accessconverter.profile.DataProfile;
import com.lytrax.accessconverter.profile.DataProfile.ColumnStats;
import com.lytrax.accessconverter.profile.DataProfile.RelationshipProfile;
import com.lytrax.accessconverter.profile.DataProfile.RuleStats;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.IdentifierPolicy;
import com.lytrax.accessconverter.target.sqlite.SqliteExpressions.Kind;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedColumn;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedForeignKey;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedIndex;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedKey;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedTable;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.ValueForm;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Decides what the SQLite file will look like (06): declared types, which constraints the data allows, keys,
 * indexes, foreign keys and the exact DDL. Rows are never read here; every decision about the data comes from the
 * {@link DataProfile}. Without a profile the conservative choice is taken everywhere, so nothing is ever lost.
 */
public final class SqlitePlanner {

    /** How long a comment inside the DDL may get; the whole text is in {@code --sqlite-metadata}'s tables. */
    private static final int MAX_COMMENT = 1000;

    private static final String INDENT = "  ";

    private final SchemaModel model;
    private final DataProfile profile;
    private final ConvertOptions options;
    private final SqliteOptions sqlite;
    private final Issues issues;
    private final IdentifierPolicy names = new IdentifierPolicy();
    private final List<TableDraft> drafts = new ArrayList<>();

    private SqlitePlanner(
            SchemaModel model, DataProfile profile, ConvertOptions options, SqliteOptions sqlite, Issues issues) {
        this.model = model;
        this.profile = profile;
        this.options = options;
        this.sqlite = sqlite;
        this.issues = issues;
    }

    /** @param profile null when {@code --no-profile} skipped it */
    public static SqlitePlan plan(
            SchemaModel model, DataProfile profile, ConvertOptions options, SqliteOptions sqlite, Issues issues) {
        return new SqlitePlanner(model, profile, options, sqlite, issues).build();
    }

    private SqlitePlan build() {
        if (profile == null) {
            issues.add(
                    IssueCode.PROFILE_SKIPPED,
                    null,
                    null,
                    "the data was not profiled (--no-profile): exact decimals are stored as text, NOT NULL and CHECK"
                            + " are only emitted where Access itself guarantees them, and text foreign keys compare"
                            + " case-insensitively");
        }
        for (TableModel table : model.tables()) {
            if (!table.isLinked()) {
                drafts.add(new TableDraft(table));
            }
        }
        SqlitePlan.Metadata metadata = sqlite.metadata()
                ? new SqlitePlan.Metadata(
                        names.register("_access_columns", "_access_columns", "table", issues, null),
                        names.register("_access_relationships", "_access_relationships", "table", issues, null))
                : null;
        relationships();
        for (TableDraft draft : drafts) {
            draft.checks();
            draft.indexes();
        }
        List<PlannedTable> tables = drafts.stream().map(TableDraft::freeze).toList();
        return new SqlitePlan(model, sqlite.strict(), tables, metadata, profile != null);
    }

    // ---------------------------------------------------------------- relationships

    /** Foreign keys for enforced relationships without orphans, and the index D7 asks for on the others. */
    private void relationships() {
        for (ForeignKeyModel fk : model.relationships()) {
            TableDraft child = draft(fk.childTable());
            if (child == null) {
                continue;
            }
            switch (fk.status()) {
                case EMIT -> foreignKey(fk, child);
                case NOT_ENFORCED -> child.relationshipIndex(fk);
                default -> {} // already reported by the extractor
            }
        }
    }

    private void foreignKey(ForeignKeyModel fk, TableDraft child) {
        TableDraft parent = draft(fk.parentTable());
        if (parent == null) {
            return;
        }
        List<ColumnDraft> childColumns = columnsOf(child, fk.childColumns());
        List<ColumnDraft> parentColumns = columnsOf(parent, fk.parentColumns());
        if (childColumns == null || parentColumns == null) {
            issues.add(
                    IssueCode.FK_SKIPPED_PARENT_KEY_MISSING,
                    fk.childTable(),
                    fk.name(),
                    "foreign key skipped: one of its columns is not written to the output");
            return;
        }
        RelationshipProfile stats =
                profile == null ? null : profile.relationship(fk.name()).orElse(null);
        if (stats != null && stats.orphans() > 0) {
            issues.add(
                    IssueCode.FK_SKIPPED_ORPHANS,
                    fk.childTable(),
                    fk.name(),
                    "foreign key skipped: " + stats.orphans() + " of " + stats.checked()
                            + " child rows have no parent row, which SQLite would reject",
                    String.join("; ", stats.orphanSamples()));
            return;
        }
        boolean relaxCollation =
                stats == null ? childColumns.stream().anyMatch(c -> c.kind == Kind.TEXT) : stats.inexact() > 0;
        if (stats != null && stats.inexact() > 0 && !stats.inexactAsciiCaseOnly()) {
            issues.add(
                    IssueCode.FK_SKIPPED_ORPHANS,
                    fk.childTable(),
                    fk.name(),
                    "foreign key skipped: " + stats.inexact() + " child rows match their parent only under Access's"
                            + " text comparison, and the difference is more than ASCII letter case",
                    String.join("; ", stats.inexactSamples()));
            return;
        }
        if (relaxCollation) {
            boolean relaxed = false;
            for (int i = 0; i < childColumns.size(); i++) {
                relaxed |= childColumns.get(i).relaxCollation();
                relaxed |= parentColumns.get(i).relaxCollation();
            }
            if (relaxed && stats != null) {
                issues.add(
                        IssueCode.FK_COLLATION_RELAXED,
                        fk.childTable(),
                        fk.name(),
                        "COLLATE NOCASE on both sides, because " + stats.inexact()
                                + " child rows match their parent only when letter case is ignored, as Access does",
                        String.join("; ", stats.inexactSamples()));
            }
        }
        Action onDelete = fk.onDelete();
        if (onDelete == Action.SET_NULL) {
            ColumnDraft notNull =
                    childColumns.stream().filter(c -> c.notNull).findFirst().orElse(null);
            if (notNull != null) {
                onDelete = Action.NO_ACTION;
                issues.add(
                        IssueCode.FK_SET_NULL_ON_REQUIRED,
                        fk.childTable(),
                        fk.name(),
                        "ON DELETE SET NULL downgraded to NO ACTION: " + notNull.name + " is NOT NULL");
            }
        }
        parent.parentKeys.add(fk.parentKey());
        child.foreignKeys.add(new PlannedForeignKey(
                fk,
                childColumns.stream().map(c -> c.name).toList(),
                parent.name,
                parentColumns.stream().map(c -> c.name).toList(),
                fk.onUpdate(),
                onDelete));
    }

    private TableDraft draft(String table) {
        return drafts.stream()
                .filter(d -> d.source.name().equalsIgnoreCase(table))
                .findFirst()
                .orElse(null);
    }

    /** The drafts of these columns, or null when one of them isn't written. */
    private static List<ColumnDraft> columnsOf(TableDraft table, List<String> columns) {
        List<ColumnDraft> found = new ArrayList<>(columns.size());
        for (String column : columns) {
            ColumnDraft draft = table.column(column);
            if (draft == null) {
                return null;
            }
            found.add(draft);
        }
        return found;
    }

    // ---------------------------------------------------------------- profile lookups

    private ColumnStats stats(String table, String column) {
        if (profile == null) {
            return null;
        }
        return profile.table(table).flatMap(t -> t.column(column)).orElse(null);
    }

    private RuleStats tableRule(String table) {
        if (profile == null) {
            return null;
        }
        return profile.table(table).map(DataProfile.TableProfile::tableRule).orElse(null);
    }

    // ---------------------------------------------------------------- table

    /** One table being planned: its columns, key, checks, indexes and foreign keys, and finally its DDL. */
    private final class TableDraft {
        private final TableModel source;
        private final String name;
        private final List<ColumnDraft> columns = new ArrayList<>();
        private final List<PlannedForeignKey> foreignKeys = new ArrayList<>();
        private final List<PlannedIndex> plannedIndexes = new ArrayList<>();
        private final List<String> constraints = new ArrayList<>();
        private final List<String> tableComments = new ArrayList<>();
        private final Set<String> parentKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        private final Set<String> constraintNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        private final List<ForeignKeyModel> relationshipIndexes = new ArrayList<>();
        private PlannedKey key;
        private String autoIncrementColumn;

        TableDraft(TableModel source) {
            this.source = source;
            this.name = names.register(source.name(), "table " + source.name(), "table", issues, source.name());
            Set<String> required = requiredColumns();
            Set<String> keyColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            if (source.primaryKey() != null) {
                keyColumns.addAll(source.primaryKey().columnNames());
            }
            for (int i = 0; i < source.columns().size(); i++) {
                ColumnModel column = source.columns().get(i);
                if (skip(column, keyColumns.contains(column.name()))) {
                    continue;
                }
                columns.add(new ColumnDraft(source, column, i, required.contains(column.name()), keyColumns));
            }
            if (columns.isEmpty()) {
                // A table must have at least one column; keep everything rather than write an empty one
                for (int i = 0; i < source.columns().size(); i++) {
                    ColumnModel column = source.columns().get(i);
                    columns.add(new ColumnDraft(source, column, i, required.contains(column.name()), keyColumns));
                }
            }
            primaryKey();
            if (source.description() != null) {
                tableComments.add(source.description());
            }
        }

        /** Columns Access marks as required, directly or through an index that can't hold NULLs. */
        private Set<String> requiredColumns() {
            Set<String> required = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            for (ColumnModel column : source.columns()) {
                if (column.required()) {
                    required.add(column.name());
                }
            }
            for (IndexModel index : source.allIndexes()) {
                if (index.required()) {
                    required.addAll(index.columnNames());
                }
            }
            return required;
        }

        /** Whether a column is left out of the output: version history, or a hidden system column (04). */
        private boolean skip(ColumnModel column, boolean inPrimaryKey) {
            if (inPrimaryKey) {
                return false;
            }
            if (column.type() == AccessType.VERSION_HISTORY) {
                issues.add(
                        IssueCode.VERSION_HISTORY_SKIPPED,
                        source.name(),
                        column.name(),
                        "the append-only memo's version history is not written; the memo itself is");
                return true;
            }
            if (column.hidden() && !options.includeHidden()) {
                issues.add(
                        IssueCode.HIDDEN_COLUMN_SKIPPED,
                        source.name(),
                        column.name(),
                        "Access maintains this column itself; pass --include-hidden to write it");
                return true;
            }
            return false;
        }

        /**
         * The primary key exactly as Access has it, never one inferred from an autonumber (F-32). A single ascending
         * integer column becomes SQLite's rowid alias, which is where AUTOINCREMENT can live.
         */
        private void primaryKey() {
            IndexModel pk = source.primaryKey();
            if (pk == null) {
                return;
            }
            List<ColumnDraft> keyColumns = columnsOf(this, pk.columnNames());
            if (keyColumns == null) {
                issues.add(
                        IssueCode.NO_PRIMARY_KEY,
                        source.name(),
                        pk.name(),
                        "primary key skipped: one of its columns is not written to the output");
                return;
            }
            ColumnDraft only = keyColumns.size() == 1 ? keyColumns.get(0) : null;
            boolean rowidAlias =
                    only != null && pk.columns().get(0).ascending() && only.notNull && isInteger(only.source.type());
            key = new PlannedKey(pk, pk.columns(), rowidAlias);
            if (rowidAlias) {
                only.rowidAlias = true;
                if (only.source.type() == AccessType.AUTONUMBER_LONG) {
                    autoIncrementColumn = only.name;
                }
            } else {
                // SQLite allows NULLs in a PRIMARY KEY that isn't the rowid, so the columns say NOT NULL themselves;
                // ColumnDraft.notNull already granted that, unless the profiler found NULLs there
                constraints.add("PRIMARY KEY (" + columnList(pk.columns()) + ")");
            }
            for (ColumnDraft column : columns) {
                if (column.source.type() == AccessType.AUTONUMBER_LONG && !column.name.equals(autoIncrementColumn)) {
                    issues.add(
                            IssueCode.AUTOINCREMENT_NOT_PRESERVED,
                            source.name(),
                            column.source.name(),
                            "the values are kept, but SQLite can only generate them for an INTEGER PRIMARY KEY, and"
                                    + " this autonumber is not one");
                }
            }
        }

        /** CHECK constraints: column rules, table rules and AllowZeroLength, only where the data complies. */
        void checks() {
            for (ColumnDraft column : columns) {
                column.check();
            }
            CheckRule rule = source.validation();
            if (rule == null) {
                return;
            }
            if (!rule.isTranslated()) {
                tableComments.add("Access validation rule: " + rule.raw());
                return;
            }
            if (!ruleHolds(rule, tableRule(source.name()), null)) {
                tableComments.add("Access validation rule: " + rule.raw());
                return;
            }
            SqliteExpressions.Result check = SqliteExpressions.check(rule.expr(), new ColumnLookup(this));
            if (check.isPresent()) {
                addConstraint("ck_" + source.name(), check.sql());
            } else {
                issues.add(
                        IssueCode.CHECK_UNTRANSLATABLE,
                        source.name(),
                        null,
                        "the table's validation rule " + rule.raw() + " has no SQLite CHECK: " + check.problem());
                tableComments.add("Access validation rule: " + rule.raw());
            }
        }

        /** Whether the profile proves every row satisfies a rule, so a CHECK can be emitted. */
        private boolean ruleHolds(CheckRule rule, RuleStats stats, String column) {
            if (stats == null) {
                if (profile != null) {
                    return false;
                }
                issues.add(
                        IssueCode.CHECK_VIOLATED_BY_DATA,
                        source.name(),
                        column,
                        "no CHECK for the validation rule " + rule.raw()
                                + ": without profiling (--no-profile) the data can't be shown to comply");
                return false;
            }
            if (stats.holds()) {
                return true;
            }
            issues.add(
                    IssueCode.CHECK_VIOLATED_BY_DATA,
                    source.name(),
                    column,
                    "no CHECK for the validation rule " + rule.raw() + ": "
                            + (stats.violations() > 0
                                    ? stats.violations() + " existing rows violate it, as Access allows"
                                    : stats.unevaluable() + " rows could not be checked (" + stats.firstError() + ")"),
                    String.join("; ", stats.samples()));
            return false;
        }

        void addConstraint(String preferred, String check) {
            String constraint = preferred;
            for (int n = 2; constraintNames.contains(constraint); n++) {
                constraint = preferred + "_" + n;
            }
            constraintNames.add(constraint);
            constraints.add("CONSTRAINT " + quote(constraint) + " CHECK (" + check + ")");
        }

        /** Remembers that a relationship Access doesn't back with an index needs one here (D7). */
        void relationshipIndex(ForeignKeyModel fk) {
            relationshipIndexes.add(fk);
        }

        /** The secondary indexes: Access's own, then the ones the planner adds for relationships. */
        void indexes() {
            for (IndexModel index : source.indexes()) {
                index(index, index.name(), index.unique());
            }
            for (ForeignKeyModel fk : relationshipIndexes) {
                if (covered(fk.childColumns())) {
                    continue;
                }
                List<IndexColumn> columns = fk.childColumns().stream()
                        .map(c -> new IndexColumn(c, true))
                        .toList();
                if (index(
                        new IndexModel(
                                fk.name(),
                                columns,
                                false,
                                false,
                                false,
                                false,
                                IndexModel.Origin.RELATIONSHIP,
                                List.of(fk.name())),
                        fk.name(),
                        false)) {
                    issues.add(
                            IssueCode.INDEX_ADDED_FOR_RELATIONSHIP,
                            source.name(),
                            fk.name(),
                            "index on " + String.join(", ", fk.childColumns())
                                    + ": Access creates none for a relationship without referential integrity");
                }
            }
        }

        /** Whether a kept index or the primary key already starts with these columns, so lookups are covered. */
        private boolean covered(List<String> columns) {
            List<List<String>> existing = new ArrayList<>();
            if (key != null) {
                existing.add(key.columnNames());
            }
            plannedIndexes.forEach(i -> existing.add(i.columnNames()));
            for (List<String> candidate : existing) {
                if (candidate.size() < columns.size()) {
                    continue;
                }
                boolean prefix = true;
                for (int i = 0; i < columns.size(); i++) {
                    prefix &= candidate.get(i).equalsIgnoreCase(columns.get(i));
                }
                if (prefix) {
                    return true;
                }
            }
            return false;
        }

        private boolean index(IndexModel index, String preferred, boolean unique) {
            List<ColumnDraft> indexColumns = columnsOf(this, index.columnNames());
            if (indexColumns == null) {
                return false;
            }
            ColumnDraft complex = indexColumns.stream()
                    .filter(c -> c.source.type().isComplex())
                    .findFirst()
                    .orElse(null);
            if (complex != null) {
                // Access's hidden index on an attachment or multi-value column indexes data we don't store (F-16)
                issues.add(
                        IssueCode.INDEX_SKIPPED_COMPLEX_COLUMN,
                        source.name(),
                        index.name(),
                        "index skipped: " + complex.source.name() + " is an Access "
                                + complex.source
                                        .type()
                                        .name()
                                        .toLowerCase(java.util.Locale.ROOT)
                                        .replace('_', ' ')
                                + " column, whose values become child tables");
                return false;
            }
            String where = null;
            if (index.ignoreNulls()) {
                if (unique && parentKeys.contains(index.name())) {
                    // SQLite refuses a partial index as a foreign key's parent key ("foreign key mismatch")
                    issues.add(
                            IssueCode.PARTIAL_INDEX_DROPPED_PARENT_KEY,
                            source.name(),
                            index.name(),
                            "Access leaves all-NULL keys out of this index, but SQLite can't use a partial index as a"
                                    + " foreign key's parent: the index covers every row instead, which accepts the"
                                    + " same values (NULLs never collide in SQLite either)");
                } else {
                    where = index.columnNames().stream()
                            .map(c -> quote(column(c).name) + " IS NOT NULL")
                            .collect(Collectors.joining(" OR "));
                }
            }
            String indexName = names.register(
                    source.name() + "_" + preferred,
                    "index " + source.name() + "." + index.name(),
                    "index",
                    issues,
                    source.name());
            List<IndexColumn> spelled = index.columns().stream()
                    .map(c -> new IndexColumn(column(c.name()).name, c.ascending()))
                    .toList();
            String sql = "CREATE " + (unique ? "UNIQUE " : "") + "INDEX " + quote(indexName) + " ON " + quote(name)
                    + " (" + columnList(spelled) + ")" + (where == null ? "" : " WHERE " + where);
            plannedIndexes.add(new PlannedIndex(index, name, indexName, spelled, unique, where, sql));
            return true;
        }

        ColumnDraft column(String accessName) {
            return columns.stream()
                    .filter(c -> c.source.name().equalsIgnoreCase(accessName))
                    .findFirst()
                    .orElse(null);
        }

        private String columnList(List<IndexColumn> indexColumns) {
            return indexColumns.stream()
                    .map(c -> quote(column(c.name()) == null ? c.name() : column(c.name()).name)
                            + (c.ascending() ? "" : " DESC"))
                    .collect(Collectors.joining(", "));
        }

        PlannedTable freeze() {
            return new PlannedTable(
                    source,
                    name,
                    columns.stream().map(ColumnDraft::freeze).toList(),
                    key,
                    plannedIndexes,
                    foreignKeys,
                    autoIncrementColumn,
                    autoIncrementColumn == null ? null : seed(),
                    createTable());
        }

        /** What {@code sqlite_sequence} must hold so the next generated id is one Access hasn't used (04). */
        private Long seed() {
            ColumnStats stats = stats(source.name(), autoIncrementColumn);
            return stats == null ? null : stats.maxAutoNumber();
        }

        /** {@code FOREIGN KEY (…) REFERENCES …}: inline, because SQLite can't add a constraint later. */
        private String foreignKey(PlannedForeignKey fk) {
            StringBuilder sql = new StringBuilder("FOREIGN KEY (")
                    .append(quoted(fk.childColumns()))
                    .append(") REFERENCES ")
                    .append(quote(fk.parentTable()))
                    .append(" (")
                    .append(quoted(fk.parentColumns()))
                    .append(")");
            if (fk.onUpdate() == Action.CASCADE) {
                sql.append(" ON UPDATE CASCADE");
            }
            switch (fk.onDelete()) {
                case CASCADE -> sql.append(" ON DELETE CASCADE");
                case SET_NULL -> sql.append(" ON DELETE SET NULL");
                case NO_ACTION -> {} // SQLite's default, and what Access does without a cascade rule (F-33)
            }
            return sql.toString();
        }

        private static String quoted(List<String> columns) {
            return columns.stream().map(IdentifierPolicy::quote).collect(Collectors.joining(", "));
        }

        private String createTable() {
            List<String> items = new ArrayList<>();
            List<String> comments = new ArrayList<>();
            for (ColumnDraft column : columns) {
                items.add(column.definition());
                comments.add(column.comment());
            }
            for (String constraint : constraints) {
                items.add(constraint);
                comments.add(null);
            }
            for (PlannedForeignKey fk : foreignKeys) {
                items.add(foreignKey(fk));
                // SQLite has no name for a foreign key, so the Access relationship's name goes in a comment
                comments.add("Access relationship: " + fk.source().name());
            }
            StringBuilder sql =
                    new StringBuilder("CREATE TABLE ").append(quote(name)).append(" (\n");
            for (String comment : tableComments) {
                sql.append(INDENT).append(comment(comment)).append('\n');
            }
            for (int i = 0; i < items.size(); i++) {
                sql.append(INDENT).append(items.get(i));
                if (i < items.size() - 1) {
                    sql.append(',');
                }
                if (comments.get(i) != null) {
                    sql.append(' ').append(comment(comments.get(i)));
                }
                sql.append('\n');
            }
            return sql.append(')').append(sqlite.strict() ? " STRICT" : "").toString();
        }
    }

    // ---------------------------------------------------------------- column

    /** One column being planned. Mutable while relationships and checks are decided, then frozen. */
    private final class ColumnDraft {
        private final TableModel table;
        private final ColumnModel source;
        private final int sourceIndex;
        private final Kind kind;
        private final ValueForm form;
        private final int fractionDigits;
        private final String name;
        private final List<String> comments = new ArrayList<>();
        private boolean notNull;
        private boolean nocase;
        private boolean rowidAlias;
        private String defaultSql;

        ColumnDraft(TableModel table, ColumnModel source, int sourceIndex, boolean required, Set<String> keyColumns) {
            this.table = table;
            this.source = source;
            this.sourceIndex = sourceIndex;
            this.name = source.name();
            this.kind = kind(source.type());
            boolean decimalAsText = source.type().isExactNumeric() && decimalAsText();
            this.form = form(source.type(), decimalAsText);
            this.fractionDigits = fractionDigits();
            this.nocase = sqlite.nocase() && kind == Kind.TEXT;
            this.notNull = notNull(required, keyColumns.contains(source.name()));
            defaults();
            if (source.isCalculated()) {
                comments.add("Access expression: " + source.calculatedExpression());
            }
            if (source.description() != null) {
                comments.add(source.description());
            }
        }

        /** Whether an exact decimal must be kept as text because a NUMERIC column would round it (F-06). */
        private boolean decimalAsText() {
            if (sqlite.strict()) {
                return true;
            }
            ColumnStats stats = stats(table.name(), source.name());
            Integer digits = stats == null ? null : stats.maxSignificantDigits();
            boolean asText = digits == null || !SqliteValues.fitsNumeric(digits);
            if (asText) {
                issues.add(
                        IssueCode.DECIMAL_STORED_AS_TEXT,
                        table.name(),
                        source.name(),
                        (digits == null
                                        ? "stored as exact text: without profiling (--no-profile) the values could"
                                                + " need more than the "
                                                + SqliteValues.EXACT_DECIMAL_DIGITS
                                                + " significant digits SQLite gives back unchanged"
                                        : "stored as exact text: the values need " + digits
                                                + " significant digits, and SQLite keeps a number in a REAL, which"
                                                + " gives back at most " + SqliteValues.EXACT_DECIMAL_DIGITS
                                                + " of them unchanged")
                                + ". The column therefore sorts and aggregates as text; CAST(" + source.name()
                                + " AS REAL) compares and sums it as a number again");
            }
            return asText;
        }

        private int fractionDigits() {
            if (!source.type().isDateTime()) {
                return 0;
            }
            if (source.type() == AccessType.EXT_DATE_TIME) {
                return SqliteValues.EXTENDED_FRACTION_DIGITS;
            }
            ColumnStats stats = stats(table.name(), source.name());
            Integer used = stats == null ? null : stats.maxFractionDigits();
            if (used == null) {
                return SqliteValues.DEFAULT_FRACTION_DIGITS;
            }
            return used == 0 ? 0 : Math.max(SqliteValues.DEFAULT_FRACTION_DIGITS, used);
        }

        /**
         * NOT NULL only where the data allows it (05, Constraints): Access's own guarantees (Yes/No, autonumbers and
         * primary keys) plus Required columns whose profiled NULL count is zero.
         */
        private boolean notNull(boolean required, boolean inPrimaryKey) {
            boolean guaranteed =
                    source.type() == AccessType.BOOLEAN || source.type().isAutoNumber() || inPrimaryKey;
            if (!guaranteed && !required) {
                return false;
            }
            ColumnStats stats = stats(table.name(), source.name());
            Long nulls = stats == null ? null : stats.nulls();
            if (nulls == null) {
                return guaranteed;
            }
            if (nulls == 0) {
                return true;
            }
            issues.add(
                    IssueCode.NOT_NULL_DROPPED_NULLS_PRESENT,
                    table.name(),
                    source.name(),
                    "the column is nullable in the output: Access marks it as required, but " + nulls
                            + " rows hold NULL");
            return false;
        }

        private void defaults() {
            DefaultValue value = source.defaultValue();
            if (source.type() == AccessType.AUTONUMBER_GUID) {
                // Access generates a Replication ID itself; SQLite can do the same
                defaultSql = SqliteExpressions.RANDOM_GUID;
                return;
            }
            if (value == null) {
                // An Access Yes/No column without a default is No
                defaultSql = source.type() == AccessType.BOOLEAN ? "0" : null;
                return;
            }
            if (!value.isTranslated()) {
                comments.add("Access default: " + value.raw());
                return;
            }
            SqliteExpressions.Result result = SqliteExpressions.defaultClause(value.expr(), kind, fractionDigits);
            if (result.isPresent()) {
                defaultSql = result.sql();
                return;
            }
            issues.add(
                    result.typeMismatch() ? IssueCode.DEFAULT_DROPPED_TYPE_MISMATCH : IssueCode.DEFAULT_UNTRANSLATABLE,
                    table.name(),
                    source.name(),
                    "the default " + value.raw() + " is not written: " + result.problem());
            comments.add("Access default: " + value.raw());
        }

        /** The column's CHECKs: its validation rule, and {@code <> ''} when Access rejects the empty string. */
        void check() {
            TableDraft draft = draft(table.name());
            ColumnStats stats = stats(table.name(), source.name());
            CheckRule rule = source.validation();
            if (rule != null) {
                if (rule.isTranslated() && draft.ruleHolds(rule, stats == null ? null : stats.rule(), source.name())) {
                    checkFor(draft, rule);
                } else {
                    comments.add("Access validation rule: " + rule.raw());
                }
            }
            Long empty = stats == null ? null : stats.emptyStrings();
            if (kind == Kind.TEXT && !source.allowZeroLength() && empty != null && empty == 0) {
                draft.addConstraint("ck_" + source.name() + "_nonempty", quote(name) + " <> ''");
            }
        }

        private void checkFor(TableDraft draft, CheckRule rule) {
            Set<String> referenced = ExprColumns.referenced(rule.expr());
            for (String other : referenced) {
                ColumnDraft column = draft.column(other);
                if (column != null && column.form == ValueForm.DECIMAL_TEXT) {
                    issues.add(
                            IssueCode.CHECK_UNTRANSLATABLE,
                            table.name(),
                            source.name(),
                            "the validation rule " + rule.raw() + " has no SQLite CHECK: " + other
                                    + " is stored as exact text, which doesn't compare as a number");
                    comments.add("Access validation rule: " + rule.raw());
                    return;
                }
            }
            SqliteExpressions.Result result = SqliteExpressions.check(rule.expr(), new ColumnLookup(draft));
            if (result.isPresent()) {
                draft.addConstraint("ck_" + source.name(), result.sql());
            } else {
                issues.add(
                        IssueCode.CHECK_UNTRANSLATABLE,
                        table.name(),
                        source.name(),
                        "the validation rule " + rule.raw() + " has no SQLite CHECK: " + result.problem());
                comments.add("Access validation rule: " + rule.raw());
            }
        }

        /** Compares this column case-insensitively, as Access does. Returns whether that changed anything. */
        boolean relaxCollation() {
            if (kind != Kind.TEXT || nocase) {
                return false;
            }
            nocase = true;
            return true;
        }

        String definition() {
            StringBuilder sql = new StringBuilder(quote(name)).append(' ').append(declaredType());
            if (rowidAlias) {
                sql.append(" PRIMARY KEY");
                if (name.equals(draft(table.name()).autoIncrementColumn)) {
                    sql.append(" AUTOINCREMENT");
                }
            }
            if (notNull) {
                sql.append(" NOT NULL");
            }
            if (defaultSql != null) {
                sql.append(" DEFAULT ").append(defaultSql);
            }
            if (nocase) {
                sql.append(" COLLATE NOCASE");
            }
            return sql.toString();
        }

        String comment() {
            return comments.isEmpty() ? null : String.join("; ", comments);
        }

        /** The declared type: descriptive, and chosen so SQLite's affinity stores every value correctly (06). */
        String declaredType() {
            if (rowidAlias) {
                return "INTEGER"; // the only spelling SQLite accepts for a rowid alias
            }
            if (sqlite.strict()) {
                return switch (form) {
                    case BOOLEAN_INT, INTEGER, LONG, COMPLEX_ID -> "INTEGER";
                    case REAL, REAL_FROM_FLOAT -> "REAL";
                    case DECIMAL_NUMBER, DECIMAL_TEXT, DATE_TEXT, TEXT -> "TEXT";
                    case BLOB -> "BLOB";
                };
            }
            return switch (source.type()) {
                case BOOLEAN -> "BOOLEAN";
                case BYTE -> "TINYINT";
                case INT -> "SMALLINT";
                case LONG, AUTONUMBER_LONG -> "INTEGER";
                case BIG_INT -> "BIGINT";
                case FLOAT -> "FLOAT";
                case DOUBLE -> "DOUBLE";
                case MONEY -> form == ValueForm.DECIMAL_TEXT ? "TEXT" : "DECIMAL(19,4)";
                case NUMERIC ->
                    form == ValueForm.DECIMAL_TEXT
                            ? "TEXT"
                            : "DECIMAL(" + source.precision() + "," + source.scale() + ")";
                case SHORT_DATE_TIME, EXT_DATE_TIME -> "DATETIME";
                case TEXT -> "VARCHAR(" + (source.length() == null ? 255 : source.length()) + ")";
                case MEMO, HYPERLINK -> "TEXT";
                case GUID, AUTONUMBER_GUID -> "CHAR(38)";
                case BINARY, OLE, UNSUPPORTED -> "BLOB";
                case ATTACHMENT, MULTI_VALUE, VERSION_HISTORY, COMPLEX_UNSUPPORTED -> "INTEGER";
            };
        }

        PlannedColumn freeze() {
            return new PlannedColumn(
                    source, sourceIndex, name, declaredType(), notNull, nocase, defaultSql, form, fractionDigits);
        }
    }

    /** Looks up the columns an expression refers to while a table's CHECKs are rendered. */
    private record ColumnLookup(TableDraft table) implements SqliteExpressions.Columns {
        @Override
        public Optional<String> name(String accessName) {
            ColumnDraft column = table.column(accessName);
            return column == null ? Optional.empty() : Optional.of(column.name);
        }

        @Override
        public Kind kind(String accessName) {
            ColumnDraft column = table.column(accessName);
            return column == null ? Kind.NUMERIC : column.kind;
        }

        @Override
        public int fractionDigits(String accessName) {
            ColumnDraft column = table.column(accessName);
            return column == null ? 0 : column.fractionDigits;
        }
    }

    // ---------------------------------------------------------------- static mapping

    private static boolean isInteger(AccessType type) {
        return switch (type) {
            case BYTE, INT, LONG, AUTONUMBER_LONG, BIG_INT -> true;
            default -> false;
        };
    }

    static Kind kind(AccessType type) {
        return switch (type) {
            case BOOLEAN, BYTE, INT, LONG, AUTONUMBER_LONG, BIG_INT, FLOAT, DOUBLE, MONEY, NUMERIC -> Kind.NUMERIC;
            case SHORT_DATE_TIME, EXT_DATE_TIME -> Kind.DATE;
            case TEXT, MEMO, HYPERLINK -> Kind.TEXT;
            case GUID, AUTONUMBER_GUID -> Kind.GUID;
            case BINARY, OLE, UNSUPPORTED -> Kind.BLOB;
            case ATTACHMENT, MULTI_VALUE, VERSION_HISTORY, COMPLEX_UNSUPPORTED -> Kind.NUMERIC;
        };
    }

    static ValueForm form(AccessType type, boolean decimalAsText) {
        return switch (type) {
            case BOOLEAN -> ValueForm.BOOLEAN_INT;
            case BYTE, INT, LONG, AUTONUMBER_LONG -> ValueForm.INTEGER;
            case BIG_INT -> ValueForm.LONG;
            case FLOAT -> ValueForm.REAL_FROM_FLOAT;
            case DOUBLE -> ValueForm.REAL;
            case MONEY, NUMERIC -> decimalAsText ? ValueForm.DECIMAL_TEXT : ValueForm.DECIMAL_NUMBER;
            case SHORT_DATE_TIME, EXT_DATE_TIME -> ValueForm.DATE_TEXT;
            case TEXT, MEMO, HYPERLINK, GUID, AUTONUMBER_GUID -> ValueForm.TEXT;
            case BINARY, OLE, UNSUPPORTED -> ValueForm.BLOB;
            case ATTACHMENT, MULTI_VALUE, VERSION_HISTORY, COMPLEX_UNSUPPORTED -> ValueForm.COMPLEX_ID;
        };
    }

    /** A one-line SQL comment: no line breaks, and short enough to keep the DDL readable. */
    static String comment(String text) {
        String flat = text.replaceAll("\\s+", " ").strip();
        if (flat.length() > MAX_COMMENT) {
            flat = flat.substring(0, MAX_COMMENT) + "…";
        }
        return "-- " + flat;
    }
}
