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
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.target.BinaryCells;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.IdentifierPolicy;
import com.lytrax.accessconverter.target.PlanRules;
import com.lytrax.accessconverter.target.Rendered;
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
    private final PlanRules rules;
    private final IdentifierPolicy names = new IdentifierPolicy(IdentifierPolicy.SQLITE);
    private final List<TableDraft> drafts = new ArrayList<>();

    private SqlitePlanner(
            SchemaModel model, DataProfile profile, ConvertOptions options, SqliteOptions sqlite, Issues issues) {
        this.model = model;
        this.profile = profile;
        this.options = options;
        this.sqlite = sqlite;
        this.issues = issues;
        this.rules = new PlanRules(profile, options, issues, "SQLite");
    }

    /** @param profile null when {@code --no-profile} skipped it */
    public static SqlitePlan plan(
            SchemaModel model, DataProfile profile, ConvertOptions options, SqliteOptions sqlite, Issues issues) {
        return new SqlitePlanner(model, profile, options, sqlite, issues).build();
    }

    private SqlitePlan build() {
        rules.reportProfileSkipped("exact decimals are stored as text, NOT NULL and CHECK are only emitted where"
                + " Access itself guarantees them, and text foreign keys compare case-insensitively");
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
        return new SqlitePlan(model, sqlite.strict(), tables, metadata, profile != null, options);
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
        PlanRules.ForeignKeyData data = rules.foreignKeyData(fk, false);
        if (data == PlanRules.ForeignKeyData.BLOCKED) {
            return;
        }
        RelationshipProfile stats = rules.relationship(fk);
        boolean relaxCollation = data == PlanRules.ForeignKeyData.UNKNOWN
                ? childColumns.stream().anyMatch(c -> c.kind == Kind.TEXT)
                : data == PlanRules.ForeignKeyData.ASCII_CASE_ONLY;
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
        Action onDelete = rules.onDelete(
                fk,
                childColumns.stream()
                        .filter(c -> c.notNull)
                        .map(c -> c.name)
                        .findFirst()
                        .orElse(null));
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
            Set<String> required = PlanRules.requiredColumns(source);
            Set<String> keyColumns = PlanRules.primaryKeyColumns(source);
            for (PlanRules.WrittenColumn written : rules.writtenColumns(source, true)) {
                ColumnModel column = written.column();
                columns.add(new ColumnDraft(
                        source,
                        column,
                        written.sourceIndex(),
                        written.olePart(),
                        required.contains(column.name()),
                        keyColumns));
            }
            primaryKey();
            if (source.description() != null) {
                tableComments.add(source.description());
            }
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
                    if (only.source.isRandomAutoNumber()) {
                        issues.add(
                                IssueCode.AUTONUMBER_RANDOM_SEQUENTIAL,
                                source.name(),
                                only.source.name(),
                                "Access generates random values for this autonumber (New Values: Random); SQLite's"
                                        + " AUTOINCREMENT generates them in sequence, after the largest value. SQLite's"
                                        + " counter is 64-bit, so it never runs out, but a generated value past"
                                        + " 2147483647 no longer fits an Access Long. The existing values are kept"
                                        + " exactly");
                    }
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
            if (!rules.ruleHoldsUnderAsciiNocase(source, rule, null)) {
                tableComments.add("Access validation rule: " + rule.raw());
                return;
            }
            Rendered check = SqliteExpressions.check(rule.expr(), new ColumnLookup(this));
            if (check.isPresent()) {
                addConstraint("ck_" + source.name(), check.sql());
            } else {
                rules.checkUntranslatable(source, null, rule, check.problem());
                tableComments.add("Access validation rule: " + rule.raw());
            }
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
                if (index(PlanRules.relationshipIndex(fk), fk.name(), false)) {
                    rules.indexAddedForRelationship(
                            fk, "Access creates none for a relationship without referential integrity");
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
            return PlanRules.covered(existing, columns);
        }

        private boolean index(IndexModel index, String preferred, boolean unique) {
            List<ColumnDraft> indexColumns = columnsOf(this, index.columnNames());
            if (indexColumns == null) {
                return false;
            }
            if (rules.skipsComplexIndex(source, index, true)) {
                return false; // F-16
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
            ColumnStats stats = rules.stats(source.name(), autoIncrementColumn);
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
        private final PlanRules.OlePart olePart;
        private final Kind kind;
        private final ValueForm form;
        private final int fractionDigits;
        private final String name;
        private final List<String> comments = new ArrayList<>();
        private boolean notNull;
        private boolean nocase;
        private boolean rowidAlias;
        private String defaultSql;

        ColumnDraft(
                TableModel table,
                ColumnModel source,
                int sourceIndex,
                PlanRules.OlePart olePart,
                boolean required,
                Set<String> keyColumns) {
            this.table = table;
            this.source = source;
            this.sourceIndex = sourceIndex;
            this.olePart = olePart;
            this.name = source.name();
            this.kind = kind(source.type());
            boolean decimalAsText = source.type().isExactNumeric() && decimalAsText();
            this.form = switch (BinaryCells.storage(source, options)) {
                case PATH -> ValueForm.PATH;
                case SIZE -> ValueForm.SIZE;
                case VALUE -> form(source.type(), decimalAsText);
            };
            this.fractionDigits = fractionDigits();
            this.nocase = sqlite.nocase() && kind == Kind.TEXT;
            this.notNull = rules.notNull(table, source, required, keyColumns.contains(source.name()));
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
            ColumnStats stats = rules.stats(table.name(), source.name());
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
            ColumnStats stats = rules.stats(table.name(), source.name());
            Integer used = stats == null ? null : stats.maxFractionDigits();
            if (used == null) {
                return SqliteValues.DEFAULT_FRACTION_DIGITS;
            }
            return used == 0 ? 0 : Math.max(SqliteValues.DEFAULT_FRACTION_DIGITS, used);
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
            // GenUniqueID() is the Random setting of the autonumber, not a default (reported with the key)
            if (!value.isTranslated() || source.isRandomAutoNumber()) {
                comments.add("Access default: " + value.raw());
                return;
            }
            Rendered result = SqliteExpressions.defaultClause(value.expr(), kind, fractionDigits);
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
            CheckRule rule = source.validation();
            if (rule != null) {
                if (rule.isTranslated() && rules.ruleHoldsUnderAsciiNocase(table, rule, source.name())) {
                    checkFor(draft, rule);
                } else {
                    comments.add("Access validation rule: " + rule.raw());
                }
            }
            if (rules.noEmptyStrings(table, source)) {
                draft.addConstraint("ck_" + source.name() + "_nonempty", quote(name) + " <> ''");
            }
        }

        private void checkFor(TableDraft draft, CheckRule rule) {
            Set<String> referenced = ExprColumns.referenced(rule.expr());
            for (String other : referenced) {
                ColumnDraft column = draft.column(other);
                if (column != null && column.form == ValueForm.DECIMAL_TEXT) {
                    rules.checkUntranslatable(
                            table,
                            source.name(),
                            rule,
                            other + " is stored as exact text, which doesn't compare as a number");
                    comments.add("Access validation rule: " + rule.raw());
                    return;
                }
            }
            Rendered result = SqliteExpressions.check(rule.expr(), new ColumnLookup(draft));
            if (result.isPresent()) {
                draft.addConstraint("ck_" + source.name(), result.sql());
            } else {
                rules.checkUntranslatable(table, source.name(), rule, result.problem());
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
                    case BOOLEAN_INT, INTEGER, LONG, SIZE, COMPLEX_ID -> "INTEGER";
                    case REAL, REAL_FROM_FLOAT -> "REAL";
                    case DECIMAL_NUMBER, DECIMAL_TEXT, DATE_TEXT, TEXT, PATH -> "TEXT";
                    case BLOB -> "BLOB";
                };
            }
            if (form == ValueForm.PATH) {
                return "VARCHAR(" + BinaryCells.PATH_LENGTH + ")";
            }
            if (form == ValueForm.SIZE) {
                return "BIGINT";
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
                    source,
                    sourceIndex,
                    name,
                    declaredType(),
                    notNull,
                    nocase,
                    defaultSql,
                    form,
                    fractionDigits,
                    olePart);
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
