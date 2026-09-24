package com.lytrax.accessconverter.target.mysql;

import static com.lytrax.accessconverter.target.mysql.MySqlLiterals.identifier;

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
import com.lytrax.accessconverter.profile.KeyText;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.target.BinaryCells;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.IdentifierPolicy;
import com.lytrax.accessconverter.target.PlanRules;
import com.lytrax.accessconverter.target.PlanRules.ForeignKeyData;
import com.lytrax.accessconverter.target.Rendered;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.KeyPart;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedCheck;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedColumn;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedForeignKey;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedIndex;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedTable;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.ValueForm;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Decides what the MySQL/MariaDB dump will create (05): column types, which constraints the data allows, keys within
 * InnoDB's limits, foreign keys and the exact statements. Rows are never read here; every decision about the data
 * comes from the {@link DataProfile}. Without a profile the conservative choice is taken everywhere.
 *
 * <p>The order follows the dependencies: columns, then keys and indexes (whose size decides which stay unique),
 * then relationships (which need the parent's unique key and add indexes of their own), then the AUTO_INCREMENT key
 * and the row size (which need every index), then CHECKs (which need the foreign keys' actions), and last the
 * defaults (which need the final types).
 */
public final class MySqlPlanner {

    /** InnoDB's largest index key, in bytes (05, measured: exactly 3072 passes, 3073 fails). */
    static final int MAX_KEY_BYTES = 3072;

    /** MySQL's largest row, in bytes, counting each column's maximum and the NULL bitmap (F-18, measured). */
    static final int MAX_ROW_BYTES = 65_535;

    /**
     * The most bytes an InnoDB row may keep in its page: under half a 16 KB page. MySQL 8.0 and MariaDB check it when
     * they create a table (ERROR 1118 "Row size too large (> 8126)"); MySQL 8.4 doesn't, but a MySQL dump must load
     * on 8.0 too.
     */
    static final int MAX_INLINE_BYTES = 8125;

    /** A variable-length column up to this many bytes is always stored in its row. */
    private static final int SHORT_VARCHAR_BYTES = 255;

    /**
     * What a column that can move off the page counts inline, measured to the byte: MariaDB counts its 20-byte pointer
     * and a length byte, MySQL 8.0 the 40 bytes InnoDB may keep locally and a length byte.
     */
    private static long offPageBytes(MySqlDialect dialect) {
        return dialect == MySqlDialect.MARIADB ? 21 : 41;
    }

    /** Access indexes the first 255 characters of a Long Text (05), so a Long Text key part is that long. */
    static final int LONG_TEXT_KEY_PREFIX = 255;

    static final int MAX_COLUMN_COMMENT = 1024;
    static final int MAX_TABLE_COMMENT = 2048;

    /** Never stricter than Access, for the key text the default collation would compare more strictly (05). */
    static final String BINARY_COLLATION = "utf8mb4_bin";

    /** utf8mb4 needs up to 4 bytes per character. */
    private static final int BYTES_PER_CHAR = 4;

    private static final String INDENT = "  ";

    private final SchemaModel model;
    private final ConvertOptions options;
    private final MySqlOptions mysql;
    private final Issues issues;
    private final PlanRules rules;
    private final IdentifierPolicy tableNames = new IdentifierPolicy(IdentifierPolicy.MYSQL);
    /** Foreign-key and CHECK names are unique per database. */
    private final IdentifierPolicy constraintNames = new IdentifierPolicy(IdentifierPolicy.MYSQL);

    private final List<TableDraft> drafts = new ArrayList<>();

    private MySqlPlanner(
            SchemaModel model, DataProfile profile, ConvertOptions options, MySqlOptions mysql, Issues issues) {
        this.model = model;
        this.options = options;
        this.mysql = mysql;
        this.issues = issues;
        this.rules = new PlanRules(profile, options, issues, mysql.dialect().displayName());
    }

    /** @param profile null when {@code --no-profile} skipped it */
    public static MySqlPlan plan(
            SchemaModel model, DataProfile profile, ConvertOptions options, MySqlOptions mysql, Issues issues) {
        return new MySqlPlanner(model, profile, options, mysql, issues).build();
    }

    private MySqlPlan build() {
        rules.reportProfileSkipped("NOT NULL and CHECK are only emitted where Access itself guarantees them, dates"
                + " keep milliseconds, and foreign keys are written without an orphan check, so the import fails if"
                + " the data has orphans");
        for (TableModel table : model.tables()) {
            if (!table.isLinked()) {
                drafts.add(new TableDraft(table));
            }
        }
        binaryKeyText();
        for (ForeignKeyModel fk : model.relationships()) {
            TableDraft child = draft(fk.childTable());
            if (child != null && fk.status() == ForeignKeyModel.Status.NOT_ENFORCED) {
                child.relationshipIndex(fk);
            }
        }
        for (ForeignKeyModel fk : model.relationships()) {
            if (fk.status() == ForeignKeyModel.Status.EMIT) {
                foreignKey(fk);
            }
        }
        for (TableDraft draft : drafts) {
            draft.autoIncrementKey();
            draft.rowSize();
            draft.checks();
        }
        List<PlannedTable> tables = drafts.stream().map(TableDraft::freeze).toList();
        return new MySqlPlan(model, mysql.dialect(), mysql.effectiveCollation(), tables, rules.profiled(), options);
    }

    /**
     * The key text the default collation would compare more strictly than Access (05, Collation): a primary-key or
     * unique column holding a character outside {@link KeyText}'s safe set, or any such column without a profile, is
     * compared with {@code utf8mb4_bin}, which is never stricter than Access. Foreign keys need the same collation on
     * both sides, so the partners of such a column follow it. An explicit {@code --collation} is the user's choice
     * and is left alone.
     */
    private void binaryKeyText() {
        if (mysql.collation() != null) {
            return;
        }
        for (TableDraft table : drafts) {
            Set<String> uniqueKeys = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            table.source.allIndexes().stream()
                    .filter(IndexModel::unique)
                    .forEach(i -> uniqueKeys.addAll(i.columnNames()));
            for (ColumnDraft column : table.columns) {
                if (!column.source.type().isText() || !uniqueKeys.contains(column.source.name())) {
                    continue;
                }
                ColumnStats stats = rules.stats(table.source.name(), column.source.name());
                if (!rules.profiled()) {
                    column.binary("without profiling (--no-profile) its values can't be shown to hold only characters "
                            + mysql.effectiveCollation() + " compares as Access does");
                } else if (stats != null && stats.unsafeKeyText() != null) {
                    column.binary(stats.unsafeKeyText() + " of its values hold characters " + mysql.effectiveCollation()
                            + " compares unlike Access (such as kana variants, no-break spaces, control characters or"
                            + " ligatures), so a key Access holds unique could collide there");
                }
            }
        }
        for (boolean changed = true; changed; ) {
            changed = false;
            for (ForeignKeyModel fk : model.relationships()) {
                if (fk.status() != ForeignKeyModel.Status.EMIT) {
                    continue;
                }
                TableDraft child = draft(fk.childTable());
                TableDraft parent = draft(fk.parentTable());
                if (child == null || parent == null) {
                    continue;
                }
                for (int i = 0; i < fk.childColumns().size(); i++) {
                    ColumnDraft c = child.column(fk.childColumns().get(i));
                    ColumnDraft p = parent.column(fk.parentColumns().get(i));
                    if (c == null
                            || p == null
                            || c.binary == p.binary
                            || !c.source.type().isText()) {
                        continue;
                    }
                    ColumnDraft follower = c.binary ? p : c;
                    ColumnDraft leader = c.binary ? c : p;
                    follower.binary("the foreign key " + fk.name() + " pairs it with " + leader.table.source.name()
                            + "." + leader.source.name()
                            + ", which is compared that way, and both sides need one collation");
                    changed = true;
                }
            }
        }
    }

    private TableDraft draft(String table) {
        return drafts.stream()
                .filter(d -> d.source.name().equalsIgnoreCase(table))
                .findFirst()
                .orElse(null);
    }

    // ---------------------------------------------------------------- relationships

    /** A foreign key for an enforced relationship, when the data, the parent's key and the types allow one. */
    private void foreignKey(ForeignKeyModel fk) {
        TableDraft child = draft(fk.childTable());
        TableDraft parent = draft(fk.parentTable());
        if (child == null || parent == null) {
            return; // linked or excluded: the extractor reported it
        }
        List<ColumnDraft> childColumns = child.columnsOf(fk.childColumns());
        List<ColumnDraft> parentColumns = parent.columnsOf(fk.parentColumns());
        if (childColumns == null || parentColumns == null) {
            rules.foreignKeySkipped(
                    fk, IssueCode.FK_SKIPPED_PARENT_KEY_MISSING, "one of its columns is not written to the output");
            return;
        }
        boolean binary = childColumns.stream().anyMatch(c -> c.binary);
        ForeignKeyData data = rules.foreignKeyData(fk, mysql.caseInsensitive() && !binary);
        if (data == ForeignKeyData.BLOCKED) {
            return;
        }
        if (data == ForeignKeyData.ASCII_CASE_ONLY && (!mysql.caseInsensitive() || binary)) {
            RelationshipProfile stats = rules.relationship(fk);
            issues.add(
                    IssueCode.FK_SKIPPED_ORPHANS,
                    fk.childTable(),
                    fk.name(),
                    "foreign key skipped: " + stats.inexact() + " child rows match their parent only when letter"
                            + " case is ignored, as Access does, and the collation "
                            + (binary ? BINARY_COLLATION : mysql.effectiveCollation()) + " compares case",
                    String.join("; ", stats.inexactSamples()));
            return;
        }
        PlannedIndex parentKey = parent.uniqueKeyOn(parentColumns);
        if (parentKey == null) {
            rules.foreignKeySkipped(
                    fk,
                    IssueCode.FK_SKIPPED_PARENT_KEY_MISSING,
                    "the parent key on " + String.join(", ", fk.parentColumns()) + " is not unique in the output (its"
                            + " key is too long for an InnoDB index), and MySQL needs a unique key to refer to");
            return;
        }
        // InnoDB matches the columns in the parent key's order
        List<ColumnDraft> orderedChild = new ArrayList<>();
        List<ColumnDraft> orderedParent = new ArrayList<>();
        for (KeyPart part : parentKey.parts()) {
            for (int i = 0; i < parentColumns.size(); i++) {
                if (parentColumns.get(i).name.equals(part.column())) {
                    orderedParent.add(parentColumns.get(i));
                    orderedChild.add(childColumns.get(i));
                }
            }
        }
        for (int i = 0; i < orderedChild.size(); i++) {
            ColumnDraft c = orderedChild.get(i);
            ColumnDraft p = orderedParent.get(i);
            if (!c.keyFamily().equals(p.keyFamily()) || c.keyFamily().equals(ColumnDraft.LOB)) {
                rules.foreignKeySkipped(
                        fk,
                        IssueCode.FK_SKIPPED_TYPE_MISMATCH,
                        c.name + " is " + c.type() + " and " + parent.name + "." + p.name + " is " + p.type()
                                + "; MySQL needs matching column types for a foreign key");
                return;
            }
        }
        Action onDelete = rules.onDelete(
                fk,
                orderedChild.stream()
                        .filter(c -> c.notNull)
                        .map(c -> c.name)
                        .findFirst()
                        .orElse(null));
        String wanted = fk.name().isBlank() ? "fk_" + child.name + "_" + parent.name : fk.name();
        String name =
                constraintNames.register(wanted, "foreign key " + fk.name(), "foreign key", issues, fk.childTable());
        List<String> childNames = orderedChild.stream().map(c -> c.name).toList();
        child.foreignKeys.add(new PlannedForeignKey(
                fk,
                name,
                childNames,
                parent.name,
                orderedParent.stream().map(c -> c.name).toList(),
                fk.onUpdate(),
                onDelete));
        child.foreignKeyColumns.addAll(childNames);
        String changes = changes(fk.onUpdate(), onDelete);
        if (changes != null) {
            for (String column : childNames) {
                child.changedByForeignKey.putIfAbsent(
                        column, "the foreign key " + name + " changes it (" + changes + ")");
            }
        }
        child.foreignKeyIndex(
                fk, orderedChild.stream().map(c -> c.source.name()).toList());
    }

    /** The referential actions that write to the child columns, which MySQL won't combine with a CHECK on them. */
    private static String changes(Action onUpdate, Action onDelete) {
        List<String> actions = new ArrayList<>();
        if (onUpdate == Action.CASCADE) {
            actions.add("ON UPDATE CASCADE");
        }
        if (onDelete == Action.SET_NULL) {
            actions.add("ON DELETE SET NULL");
        }
        return actions.isEmpty() ? null : String.join(", ", actions);
    }

    // ---------------------------------------------------------------- table

    /** One table being planned: its columns, keys, indexes, checks and foreign keys, and finally its statements. */
    private final class TableDraft {
        private final TableModel source;
        private final String name;
        private final List<ColumnDraft> columns = new ArrayList<>();
        private final IdentifierPolicy columnNames = new IdentifierPolicy(IdentifierPolicy.MYSQL);
        private final IdentifierPolicy indexNames = new IdentifierPolicy(IdentifierPolicy.MYSQL);
        private final List<PlannedIndex> inlineIndexes = new ArrayList<>();
        private final List<PlannedIndex> indexes = new ArrayList<>();
        private final List<PlannedCheck> checks = new ArrayList<>();
        private final List<PlannedForeignKey> foreignKeys = new ArrayList<>();
        private final List<String> tableComments = new ArrayList<>();
        /** Child columns of foreign keys: never widened, so both sides keep matching types. */
        private final Set<String> foreignKeyColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        /** Columns a foreign key's action writes to, and why: MySQL allows no CHECK on them. */
        private final Map<String, String> changedByForeignKey = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        private PlannedIndex primaryKey;
        private ColumnDraft autoIncrement;

        TableDraft(TableModel source) {
            this.source = source;
            this.name = tableNames.register(source.name(), "table " + source.name(), "table", issues, source.name());
            // MySQL keeps this name for the primary key
            indexNames.register("PRIMARY", "PRIMARY", "index", issues, source.name());
            Set<String> required = PlanRules.requiredColumns(source);
            Set<String> keyColumns = PlanRules.primaryKeyColumns(source);
            for (PlanRules.WrittenColumn written : rules.writtenColumns(source, true)) {
                ColumnModel column = written.column();
                columns.add(new ColumnDraft(
                        this,
                        column,
                        written.sourceIndex(),
                        written.olePart(),
                        required.contains(column.name()),
                        keyColumns.contains(column.name())));
            }
            for (ColumnDraft column : columns) {
                if (column.source.type() != AccessType.AUTONUMBER_LONG) {
                    continue;
                }
                if (column.source.isRandomAutoNumber()) {
                    randomAutoNumber(column.source);
                    continue;
                }
                if (autoIncrement == null) {
                    autoIncrement = column;
                    column.autoIncrement = true;
                } else {
                    issues.add(
                            IssueCode.AUTOINCREMENT_NOT_PRESERVED,
                            source.name(),
                            column.source.name(),
                            "the values are kept, but MySQL generates values for one column per table, and "
                                    + autoIncrement.name + " is that column");
                }
            }
            primaryKey();
            for (IndexModel index : source.indexes()) {
                index(index, index.name(), index.unique());
            }
            if (source.description() != null) {
                tableComments.add(source.description());
            }
        }

        /** The primary key exactly as Access has it, never one inferred from an autonumber (F-32). */
        private void primaryKey() {
            IndexModel pk = source.primaryKey();
            if (pk == null) {
                return;
            }
            if (columnsOf(pk.columnNames()) == null) {
                issues.add(
                        IssueCode.NO_PRIMARY_KEY,
                        source.name(),
                        pk.name(),
                        "primary key skipped: one of its columns is not written to the output");
                return;
            }
            PlannedIndex planned = key(pk, "PRIMARY", true, true);
            if (planned.primary()) {
                primaryKey = planned;
            } else {
                indexes.add(planned);
            }
        }

        /** Plans an index; returns whether it is written. */
        private boolean index(IndexModel index, String preferred, boolean unique) {
            if (columnsOf(index.columnNames()) == null || rules.skipsComplexIndex(source, index, true)) {
                return false;
            }
            indexes.add(key(
                    index,
                    indexNames.register(preferred, context(index), "index", issues, source.name()),
                    unique,
                    false));
            return true;
        }

        private String context(IndexModel index) {
            return "index " + source.name() + "." + index.name();
        }

        /**
         * An index within InnoDB's key limit: Long Text parts are the 255 characters Access indexes. A key that is
         * still too long loses its uniqueness, because a prefix would make it stricter than Access (05), and then
         * gets prefix lengths on its longest text parts until it fits.
         */
        private PlannedIndex key(IndexModel index, String indexName, boolean unique, boolean primary) {
            List<KeyPart> parts = new ArrayList<>();
            for (IndexColumn c : index.columns()) {
                ColumnDraft column = column(c.name());
                parts.add(new KeyPart(column.name, c.ascending(), column.lobKeyPrefix()));
            }
            long bytes = keyBytes(parts);
            if (bytes <= MAX_KEY_BYTES) {
                return new PlannedIndex(index, indexName, parts, primary, unique);
            }
            if (unique) {
                issues.add(
                        IssueCode.UNIQUE_DOWNGRADED_KEY_TOO_LONG,
                        source.name(),
                        index.name(),
                        (primary ? "the primary key" : "the unique index") + " is written as a plain index: its key"
                                + " takes up to " + bytes + " bytes, over InnoDB's " + MAX_KEY_BYTES + "-byte limit,"
                                + " and a prefix would make it reject values Access accepts");
            }
            String plainName = primary
                    ? indexNames.register(index.name(), context(index), "index", issues, source.name())
                    : indexName;
            return new PlannedIndex(index, plainName, fit(parts), false, false);
        }

        /**
         * Prefix lengths on the text and binary parts, sharing out what the fixed-size parts leave so the shortest
         * parts stay whole.
         */
        private List<KeyPart> fit(List<KeyPart> parts) {
            long budget = MAX_KEY_BYTES;
            List<Integer> shrinkable = new ArrayList<>();
            for (int i = 0; i < parts.size(); i++) {
                ColumnDraft column = byName(parts.get(i).column());
                if (column.shape == Shape.FIXED) {
                    budget -= partBytes(column, null);
                } else {
                    shrinkable.add(i);
                }
            }
            shrinkable.sort(Comparator.comparingLong(
                    i -> partBytes(byName(parts.get(i).column()), parts.get(i).prefix())));
            List<KeyPart> fitted = new ArrayList<>(parts);
            int remaining = shrinkable.size();
            for (int i : shrinkable) {
                KeyPart part = parts.get(i);
                ColumnDraft column = byName(part.column());
                long full = partBytes(column, part.prefix());
                long share = budget / remaining;
                if (full > share) {
                    int unit = column.bytesPerUnit();
                    int prefix = (int) Math.max(1, share / unit);
                    fitted.set(i, new KeyPart(part.column(), part.ascending(), prefix));
                    full = (long) prefix * unit;
                }
                budget -= full;
                remaining--;
            }
            return fitted;
        }

        private long keyBytes(List<KeyPart> parts) {
            return parts.stream()
                    .mapToLong(p -> partBytes(byName(p.column()), p.prefix()))
                    .sum();
        }

        /** Remembers the index D7 asks for on a relationship Access doesn't enforce, unless one already covers it. */
        void relationshipIndex(ForeignKeyModel fk) {
            List<ColumnDraft> childColumns = columnsOf(fk.childColumns());
            if (childColumns == null || PlanRules.covered(indexedColumns(false), names(childColumns))) {
                return;
            }
            if (index(PlanRules.relationshipIndex(fk), fk.name(), false)) {
                rules.indexAddedForRelationship(
                        fk, "Access creates none for a relationship without referential integrity");
            }
        }

        /** MySQL needs an index that starts with a foreign key's columns, whole and in its order. */
        void foreignKeyIndex(ForeignKeyModel fk, List<String> accessColumns) {
            List<ColumnDraft> childColumns = columnsOf(accessColumns);
            if (PlanRules.covered(indexedColumns(true), names(childColumns))) {
                return;
            }
            IndexModel index = new IndexModel(
                    fk.name(),
                    accessColumns.stream().map(c -> new IndexColumn(c, true)).toList(),
                    false,
                    false,
                    false,
                    false,
                    IndexModel.Origin.RELATIONSHIP,
                    List.of(fk.name()));
            if (index(index, fk.name(), false)) {
                rules.indexAddedForRelationship(
                        fk, "MySQL needs an index that starts with a foreign key's columns, and no kept index does");
            }
        }

        /**
         * The column lists of every index so far.
         *
         * @param wholeOnly stop each list at its first prefix part, which a foreign key can't use
         */
        private List<List<String>> indexedColumns(boolean wholeOnly) {
            List<List<String>> lists = new ArrayList<>();
            for (PlannedIndex index : all()) {
                List<String> names = new ArrayList<>();
                for (KeyPart part : index.parts()) {
                    if (wholeOnly && part.prefix() != null) {
                        break;
                    }
                    names.add(part.column());
                }
                lists.add(names);
            }
            return lists;
        }

        private List<PlannedIndex> all() {
            List<PlannedIndex> all = new ArrayList<>();
            if (primaryKey != null) {
                all.add(primaryKey);
            }
            all.addAll(inlineIndexes);
            all.addAll(indexes);
            return all;
        }

        /** The primary key or a unique index on exactly these columns (in any order), whole; or null. */
        PlannedIndex uniqueKeyOn(List<ColumnDraft> keyColumns) {
            Set<String> wanted = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            wanted.addAll(names(keyColumns));
            for (PlannedIndex index : all()) {
                if (!index.unique() || index.parts().size() != wanted.size()) {
                    continue;
                }
                boolean matches =
                        index.parts().stream().allMatch(p -> p.prefix() == null && wanted.contains(p.column()));
                if (matches) {
                    return index;
                }
            }
            return null;
        }

        /**
         * InnoDB can only generate values for a column that starts an index when the table is created, so the key
         * for an autonumber that isn't the primary key goes into {@code CREATE TABLE} (F-35).
         */
        void autoIncrementKey() {
            if (autoIncrement == null) {
                return;
            }
            if (primaryKey != null && startsWith(primaryKey, autoIncrement)) {
                return;
            }
            for (PlannedIndex index : indexes) {
                if (startsWith(index, autoIncrement)) {
                    indexes.remove(index);
                    inlineIndexes.add(index);
                    return;
                }
            }
            String accessName = autoIncrement.source.name();
            IndexModel added = new IndexModel(
                    accessName,
                    List.of(new IndexColumn(accessName, true)),
                    false,
                    false,
                    false,
                    false,
                    IndexModel.Origin.USER,
                    List.of());
            String indexName = indexNames.register(
                    accessName, "auto-increment " + source.name() + "." + accessName, "index", issues, source.name());
            inlineIndexes.add(new PlannedIndex(
                    added, indexName, List.of(new KeyPart(autoIncrement.name, true, null)), false, false));
            issues.add(
                    IssueCode.INDEX_ADDED_FOR_AUTO_INCREMENT,
                    source.name(),
                    accessName,
                    "plain index on " + accessName + ": MySQL only generates values for an AUTO_INCREMENT column that"
                            + " starts an index, and this autonumber isn't the primary key");
        }

        private static boolean startsWith(PlannedIndex index, ColumnDraft column) {
            KeyPart first = index.parts().get(0);
            return first.column().equals(column.name) && first.prefix() == null;
        }

        /**
         * MySQL rejects a table whose rows could take more than 65,535 bytes (F-18). The largest VARCHARs that no
         * index and no foreign key uses become TEXT, which holds the same values in 10 bytes of the row, until it
         * fits.
         */
        void rowSize() {
            long total = rowBytes();
            if (total > MAX_ROW_BYTES) {
                widen(
                        total,
                        MAX_ROW_BYTES,
                        ColumnDraft::rowBytes,
                        Integer.MAX_VALUE,
                        this::rowBytes,
                        needed -> "the"
                                + " table's rows could take up to " + needed + " bytes, over MySQL's " + MAX_ROW_BYTES
                                + "-byte row limit");
            }
            long inline = inlineBytes();
            if (inline > MAX_INLINE_BYTES) {
                // Only a VARCHAR of at most 255 bytes counts whole; a longer one already counts as a TEXT does
                widen(
                        inline,
                        MAX_INLINE_BYTES,
                        ColumnDraft::inlineBytes,
                        SHORT_VARCHAR_BYTES / BYTES_PER_CHAR,
                        this::inlineBytes,
                        needed -> "MySQL 8.0 and MariaDB reject a table whose rows could keep more than "
                                + MAX_INLINE_BYTES + " bytes inline (InnoDB's limit for a 16 KB page), and this"
                                + " table's could keep " + needed);
                if (inlineBytes() > MAX_INLINE_BYTES) {
                    issues.add(
                            IssueCode.TABLE_ROW_TOO_LARGE,
                            source.name(),
                            null,
                            "the table's rows could keep " + inlineBytes() + " bytes inline even with every VARCHAR it"
                                    + " can spare as TEXT, over the " + MAX_INLINE_BYTES + " bytes "
                                    + (mysql.dialect() == MySqlDialect.MARIADB ? "MariaDB" : "MySQL 8.0")
                                    + " accepts when it creates a table (ERROR 1118)"
                                    + (mysql.dialect() == MySqlDialect.MYSQL ? "; MySQL 8.4 creates it" : ""));
                }
            }
            if (rowBytes() > MAX_ROW_BYTES) {
                issues.add(
                        IssueCode.TABLE_ROW_TOO_LARGE,
                        source.name(),
                        null,
                        "the table's rows could take " + rowBytes() + " bytes even with every VARCHAR it can spare as"
                                + " TEXT, over MySQL's " + MAX_ROW_BYTES + "-byte limit (ERROR 1118)");
            }
        }

        /**
         * Turns the largest VARCHARs that no index and no foreign key uses into TEXT until {@code size} is within
         * {@code limit}.
         *
         * @param longest only VARCHARs of at most this many characters save anything
         */
        private void widen(
                long needed,
                long limit,
                java.util.function.ToLongFunction<ColumnDraft> bytes,
                int longest,
                java.util.function.LongSupplier size,
                java.util.function.LongFunction<String> why) {
            Set<String> indexed = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            all().forEach(i -> indexed.addAll(i.columnNames()));
            List<ColumnDraft> candidates = columns.stream()
                    .filter(c -> c.shape == Shape.VARCHAR && c.length <= longest)
                    .filter(c -> !indexed.contains(c.name) && !foreignKeyColumns.contains(c.name))
                    .sorted(Comparator.comparingInt((ColumnDraft c) -> c.length)
                            .reversed()
                            .thenComparingInt(c -> c.sourceIndex))
                    .toList();
            for (ColumnDraft column : candidates) {
                String before = column.type();
                long saved = bytes.applyAsLong(column);
                column.shape = Shape.TEXT;
                if (saved <= bytes.applyAsLong(column)) {
                    column.shape = Shape.VARCHAR;
                    continue;
                }
                issues.add(
                        IssueCode.TEXT_WIDENED_ROW_SIZE,
                        source.name(),
                        column.source.name(),
                        before + " is written as TEXT, which holds the same values: " + why.apply(needed));
                if (size.getAsLong() <= limit) {
                    return;
                }
            }
        }

        private long rowBytes() {
            long nullable = columns.stream().filter(c -> !c.notNull).count();
            return columns.stream().mapToLong(ColumnDraft::rowBytes).sum() + (nullable + 7) / 8;
        }

        /**
         * The most bytes a row keeps in its InnoDB page (ROW_FORMAT=DYNAMIC), as MySQL 8.0 and MariaDB count it when they create the
         * table: a 5-byte header, the NULL bitmap, the transaction and rollback fields (and a row id without a
         * primary key), and every column; columns longer than 255 bytes can move off the page and count 21 (MariaDB) or 41 (MySQL 8.0). Measured
         * to the byte on MySQL 8.0 and MariaDB 10.11 and 11.8; MySQL 8.4 doesn't check it.
         */
        private long inlineBytes() {
            long nullable = columns.stream().filter(c -> !c.notNull).count();
            return 5
                    + (nullable + 7) / 8
                    + (primaryKey != null ? 13 : 19)
                    + columns.stream().mapToLong(ColumnDraft::inlineBytes).sum();
        }

        /** CHECK constraints: column rules, table rules and AllowZeroLength, only where the data complies. */
        void checks() {
            for (ColumnDraft column : columns) {
                CheckRule rule = column.source.validation();
                if (rule != null) {
                    if (rule.isTranslated() && rules.ruleHolds(source, rule, column.source.name())) {
                        check(rule, column);
                    } else {
                        column.comments.add("Access validation rule: " + rule.raw());
                    }
                }
                if (rules.noEmptyStrings(source, column.source)) {
                    String conflict = conflict(Set.of(column.source.name()));
                    if (conflict == null) {
                        // CHAR_LENGTH, not <> '': a PAD SPACE collation (MariaDB's) finds ' ' equal to ''
                        addCheck(
                                "chk_" + source.name() + "_" + column.source.name() + "_nonempty",
                                "CHAR_LENGTH(" + identifier(column.name) + ") > 0",
                                column.source.name());
                    } else {
                        issues.add(
                                IssueCode.CHECK_UNTRANSLATABLE,
                                source.name(),
                                column.source.name(),
                                "AllowZeroLength = No has no " + mysql.dialect().displayName() + " CHECK: " + conflict);
                    }
                }
            }
            CheckRule rule = source.validation();
            if (rule == null) {
                return;
            }
            if (rule.isTranslated() && rules.ruleHolds(source, rule, null)) {
                check(rule, null);
            } else {
                tableComments.add("Access validation rule: " + rule.raw());
            }
        }

        private void check(CheckRule rule, ColumnDraft column) {
            String accessColumn = column == null ? null : column.source.name();
            String conflict = conflict(ExprColumns.referenced(rule.expr()));
            Rendered rendered = conflict != null
                    ? Rendered.unsupported(conflict)
                    : MySqlExpressions.check(
                            rule.expr(),
                            name -> Optional.ofNullable(column(name)).map(ColumnDraft::shape),
                            mysql.caseInsensitive() ? null : mysql.dialect().defaultCollation(),
                            mysql.dialect().defaultCollation());
            if (rendered.isPresent()) {
                addCheck(
                        "chk_" + source.name() + (column == null ? "" : "_" + accessColumn),
                        rendered.sql(),
                        accessColumn);
                return;
            }
            rules.checkUntranslatable(source, accessColumn, rule, rendered.problem());
            if (column == null) {
                tableComments.add("Access validation rule: " + rule.raw());
            } else {
                column.comments.add("Access validation rule: " + rule.raw());
            }
        }

        /** Why MySQL won't accept a CHECK on these columns, or null when it will. */
        private String conflict(Set<String> referenced) {
            for (String accessName : new TreeSet<>(referenced)) {
                ColumnDraft column = column(accessName);
                if (column == null) {
                    continue;
                }
                if (column.autoIncrement) {
                    return "MySQL allows no CHECK on " + column.name + ", an AUTO_INCREMENT column";
                }
                String changed = changedByForeignKey.get(column.name);
                if (changed != null) {
                    return "MySQL allows no CHECK on " + column.name + ": " + changed;
                }
            }
            return null;
        }

        private void addCheck(String preferred, String sql, String column) {
            String checkName = constraintNames.register(
                    preferred, "check " + source.name() + "." + preferred, "check constraint", issues, source.name());
            checks.add(new PlannedCheck(checkName, sql, column));
        }

        List<ColumnDraft> columnsOf(List<String> accessNames) {
            List<ColumnDraft> found = new ArrayList<>(accessNames.size());
            for (String accessName : accessNames) {
                ColumnDraft column = column(accessName);
                if (column == null) {
                    return null;
                }
                found.add(column);
            }
            return found;
        }

        /** A column by its Access name, or null when it isn't written. */
        ColumnDraft column(String accessName) {
            return columns.stream()
                    .filter(c -> c.source.name().equalsIgnoreCase(accessName))
                    .findFirst()
                    .orElse(null);
        }

        /** A column by its output name. */
        private ColumnDraft byName(String outputName) {
            return columns.stream()
                    .filter(c -> c.name.equals(outputName))
                    .findFirst()
                    .orElseThrow();
        }

        PlannedTable freeze() {
            List<PlannedColumn> planned =
                    columns.stream().map(ColumnDraft::freeze).toList();
            String comment = tableComments.isEmpty()
                    ? null
                    : comment(String.join("; ", tableComments), MAX_TABLE_COMMENT, source.name(), null);
            Long seed = seed();
            return new PlannedTable(
                    source,
                    name,
                    planned,
                    primaryKey,
                    inlineIndexes,
                    indexes,
                    checks,
                    foreignKeys,
                    comment,
                    seed,
                    createTable(planned, comment, seed),
                    indexes.isEmpty()
                            ? null
                            : alter(indexes.stream()
                                    .map(i -> "ADD " + indexClause(i))
                                    .toList()),
                    checks.isEmpty()
                            ? null
                            : alter(checks.stream()
                                    .map(c -> "ADD CONSTRAINT " + identifier(c.name()) + " CHECK (" + c.sql() + ")")
                                    .toList()),
                    foreignKeys.isEmpty() ? null : foreignKeys(name, foreignKeys));
        }

        /**
         * A Random autonumber gets a random DEFAULT, not AUTO_INCREMENT (maintainer decision, phase 6): Access draws a
         * random 32-bit value with no ceiling, and a collision is a duplicate-key error there too. AUTO_INCREMENT would
         * continue after the largest stored value, and random values scatter up to INT's maximum, so a counter would
         * run out after about 2^32 / rows inserts, or at once (the tier D fixture holds 2147483647). The price is
         * that the server doesn't return the generated key, which the report says plainly.
         */
        private void randomAutoNumber(ColumnModel column) {
            Long rows = rules.rows(source.name());
            String collisions;
            if (rows == null) {
                collisions = "; without a profile the row count, and so how often that happens, is unknown";
            } else if (rows == 0) {
                collisions = "; the table is empty, so that grows from zero as rows are added";
            } else {
                collisions = String.format(
                        Locale.ROOT,
                        "; with the table's %,d rows, about one insert in %,d draws a value that is taken",
                        rows,
                        Math.max(1, Math.round(4_294_967_296d / rows)));
            }
            issues.add(
                    IssueCode.AUTONUMBER_RANDOM_DEFAULT,
                    source.name(),
                    column.name(),
                    "Access generates random values for this autonumber (New Values: Random), and so does the column's"
                            + " DEFAULT: a random INT, with no ceiling. The server doesn't return the generated key:"
                            + " LAST_INSERT_ID() and the client calls built on it give 0, so an application that reads"
                            + " the new key after an insert must select the row instead. A value that is already taken"
                            + " fails the insert with a duplicate-key error, as in Access" + collisions
                            + ". The existing values are kept exactly");
        }

        /** The {@code AUTO_INCREMENT} table option: the next value after the highest Access used (04). */
        private Long seed() {
            if (autoIncrement == null) {
                return null;
            }
            ColumnStats stats = rules.stats(source.name(), autoIncrement.source.name());
            Long max = stats == null ? null : stats.maxAutoNumber();
            if (max == null || max < 1 || max >= Integer.MAX_VALUE) {
                return null;
            }
            return max + 1;
        }

        private String createTable(List<PlannedColumn> planned, String comment, Long seed) {
            List<String> items = new ArrayList<>();
            for (PlannedColumn column : planned) {
                items.add(columnDefinition(column));
            }
            if (primaryKey != null) {
                items.add(indexClause(primaryKey));
            }
            for (PlannedIndex index : inlineIndexes) {
                items.add(indexClause(index));
            }
            StringBuilder sql =
                    new StringBuilder("CREATE TABLE ").append(identifier(name)).append(" (\n");
            for (int i = 0; i < items.size(); i++) {
                sql.append(INDENT).append(items.get(i)).append(i < items.size() - 1 ? ",\n" : "\n");
            }
            sql.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=").append(mysql.effectiveCollation());
            if (seed != null) {
                sql.append(" AUTO_INCREMENT=").append(seed);
            }
            if (comment != null) {
                sql.append(" COMMENT=").append(MySqlLiterals.string(comment));
            }
            return sql.toString();
        }

        private String alter(List<String> clauses) {
            return "ALTER TABLE " + identifier(name) + "\n" + INDENT + String.join(",\n" + INDENT, clauses);
        }
    }

    private static String columnDefinition(PlannedColumn column) {
        StringBuilder sql =
                new StringBuilder(identifier(column.name())).append(' ').append(column.type());
        if (column.collation() != null) {
            sql.append(" COLLATE ").append(column.collation());
        }
        if (column.notNull()) {
            sql.append(" NOT NULL");
        }
        if (column.autoIncrement()) {
            sql.append(" AUTO_INCREMENT");
        }
        if (column.defaultSql() != null) {
            sql.append(" DEFAULT ").append(column.defaultSql());
        }
        if (column.comment() != null) {
            sql.append(" COMMENT ").append(MySqlLiterals.string(column.comment()));
        }
        return sql.toString();
    }

    static String indexClause(PlannedIndex index) {
        String parts = index.parts().stream()
                .map(p -> identifier(p.column())
                        + (p.prefix() == null ? "" : "(" + p.prefix() + ")")
                        + (p.ascending() ? "" : " DESC"))
                .collect(Collectors.joining(", "));
        if (index.primary()) {
            return "PRIMARY KEY (" + parts + ")";
        }
        return (index.unique() ? "UNIQUE KEY " : "KEY ") + identifier(index.name()) + " (" + parts + ")";
    }

    /** The section 5 statement for these foreign keys of a table. */
    static String foreignKeys(String table, List<PlannedForeignKey> foreignKeys) {
        return "ALTER TABLE " + identifier(table) + "\n" + INDENT
                + foreignKeys.stream()
                        .map(fk -> "ADD CONSTRAINT " + identifier(fk.name()) + " " + foreignKeyClause(fk))
                        .collect(Collectors.joining(",\n" + INDENT));
    }

    private static String foreignKeyClause(PlannedForeignKey fk) {
        StringBuilder sql = new StringBuilder("FOREIGN KEY (")
                .append(fk.childColumns().stream()
                        .map(MySqlLiterals::identifier)
                        .collect(Collectors.joining(", ")))
                .append(") REFERENCES ")
                .append(identifier(fk.parentTable()))
                .append(" (")
                .append(fk.parentColumns().stream()
                        .map(MySqlLiterals::identifier)
                        .collect(Collectors.joining(", ")))
                .append(")");
        switch (fk.onDelete()) {
            case CASCADE -> sql.append(" ON DELETE CASCADE");
            case SET_NULL -> sql.append(" ON DELETE SET NULL");
            case NO_ACTION -> {} // InnoDB's default, and what Access does without a cascade rule (F-33)
        }
        if (fk.onUpdate() == Action.CASCADE) {
            sql.append(" ON UPDATE CASCADE");
        }
        return sql.toString();
    }

    /**
     * A comment MySQL stores as it is: comments live in the data dictionary as utf8mb3, so a character outside the
     * Basic Multilingual Plane (an emoji) would become {@code ?} (MySQL warns, MariaDB doesn't), and they are capped at
     * 1024 characters for a column and 2048 for a table (05). Both are reported.
     */
    private String comment(String text, int max, String table, String column) {
        StringBuilder bmp = new StringBuilder(text.length());
        boolean replaced = false;
        for (int i = 0; i < text.length(); ) {
            int c = text.codePointAt(i);
            if (c > 0xFFFF || Character.isSurrogate((char) c)) {
                bmp.append(MySqlLiterals.REPLACEMENT);
                replaced = true;
            } else {
                bmp.append((char) c);
            }
            i += Character.charCount(c);
        }
        String result = bmp.toString();
        if (replaced) {
            issues.add(
                    IssueCode.COMMENT_TRUNCATED,
                    table,
                    column,
                    "characters outside the Basic Multilingual Plane are written as U+FFFD in the comment: MySQL keeps"
                            + " comments in utf8mb3");
        }
        if (result.length() > max) {
            result = result.substring(0, max - 1) + "…";
            issues.add(
                    IssueCode.COMMENT_TRUNCATED,
                    table,
                    column,
                    "the comment is cut to " + max + " characters, the most MySQL keeps; the Access text is longer");
        }
        return result;
    }

    // ---------------------------------------------------------------- column

    /** How a column is stored, which decides its type, its key and row sizes and its default. */
    private enum Shape {
        /** Numbers and dates. */
        FIXED,
        VARCHAR,
        /** A GUID: {@code CHAR(38)}. */
        CHAR,
        /** A VARCHAR widened by the row-size rule. */
        TEXT,
        LONGTEXT,
        VARBINARY,
        LONGBLOB
    }

    /** One column being planned. */
    private final class ColumnDraft {
        /** The key family of Long Text and OLE columns, which can't be foreign keys. */
        static final String LOB = "LOB";

        private final TableDraft table;
        private final ColumnModel source;
        private final int sourceIndex;
        private final PlanRules.OlePart olePart;
        /** How a Binary, OLE or attachment value is stored (08): the bytes, a file's path or the byte count. */
        private final BinaryCells.Storage storage;

        private final String name;
        private final ValueForm form;
        private final int fractionDigits;
        /** Currency and Decimal: {precision, scale}. */
        private final int[] decimal;

        private final boolean notNull;
        private final List<String> comments = new ArrayList<>();
        private Shape shape;
        /** VARCHAR characters or VARBINARY bytes. */
        private int length;

        private boolean autoIncrement;
        /** Compared with {@code utf8mb4_bin} rather than the table's collation. */
        private boolean binary;

        ColumnDraft(
                TableDraft table,
                ColumnModel source,
                int sourceIndex,
                PlanRules.OlePart olePart,
                boolean required,
                boolean inPrimaryKey) {
            this.table = table;
            this.source = source;
            this.sourceIndex = sourceIndex;
            this.olePart = olePart;
            this.storage = BinaryCells.storage(source, options);
            this.name = table.columnNames.register(
                    source.name(),
                    "column " + table.source.name() + "." + source.name(),
                    "column",
                    issues,
                    table.source.name());
            this.form = switch (storage) {
                case PATH -> ValueForm.PATH;
                case SIZE -> ValueForm.SIZE;
                case VALUE -> form(source.type());
            };
            this.fractionDigits = fractionDigits();
            this.decimal = source.type().isExactNumeric() ? decimalType() : null;
            this.notNull = rules.notNull(table.source, source, required, inPrimaryKey);
            switch (source.type()) {
                case TEXT -> {
                    shape = Shape.VARCHAR;
                    length = source.length() == null ? 255 : source.length();
                }
                case MEMO, HYPERLINK -> shape = Shape.LONGTEXT;
                case GUID, AUTONUMBER_GUID -> shape = Shape.CHAR;
                // A Binary without a length (an attachment's data, an OLE value's content) holds bytes of any size
                case BINARY -> shape = source.length() == null ? Shape.LONGBLOB : Shape.VARBINARY;
                case OLE, UNSUPPORTED -> shape = Shape.LONGBLOB;
                default -> shape = Shape.FIXED;
            }
            if (source.type() == AccessType.BINARY) {
                length = source.length() == null ? 0 : source.length();
            }
            switch (storage) {
                case PATH -> {
                    shape = Shape.VARCHAR;
                    length = BinaryCells.PATH_LENGTH;
                }
                case SIZE -> shape = Shape.FIXED;
                case VALUE -> {}
            }
            if (source.isCalculated()) {
                comments.add("Access expression: " + source.calculatedExpression());
            }
            if (source.description() != null) {
                comments.add(source.description());
            }
        }

        /**
         * Date/Time keeps fractional seconds only when the profile finds some ({@code DATETIME(3)} or more);
         * Date/Time Extended is always {@code DATETIME(6)}, a microsecond, the finest MySQL has.
         */
        private int fractionDigits() {
            if (source.type() == AccessType.EXT_DATE_TIME) {
                return 6;
            }
            if (source.type() != AccessType.SHORT_DATE_TIME) {
                return 0;
            }
            ColumnStats stats = rules.stats(table.source.name(), source.name());
            Integer used = stats == null ? null : stats.maxFractionDigits();
            if (used == null) {
                return 3;
            }
            return used == 0 ? 0 : Math.min(6, Math.max(3, used));
        }

        String type() {
            return switch (shape) {
                case VARCHAR -> "VARCHAR(" + length + ")";
                case CHAR -> "CHAR(38)";
                case TEXT -> "TEXT";
                case LONGTEXT -> "LONGTEXT";
                case VARBINARY -> "VARBINARY(" + length + ")";
                case LONGBLOB -> "LONGBLOB";
                case FIXED -> fixedType();
            };
        }

        private String fixedType() {
            if (storage == BinaryCells.Storage.SIZE) {
                return "BIGINT";
            }
            return switch (source.type()) {
                // BOOLEAN is TINYINT(1) without MySQL's display-width deprecation warning 1681
                case BOOLEAN -> "BOOLEAN";
                case BYTE -> "TINYINT UNSIGNED";
                case INT -> "SMALLINT";
                case LONG, AUTONUMBER_LONG, ATTACHMENT, MULTI_VALUE, VERSION_HISTORY, COMPLEX_UNSUPPORTED -> "INT";
                case BIG_INT -> "BIGINT";
                case FLOAT -> "FLOAT";
                case DOUBLE -> "DOUBLE";
                case MONEY -> "DECIMAL(19,4)";
                case NUMERIC -> "DECIMAL(" + precision() + "," + scale() + ")";
                case SHORT_DATE_TIME, EXT_DATE_TIME ->
                    fractionDigits == 0 ? "DATETIME" : "DATETIME(" + fractionDigits + ")";
                default -> throw new IllegalStateException("not a fixed-size type: " + source.type());
            };
        }

        private int precision() {
            return decimal[0];
        }

        private int scale() {
            return decimal[1];
        }

        /**
         * Precision and scale of a Currency or Decimal. A calculated Decimal's declared scale says nothing about its
         * stored values ({@code calcFieldV2010}'s {@code [Salary]/52} is declared (28,0) and holds 16 fractional
         * digits), so the scale grows to what the profile found, or to Access's maximum of 28 without a profile; the
         * server would otherwise round the values (note 1265).
         */
        private int[] decimalType() {
            if (source.type() == AccessType.MONEY) {
                return new int[] {19, 4};
            }
            int precision = source.precision() == null ? 18 : source.precision();
            int scale = source.scale() == null ? 0 : source.scale();
            ColumnStats stats = rules.stats(table.source.name(), source.name());
            Integer used = stats == null ? null : stats.maxScale();
            int needed = used != null ? used : !rules.profiled() && source.isCalculated() ? 28 : scale;
            if (needed <= scale) {
                return new int[] {precision, scale};
            }
            int widenedScale = Math.min(30, needed);
            int widenedPrecision = Math.min(65, precision - scale + widenedScale);
            issues.add(
                    IssueCode.DECIMAL_SCALE_WIDENED,
                    table.source.name(),
                    source.name(),
                    "DECIMAL(" + precision + "," + scale + ") is written as DECIMAL(" + widenedPrecision + ","
                            + widenedScale + "): "
                            + (used != null
                                    ? "the stored values have up to " + used + " fractional digits"
                                    : "without profiling (--no-profile) a calculated column's values could have up"
                                            + " to 28 fractional digits")
                            + ", and a smaller scale would round them");
            return new int[] {widenedPrecision, widenedScale};
        }

        /** The prefix a key part on this column always has: Long Text and OLE can't be indexed whole. */
        Integer lobKeyPrefix() {
            return shape == Shape.LONGTEXT || shape == Shape.TEXT || shape == Shape.LONGBLOB
                    ? LONG_TEXT_KEY_PREFIX
                    : null;
        }

        int bytesPerUnit() {
            return shape == Shape.VARBINARY || shape == Shape.LONGBLOB ? 1 : BYTES_PER_CHAR;
        }

        /** Columns whose types a foreign key may pair: the same family, lengths aside. */
        String keyFamily() {
            return switch (shape) {
                case VARCHAR, CHAR -> "STRING";
                case VARBINARY -> "BINARY";
                case TEXT, LONGTEXT, LONGBLOB -> LOB;
                case FIXED -> fixedType();
            };
        }

        /** The most bytes the column takes in a row, as MySQL counts toward its 65,535-byte limit (measured). */
        long rowBytes() {
            return switch (shape) {
                case VARCHAR -> BYTES_PER_CHAR * (long) length + (BYTES_PER_CHAR * length > 255 ? 2 : 1);
                case CHAR -> BYTES_PER_CHAR * 38L;
                case TEXT -> 10;
                case LONGTEXT, LONGBLOB -> 12;
                case VARBINARY -> length + (length > 255 ? 2 : 1);
                case FIXED -> fixedBytes();
            };
        }

        /** The most bytes the column keeps inline in an InnoDB page, as MySQL 8.0 and MariaDB count it (measured). */
        long inlineBytes() {
            return switch (shape) {
                case VARCHAR -> variableInline(BYTES_PER_CHAR * (long) length);
                case CHAR -> variableInline(BYTES_PER_CHAR * 38L); // utf8mb4 CHAR is variable-length in InnoDB
                case VARBINARY -> variableInline(length);
                case TEXT, LONGTEXT, LONGBLOB -> offPageBytes(mysql.dialect());
                case FIXED -> fixedBytes();
            };
        }

        private long variableInline(long maxBytes) {
            return maxBytes > SHORT_VARCHAR_BYTES ? offPageBytes(mysql.dialect()) : maxBytes + 1;
        }

        long fixedBytes() {
            if (storage == BinaryCells.Storage.SIZE) {
                return 8;
            }
            return switch (source.type()) {
                case BOOLEAN, BYTE -> 1;
                case INT -> 2;
                case LONG, AUTONUMBER_LONG, ATTACHMENT, MULTI_VALUE, VERSION_HISTORY, COMPLEX_UNSUPPORTED, FLOAT -> 4;
                case BIG_INT, DOUBLE -> 8;
                case MONEY, NUMERIC -> decimalBytes(precision(), scale());
                case SHORT_DATE_TIME, EXT_DATE_TIME -> 5 + (fractionDigits + 1) / 2;
                default -> throw new IllegalStateException("not a fixed-size type: " + source.type());
            };
        }

        void binary(String why) {
            if (binary) {
                return;
            }
            binary = true;
            issues.add(
                    IssueCode.COLLATION_BINARY_KEY,
                    table.source.name(),
                    source.name(),
                    "compared with " + BINARY_COLLATION + ", so this column matches case-sensitively, unlike Access and"
                            + " the rest of the table: " + why);
        }

        MySqlExpressions.Column shape() {
            return new MySqlExpressions.Column(
                    name,
                    source.type(),
                    type(),
                    shape == Shape.VARCHAR ? Integer.valueOf(length) : shape == Shape.CHAR ? Integer.valueOf(38) : null,
                    shape == Shape.TEXT || shape == Shape.LONGTEXT || shape == Shape.LONGBLOB,
                    fractionDigits,
                    notNull,
                    shape == Shape.FIXED && source.type().isExactNumeric() ? precision() : null,
                    shape == Shape.FIXED && source.type().isExactNumeric() ? scale() : null,
                    binary);
        }

        /** The {@code DEFAULT} value: only from Access's own default (F-08), and only where it fits the column. */
        private String defaultSql() {
            DefaultValue value = source.defaultValue();
            if (autoIncrement) {
                return null; // MySQL allows no default on an AUTO_INCREMENT column; Access has none there either
            }
            if (source.type() == AccessType.AUTONUMBER_GUID) {
                return MySqlExpressions.RANDOM_GUID; // Access generates a Replication ID itself; so can MySQL
            }
            if (source.isRandomAutoNumber()) {
                return MySqlExpressions.RANDOM_INT; // GenUniqueID(): the autonumber's Random setting
            }
            if (value == null) {
                return source.type() == AccessType.BOOLEAN ? "0" : null; // an Access Yes/No without a default is No
            }
            if (!value.isTranslated()) {
                // Reported by the extractor
                comments.add("Access default: " + value.raw());
                return null;
            }
            Rendered rendered = storage == BinaryCells.Storage.VALUE
                    ? MySqlExpressions.defaultClause(value.expr(), shape())
                    : Rendered.mismatch("the column holds "
                            + (storage == BinaryCells.Storage.PATH ? "a file's path" : "a byte count")
                            + " (--binary " + options.binary().label() + "), not the value");
            if (rendered.isPresent() || rendered.problem() == null) {
                return rendered.sql();
            }
            issues.add(
                    rendered.typeMismatch()
                            ? IssueCode.DEFAULT_DROPPED_TYPE_MISMATCH
                            : IssueCode.DEFAULT_UNTRANSLATABLE,
                    table.source.name(),
                    source.name(),
                    "the default " + value.raw() + " is not written: " + rendered.problem());
            comments.add("Access default: " + value.raw());
            return null;
        }

        PlannedColumn freeze() {
            String defaultSql = defaultSql();
            String comment = comments.isEmpty()
                    ? null
                    : comment(String.join("; ", comments), MAX_COLUMN_COMMENT, table.source.name(), source.name());
            return new PlannedColumn(
                    source,
                    sourceIndex,
                    name,
                    type(),
                    binary ? BINARY_COLLATION : null,
                    form,
                    notNull,
                    autoIncrement,
                    defaultSql,
                    comment,
                    fractionDigits,
                    olePart);
        }
    }

    private static List<String> names(List<ColumnDraft> columns) {
        return columns.stream().map(c -> c.name).toList();
    }

    /** Bytes a key part takes: characters × 4 for text, bytes for binary, the storage size otherwise. */
    private static long partBytes(ColumnDraft column, Integer prefix) {
        return switch (column.shape) {
            case VARCHAR -> (long) BYTES_PER_CHAR * (prefix != null ? prefix : column.length);
            case CHAR -> (long) BYTES_PER_CHAR * (prefix != null ? prefix : 38);
            case TEXT, LONGTEXT -> (long) BYTES_PER_CHAR * prefix;
            case VARBINARY -> prefix != null ? prefix : column.length;
            case LONGBLOB -> prefix;
            case FIXED -> column.fixedBytes();
        };
    }

    /** A {@code DECIMAL(p,s)}'s storage size: 4 bytes per 9 digits on each side of the point, fewer for the rest. */
    static int decimalBytes(int precision, int scale) {
        int[] partial = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
        int whole = precision - scale;
        return whole / 9 * 4 + partial[whole % 9] + scale / 9 * 4 + partial[scale % 9];
    }

    static ValueForm form(AccessType type) {
        return switch (type) {
            case BOOLEAN -> ValueForm.BOOLEAN;
            case BYTE, INT, LONG, AUTONUMBER_LONG, BIG_INT -> ValueForm.INTEGER;
            case MONEY, NUMERIC -> ValueForm.DECIMAL;
            case FLOAT -> ValueForm.FLOAT;
            case DOUBLE -> ValueForm.DOUBLE;
            case SHORT_DATE_TIME, EXT_DATE_TIME -> ValueForm.DATETIME;
            case TEXT, MEMO, HYPERLINK, GUID, AUTONUMBER_GUID -> ValueForm.TEXT;
            case BINARY, OLE, UNSUPPORTED -> ValueForm.BYTES;
            case ATTACHMENT, MULTI_VALUE, VERSION_HISTORY, COMPLEX_UNSUPPORTED -> ValueForm.COMPLEX_ID;
        };
    }
}
