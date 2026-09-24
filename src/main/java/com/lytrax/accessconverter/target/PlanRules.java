package com.lytrax.accessconverter.target;

import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.model.CheckRule;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.IndexModel.IndexColumn;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.profile.DataProfile;
import com.lytrax.accessconverter.profile.DataProfile.ColumnStats;
import com.lytrax.accessconverter.profile.DataProfile.RelationshipProfile;
import com.lytrax.accessconverter.profile.DataProfile.RuleStats;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

/**
 * The planning decisions every target makes the same way (05, 06): which columns are written, where the data allows
 * NOT NULL and CHECK, which relationships can become foreign keys, and the index D7 asks for. A target asks; what it
 * can't express is its own business.
 *
 * <p>Every decision that isn't an exact copy of Access raises its issue here, naming the target in the message.
 */
public final class PlanRules {

    private final DataProfile profile;
    private final ConvertOptions options;
    private final Issues issues;
    private final String target;

    /**
     * @param profile null when {@code --no-profile} skipped it
     * @param target the target's name for messages ("SQLite", "MySQL")
     */
    public PlanRules(DataProfile profile, ConvertOptions options, Issues issues, String target) {
        this.profile = profile;
        this.options = options;
        this.issues = issues;
        this.target = target;
    }

    public boolean profiled() {
        return profile != null;
    }

    /** Reports once that the conservative choices were taken, and which. */
    public void reportProfileSkipped(String consequences) {
        if (profile == null) {
            issues.add(
                    IssueCode.PROFILE_SKIPPED, null, null, "the data was not profiled (--no-profile): " + consequences);
        }
    }

    // ---------------------------------------------------------------- profile lookups

    public ColumnStats stats(String table, String column) {
        if (profile == null) {
            return null;
        }
        return profile.table(table).flatMap(t -> t.column(column)).orElse(null);
    }

    /** The table's row count, or null without a profile. */
    public Long rows(String table) {
        if (profile == null) {
            return null;
        }
        return profile.table(table).map(DataProfile.TableProfile::rowsScanned).orElse(null);
    }

    public RuleStats tableRule(String table) {
        if (profile == null) {
            return null;
        }
        return profile.table(table).map(DataProfile.TableProfile::tableRule).orElse(null);
    }

    public RelationshipProfile relationship(ForeignKeyModel fk) {
        return profile == null ? null : profile.relationship(fk.name()).orElse(null);
    }

    // ---------------------------------------------------------------- columns

    /**
     * A column that is written, and where its value sits in a row of the source stream.
     *
     * @param olePart for a companion column of an OLE column ({@code --ole-extract} in the SQL targets): which part of
     *     the decoded value it holds; the value at {@code sourceIndex} is then the OLE value. Null otherwise.
     */
    public record WrittenColumn(ColumnModel column, int sourceIndex, OlePart olePart) {
        public WrittenColumn(ColumnModel column, int sourceIndex) {
            this(column, sourceIndex, null);
        }
    }

    /** The parts of a decoded OLE value that the SQL targets write next to the raw column (08). */
    public enum OlePart {
        KIND("__kind"),
        NAME("__name"),
        MIME("__mime"),
        CONTENT("__content");

        private final String suffix;

        OlePart(String suffix) {
            this.suffix = suffix;
        }

        public String suffix() {
            return suffix;
        }
    }

    /**
     * The columns written to the output, in Access order: all of them except, unless asked for, version history
     * ({@code --include-version-history}) and Access's own hidden columns ({@code --include-hidden}) (04, 08). Primary-key
     * columns are always written.
     */
    public List<WrittenColumn> writtenColumns(TableModel table) {
        return writtenColumns(table, false);
    }

    /**
     * As {@link #writtenColumns(TableModel)}.
     *
     * @param oleCompanions with {@code --ole-extract}, follow each OLE column with its {@code __kind}, {@code __name},
     *     {@code __mime} and {@code __content} columns (the SQL targets; JSON writes an object instead)
     */
    public List<WrittenColumn> writtenColumns(TableModel table, boolean oleCompanions) {
        Set<String> keyColumns = primaryKeyColumns(table);
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        table.columns().forEach(c -> names.add(c.name()));
        List<WrittenColumn> written = new ArrayList<>();
        for (int i = 0; i < table.columns().size(); i++) {
            ColumnModel column = table.columns().get(i);
            if (!skip(table, column, keyColumns.contains(column.name()))) {
                written.add(new WrittenColumn(column, i));
                if (oleCompanions && options.oleExtract() && column.type() == AccessType.OLE) {
                    for (OlePart part : OlePart.values()) {
                        written.add(new WrittenColumn(companion(column, part, names), i, part));
                    }
                }
            }
        }
        if (written.isEmpty()) {
            // A table must have at least one column; keep everything rather than write an empty one
            for (int i = 0; i < table.columns().size(); i++) {
                written.add(new WrittenColumn(table.columns().get(i), i));
            }
        }
        return written;
    }

    /**
     * An OLE column's companion: {@code <col>__kind} and the rest, named apart from every column of the table. The name
     * is Long Text, so no link path or class name is ever too long for it; the MIME type is one this tool generates.
     */
    private static ColumnModel companion(ColumnModel ole, OlePart part, Set<String> names) {
        String name = ole.name() + part.suffix();
        for (int n = 2; names.contains(name); n++) {
            name = ole.name() + part.suffix() + "_" + n;
        }
        names.add(name);
        AccessType type =
                switch (part) {
                    case KIND, MIME -> AccessType.TEXT;
                    case NAME -> AccessType.MEMO;
                    case CONTENT -> AccessType.BINARY;
                };
        return new ColumnModel(
                name,
                ole.ordinal(),
                type,
                switch (part) {
                    case KIND -> 16;
                    // A type from MimeSniffer's closed list: short, so it can be indexed and sorted
                    case MIME -> 255;
                    default -> null;
                },
                null,
                null,
                false,
                true,
                null,
                null,
                null,
                null,
                null,
                false,
                null,
                false,
                false,
                null);
    }

    public static Set<String> primaryKeyColumns(TableModel table) {
        Set<String> keyColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (table.primaryKey() != null) {
            keyColumns.addAll(table.primaryKey().columnNames());
        }
        return keyColumns;
    }

    private boolean skip(TableModel table, ColumnModel column, boolean inPrimaryKey) {
        if (inPrimaryKey) {
            return false;
        }
        if (column.type() == AccessType.VERSION_HISTORY) {
            if (options.versionHistory()) {
                return false;
            }
            issues.add(
                    IssueCode.VERSION_HISTORY_SKIPPED,
                    table.name(),
                    column.name(),
                    "the append-only memo's version history is not written; the memo itself is. Pass"
                            + " --include-version-history to write it");
            return true;
        }
        if (column.hidden() && !options.includeHidden()) {
            issues.add(
                    IssueCode.HIDDEN_COLUMN_SKIPPED,
                    table.name(),
                    column.name(),
                    "Access maintains this column itself; pass --include-hidden to write it");
            return true;
        }
        return false;
    }

    /** Columns Access marks as required, directly or through an index that can't hold NULLs. */
    public static Set<String> requiredColumns(TableModel table) {
        Set<String> required = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (ColumnModel column : table.columns()) {
            if (column.required()) {
                required.add(column.name());
            }
        }
        for (IndexModel index : table.allIndexes()) {
            if (index.required()) {
                required.addAll(index.columnNames());
            }
        }
        return required;
    }

    /**
     * NOT NULL only where the data allows it (05, Constraints): Access's own guarantees (Yes/No, autonumbers and
     * primary keys) plus Required columns whose profiled NULL count is zero.
     */
    public boolean notNull(TableModel table, ColumnModel column, boolean required, boolean inPrimaryKey) {
        // A complex child's ref is its parent's complex id, which every child row is read from (08)
        boolean guaranteed = column.type() == AccessType.BOOLEAN
                || column.type().isAutoNumber()
                || inPrimaryKey
                || (table.isComplexChild()
                        && column.name().equals(table.complex().refColumn()));
        if (!guaranteed && !required) {
            return false;
        }
        ColumnStats stats = stats(table.name(), column.name());
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
                column.name(),
                "the column is nullable in the output: Access marks it as required, but " + nulls + " rows hold NULL");
        return false;
    }

    /** Whether the profile proves no existing row holds {@code ""}, so {@code AllowZeroLength = No} can be a CHECK. */
    public boolean noEmptyStrings(TableModel table, ColumnModel column) {
        ColumnStats stats = stats(table.name(), column.name());
        Long empty = stats == null ? null : stats.emptyStrings();
        return column.type().isText() && !column.allowZeroLength() && empty != null && empty == 0;
    }

    // ---------------------------------------------------------------- checks

    /**
     * Whether the profile proves every row satisfies a validation rule, so a CHECK can be emitted. Access doesn't
     * re-validate old rows when a rule is added, so the data may violate its own rule (04).
     *
     * @param column the column the rule belongs to, or null for the table's rule
     */
    public boolean ruleHolds(TableModel table, CheckRule rule, String column) {
        return ruleHolds(table, rule, column, false);
    }

    /**
     * {@link #ruleHolds(TableModel, CheckRule, String)} for a target that compares text as SQLite does, folding ASCII
     * letters only and in code-point order: the rows must also satisfy the rule compared that way, or the CHECK would
     * reject rows Access accepted (a Greek {@code "α"} against {@code In ("Α")}).
     */
    public boolean ruleHoldsUnderAsciiNocase(TableModel table, CheckRule rule, String column) {
        return ruleHolds(table, rule, column, true);
    }

    private boolean ruleHolds(TableModel table, CheckRule rule, String column, boolean asciiNocase) {
        RuleStats stats = column == null
                ? tableRule(table.name())
                : java.util.Optional.ofNullable(stats(table.name(), column))
                        .map(ColumnStats::rule)
                        .orElse(null);
        if (stats == null) {
            if (profile != null) {
                return false;
            }
            issues.add(
                    IssueCode.CHECK_VIOLATED_BY_DATA,
                    table.name(),
                    column,
                    "no CHECK for the validation rule " + rule.raw()
                            + ": without profiling (--no-profile) the data can't be shown to comply");
            return false;
        }
        if (stats.holds()) {
            if (!asciiNocase || stats.holdsUnderAsciiNocase()) {
                return true;
            }
            issues.add(
                    IssueCode.CHECK_UNTRANSLATABLE,
                    table.name(),
                    column,
                    "no CHECK for the validation rule " + rule.raw() + ": " + stats.asciiNocaseViolations()
                            + " existing rows satisfy it only as Access compares text, whose case folding covers every"
                            + " letter and whose order is alphabetical; " + target + " folds ASCII letters only"
                            + " (COLLATE NOCASE) and orders by code point",
                    String.join("; ", stats.asciiNocaseSamples()));
            return false;
        }
        issues.add(
                IssueCode.CHECK_VIOLATED_BY_DATA,
                table.name(),
                column,
                "no CHECK for the validation rule " + rule.raw() + ": "
                        + (stats.violations() > 0
                                ? stats.violations() + " existing rows violate it, as Access allows"
                                : stats.unevaluable() + " rows could not be checked (" + stats.firstError() + ")"),
                String.join("; ", stats.samples()));
        return false;
    }

    public void checkUntranslatable(TableModel table, String column, CheckRule rule, String problem) {
        issues.add(
                IssueCode.CHECK_UNTRANSLATABLE,
                table.name(),
                column,
                (column == null ? "the table's validation rule " : "the validation rule ") + rule.raw() + " has no "
                        + target + " CHECK: " + problem);
    }

    // ---------------------------------------------------------------- foreign keys

    /** What the profile says about a relationship Access enforces. */
    public enum ForeignKeyData {
        /** Every child row finds its parent exactly as stored. */
        EXACT,
        /**
         * Some child rows find their parent only under Access's case-insensitive comparison, and the difference is
         * ASCII letter case alone.
         */
        ASCII_CASE_ONLY,
        /** Not profiled: nothing is known about orphans or inexact matches. */
        UNKNOWN,
        /** Orphans, or inexact matches beyond what the target's comparison accepts: no foreign key. */
        BLOCKED
    }

    /**
     * Whether the data allows a foreign key: no orphans, and inexact matches only where the target compares text as
     * Access does. Reports {@code FK_SKIPPED_ORPHANS} when it doesn't.
     *
     * @param caseInsensitive the target already compares the key columns case-insensitively, so matches that differ
     *     in ASCII letter case alone are fine; otherwise the caller must relax the comparison or give up
     */
    public ForeignKeyData foreignKeyData(ForeignKeyModel fk, boolean caseInsensitive) {
        RelationshipProfile stats = relationship(fk);
        if (stats == null) {
            return ForeignKeyData.UNKNOWN;
        }
        if (stats.orphans() > 0) {
            issues.add(
                    IssueCode.FK_SKIPPED_ORPHANS,
                    fk.childTable(),
                    fk.name(),
                    "foreign key skipped: " + stats.orphans() + " of " + stats.checked()
                            + " child rows have no parent row, which " + target + " would reject",
                    String.join("; ", stats.orphanSamples()));
            return ForeignKeyData.BLOCKED;
        }
        if (stats.inexact() == 0) {
            return ForeignKeyData.EXACT;
        }
        if (!stats.inexactAsciiCaseOnly()) {
            issues.add(
                    IssueCode.FK_SKIPPED_ORPHANS,
                    fk.childTable(),
                    fk.name(),
                    "foreign key skipped: " + stats.inexact() + " child rows match their parent only under Access's"
                            + " text comparison, and the difference is more than ASCII letter case",
                    String.join("; ", stats.inexactSamples()));
            return ForeignKeyData.BLOCKED;
        }
        return ForeignKeyData.ASCII_CASE_ONLY;
    }

    public void foreignKeySkipped(ForeignKeyModel fk, IssueCode code, String reason) {
        issues.add(code, fk.childTable(), fk.name(), "foreign key skipped: " + reason);
    }

    /**
     * {@code ON DELETE SET NULL} can't work on a NOT NULL child column; Access's own rule is then NO ACTION (04).
     *
     * @param notNullColumn the first NOT NULL child column, or null when there is none
     */
    public ForeignKeyModel.Action onDelete(ForeignKeyModel fk, String notNullColumn) {
        if (fk.onDelete() != ForeignKeyModel.Action.SET_NULL || notNullColumn == null) {
            return fk.onDelete();
        }
        issues.add(
                IssueCode.FK_SET_NULL_ON_REQUIRED,
                fk.childTable(),
                fk.name(),
                "ON DELETE SET NULL downgraded to NO ACTION: " + notNullColumn + " is NOT NULL");
        return ForeignKeyModel.Action.NO_ACTION;
    }

    // ---------------------------------------------------------------- indexes

    /**
     * The index a relationship needs on its child columns: Access creates none for a relationship it doesn't enforce
     * (D7), and a target may need one where Access's own was left out.
     */
    public static IndexModel relationshipIndex(ForeignKeyModel fk) {
        List<IndexColumn> columns =
                fk.childColumns().stream().map(c -> new IndexColumn(c, true)).toList();
        return new IndexModel(
                fk.name(), columns, false, false, false, false, IndexModel.Origin.RELATIONSHIP, List.of(fk.name()));
    }

    public void indexAddedForRelationship(ForeignKeyModel fk, String why) {
        issues.add(
                IssueCode.INDEX_ADDED_FOR_RELATIONSHIP,
                fk.childTable(),
                fk.name(),
                "index on " + String.join(", ", fk.childColumns()) + ": " + why);
    }

    /** Whether one of these column lists starts with {@code columns}, so lookups on them are covered. */
    public static boolean covered(List<List<String>> existing, List<String> columns) {
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

    /**
     * Access's hidden unique index on an attachment or multi-value column indexes values that JSON inlines as arrays,
     * so there it is skipped (F-16). In the SQL targets the column holds its complex id, which the child table's
     * foreign key refers to, so the index is a real key and stays; only a complex column without a child table (an
     * unsupported kind) loses it.
     *
     * @param childTables whether the target writes complex columns as child tables ({@link ComplexTables})
     * @return whether the index is skipped
     */
    public boolean skipsComplexIndex(TableModel table, IndexModel index, boolean childTables) {
        ColumnModel complex = index.columnNames().stream()
                .map(name -> table.column(name).orElse(null))
                .filter(c -> c != null && c.type().isComplex() && !(childTables && ComplexTables.expands(c, options)))
                .findFirst()
                .orElse(null);
        if (complex == null) {
            return false;
        }
        issues.add(
                IssueCode.INDEX_SKIPPED_COMPLEX_COLUMN,
                table.name(),
                index.name(),
                "index skipped: " + complex.name() + " is an Access "
                        + complex.type().name().toLowerCase(Locale.ROOT).replace('_', ' ')
                        + " column, whose values "
                        + (childTables ? "have no child table" : "are written inline as an array"));
        return true;
    }
}
