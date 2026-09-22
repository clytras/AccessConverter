package com.lytrax.accessconverter.extract;

import static com.lytrax.accessconverter.extract.RelationshipDecoder.CASCADE_DELETES;
import static com.lytrax.accessconverter.extract.RelationshipDecoder.CASCADE_NULL;
import static com.lytrax.accessconverter.extract.RelationshipDecoder.CASCADE_UPDATES;
import static com.lytrax.accessconverter.extract.RelationshipDecoder.LEFT_OUTER_JOIN;
import static com.lytrax.accessconverter.extract.RelationshipDecoder.NO_REFERENTIAL_INTEGRITY;
import static com.lytrax.accessconverter.extract.RelationshipDecoder.ONE_TO_ONE;
import static com.lytrax.accessconverter.extract.RelationshipDecoder.RIGHT_OUTER_JOIN;

import com.lytrax.accessconverter.extract.RelationshipDecoder.Decoded;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.ForeignKeyModel.Action;
import com.lytrax.accessconverter.model.ForeignKeyModel.Join;
import com.lytrax.accessconverter.model.ForeignKeyModel.Status;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Classifies decoded relationships (04, Relationships): which are candidate foreign keys ({@link Status#EMIT}),
 * which are join lines only (F-31), and which can't be expressed. Relationships between system tables are Access
 * internals and are left out.
 */
public final class RelationshipResolver {
    private RelationshipResolver() {}

    /**
     * @param tables the model's tables (local and linked), by name
     * @param excluded tables left out by the table filter
     */
    public static List<ForeignKeyModel> resolve(
            List<Decoded> decoded,
            List<TableModel> tables,
            Set<String> systemTables,
            Set<String> excluded,
            Issues issues) {
        Map<String, TableModel> byName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tables.forEach(t -> byName.put(t.name(), t));
        Set<String> excludedNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        excludedNames.addAll(excluded);

        List<ForeignKeyModel> resolved = new ArrayList<>();
        for (Decoded rel : decoded) {
            if (isSystem(rel.parentTable(), systemTables) || isSystem(rel.childTable(), systemTables)) {
                continue;
            }
            resolved.add(classify(rel, byName, excludedNames, issues));
        }
        return resolved;
    }

    private static boolean isSystem(String table, Set<String> systemTables) {
        return table != null && systemTables.stream().anyMatch(table::equalsIgnoreCase);
    }

    private static ForeignKeyModel classify(
            Decoded rel, Map<String, TableModel> tables, Set<String> excluded, Issues issues) {
        TableModel parent = rel.parentTable() == null ? null : tables.get(rel.parentTable());
        TableModel child = rel.childTable() == null ? null : tables.get(rel.childTable());
        String parentName = parent != null ? parent.name() : rel.parentTable();
        String childName = child != null ? child.name() : rel.childTable();
        String table = childName;

        boolean enforced = !rel.has(NO_REFERENTIAL_INTEGRITY);
        Action onUpdate = rel.has(CASCADE_UPDATES) ? Action.CASCADE : Action.NO_ACTION;
        Action onDelete =
                rel.has(CASCADE_DELETES) ? Action.CASCADE : rel.has(CASCADE_NULL) ? Action.SET_NULL : Action.NO_ACTION;
        Join join =
                rel.has(LEFT_OUTER_JOIN) ? Join.LEFT_OUTER : rel.has(RIGHT_OUTER_JOIN) ? Join.RIGHT_OUTER : Join.INNER;

        List<String> parentColumns = rel.parentColumns();
        List<String> childColumns = rel.childColumns();
        Status status;
        String parentKey = null;

        String problem = rel.problem();
        if (problem == null && rel.has(CASCADE_DELETES) && rel.has(CASCADE_NULL)) {
            problem = "both cascade-delete and cascade-set-null are set";
        }
        if (problem == null
                && (parent == null && !excluded.contains(rel.parentTable())
                        || child == null && !excluded.contains(rel.childTable()))) {
            problem = "table " + (parent == null ? rel.parentTable() : rel.childTable()) + " doesn't exist";
        }

        if (problem != null) {
            status = Status.SKIPPED_MALFORMED;
            issues.add(IssueCode.RELATIONSHIP_MALFORMED, table, rel.name(), "relationship skipped: " + problem);
        } else if (parent == null || child == null) {
            status = Status.SKIPPED_TABLE_EXCLUDED;
            issues.add(
                    IssueCode.FK_SKIPPED_TABLE_EXCLUDED,
                    table,
                    rel.name(),
                    "relationship skipped: table " + (parent == null ? parentName : childName) + " is excluded");
        } else if (parent.isLinked() || child.isLinked()) {
            status = Status.SKIPPED_LINKED_TABLE;
            issues.add(
                    IssueCode.FK_SKIPPED_LINKED_TABLE,
                    table,
                    rel.name(),
                    "relationship skipped: table " + (parent.isLinked() ? parentName : childName) + " is linked");
        } else {
            Optional<List<String>> parentSpelled = spelled(parent, parentColumns);
            Optional<List<String>> childSpelled = spelled(child, childColumns);
            if (parentSpelled.isEmpty() || childSpelled.isEmpty()) {
                status = Status.SKIPPED_MALFORMED;
                issues.add(
                        IssueCode.RELATIONSHIP_MALFORMED,
                        table,
                        rel.name(),
                        "relationship skipped: it names a column that doesn't exist");
            } else {
                parentColumns = parentSpelled.get();
                childColumns = childSpelled.get();
                if (!enforced) {
                    status = Status.NOT_ENFORCED;
                    issues.add(
                            IssueCode.FK_SKIPPED_NOT_ENFORCED,
                            table,
                            rel.name(),
                            "join line only (no referential integrity in Access, so orphans are allowed): "
                                    + "not a foreign key");
                } else {
                    parentKey = parentKey(parent, parentColumns);
                    if (parentKey != null) {
                        status = Status.EMIT;
                    } else {
                        status = Status.SKIPPED_PARENT_KEY_MISSING;
                        issues.add(
                                IssueCode.FK_SKIPPED_PARENT_KEY_MISSING,
                                table,
                                rel.name(),
                                "relationship skipped: " + parentName + " " + parentColumns
                                        + " is not its primary key or a unique index");
                    }
                }
            }
        }
        return new ForeignKeyModel(
                rel.name(),
                parentName,
                parentColumns,
                childName,
                childColumns,
                enforced,
                onUpdate,
                onDelete,
                rel.has(ONE_TO_ONE),
                join,
                rel.flags(),
                status,
                parentKey);
    }

    /** The columns as the table spells them, or empty when one is missing. */
    private static Optional<List<String>> spelled(TableModel table, List<String> columns) {
        List<String> names = new ArrayList<>(columns.size());
        for (String column : columns) {
            Optional<ColumnModel> found = table.column(column);
            if (found.isEmpty()) {
                return Optional.empty();
            }
            names.add(found.get().name());
        }
        return Optional.of(names);
    }

    /** The PK or unique index whose column set equals {@code columns}: PK first, then indexes by name. */
    private static String parentKey(TableModel parent, List<String> columns) {
        Set<String> wanted = lower(columns);
        for (IndexModel index : parent.allIndexes()) {
            if (index.unique() && lower(index.columnNames()).equals(wanted)) {
                return index.name();
            }
        }
        return null;
    }

    private static Set<String> lower(List<String> names) {
        Set<String> lower = new TreeSet<>();
        names.forEach(n -> lower.add(n.toLowerCase(Locale.ROOT)));
        return lower;
    }
}
