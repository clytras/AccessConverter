package io.lytrax.accessconverter.extract;

import com.healthmarketscience.jackcess.Table;
import io.lytrax.accessconverter.extract.RelationshipDecoder.Decoded;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * A back-end's own relationships, for {@code --linked resolve} (D11): those between tables that were all read from
 * that back-end, under the names the linking database gives them, so an enforced one becomes a foreign key like any
 * other. A relationship to a table of the back-end that isn't read here (not linked, excluded, or linked twice) is
 * reported and left out: its other table may share a name with a table of this database, and it isn't this
 * database's relationship. A name this database already uses gets a suffix.
 */
final class BackendRelationships {
    private BackendRelationships() {}

    /**
     * @param taken the relationships decoded so far, whose names are taken
     */
    static List<Decoded> of(AccessSource.Backend backend, List<Decoded> taken, Issues issues) throws IOException {
        AccessSource source = backend.source();
        String file = source.file().getFileName().toString();
        Optional<Table> msysRelationships = source.systemTable(AccessSource.RELATIONSHIPS_TABLE);
        if (msysRelationships.isEmpty()) {
            issues.add(
                    IssueCode.RELATIONSHIPS_UNREADABLE,
                    null,
                    file,
                    file + " (linked): " + AccessSource.RELATIONSHIPS_TABLE
                            + " is missing from its catalog: none of its relationships can be read");
            return List.of();
        }
        List<Decoded> decoded = RelationshipDecoder.decode(SchemaExtractor.rows(source, msysRelationships.get()));
        Set<String> systemTables = source.systemTableNames();

        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        taken.forEach(d -> names.add(d.name()));
        List<Decoded> mapped = new ArrayList<>();
        for (Decoded d : decoded) {
            if (isSystem(d.parentTable(), systemTables) || isSystem(d.childTable(), systemTables)) {
                continue; // Access internals of the back-end
            }
            List<String> parents =
                    d.parentTable() == null ? null : backend.localNames().get(d.parentTable());
            List<String> children =
                    d.childTable() == null ? null : backend.localNames().get(d.childTable());
            String reportedTable = children != null ? children.get(0) : parents != null ? parents.get(0) : null;
            if (reportedTable == null) {
                continue; // neither table is read here: nothing of this database is concerned
            }
            if (d.problem() != null) {
                issues.add(
                        IssueCode.RELATIONSHIP_MALFORMED,
                        reportedTable,
                        d.name(),
                        "relationship " + d.name() + " of " + file + " skipped: " + d.problem());
                continue;
            }
            if (parents == null || children == null) {
                issues.add(
                        IssueCode.FK_SKIPPED_TABLE_EXCLUDED,
                        reportedTable,
                        d.name(),
                        "relationship " + d.name() + " of " + file + " skipped: its table "
                                + (parents == null ? d.parentTable() : d.childTable())
                                + " there isn't read (not linked here, or excluded)");
                continue;
            }
            if (parents.size() > 1 || children.size() > 1) {
                List<String> twice = parents.size() > 1 ? parents : children;
                issues.add(
                        IssueCode.RELATIONSHIP_MALFORMED,
                        reportedTable,
                        d.name(),
                        "relationship " + d.name() + " of " + file + " skipped: its table "
                                + (parents.size() > 1 ? d.parentTable() : d.childTable()) + " is linked here more"
                                + " than once (" + String.join(", ", twice) + "), so which one it constrains is"
                                + " unclear");
                continue;
            }
            String name = d.name();
            if (names.contains(name)) {
                String wanted = name;
                for (int n = 2; names.contains(name); n++) {
                    name = wanted + "_" + n;
                }
                issues.add(
                        IssueCode.IDENTIFIER_RENAMED,
                        children.get(0),
                        name,
                        "relationship " + wanted + " of " + file + " renamed to " + name
                                + ": this database has a relationship of that name");
            }
            names.add(name);
            mapped.add(new Decoded(
                    name,
                    parents.get(0),
                    children.get(0),
                    d.parentColumns(),
                    d.childColumns(),
                    d.flags(),
                    d.problem()));
        }
        return mapped;
    }

    private static boolean isSystem(String table, Set<String> systemTables) {
        return table != null && systemTables.contains(table);
    }
}
