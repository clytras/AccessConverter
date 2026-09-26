package io.lytrax.accessconverter.extract;

import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import io.lytrax.accessconverter.extract.RelationshipDecoder.Decoded;
import io.lytrax.accessconverter.model.ForeignKeyModel;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.AccessSource.LinkedTable;
import io.lytrax.accessconverter.source.AccessSource.ResolvedLink;
import io.lytrax.accessconverter.source.SourceException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** Builds the {@link SchemaModel} from metadata only; no rows are read (03, extract). */
public final class SchemaExtractor {

    private SchemaExtractor() {}

    public static SchemaModel extract(AccessSource source, ExtractOptions options, Issues issues) throws IOException {
        List<TableModel> tables = new ArrayList<>();
        Set<String> excluded = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        for (Table table : source.localTables()) {
            if (!options.tables().test(table.getName())) {
                excluded.add(table.getName());
                issues.add(IssueCode.TABLE_EXCLUDED, table.getName(), null, "excluded by the table filter");
                continue;
            }
            tables.add(TableReader.read(source.file(), table, issues));
        }
        for (LinkedTable linked : source.linkedTables()) {
            if (!options.tables().test(linked.name())) {
                excluded.add(linked.name());
                issues.add(IssueCode.TABLE_EXCLUDED, linked.name(), null, "excluded by the table filter");
                continue;
            }
            TableModel.LinkInfo link = new TableModel.LinkInfo(linked.database(), linked.remoteTable(), linked.odbc());
            if (source.resolvesLinks() && !linked.odbc()) {
                TableModel read = resolve(source, linked, options, issues);
                if (read != null) {
                    tables.add(read);
                    continue;
                }
            } else {
                issues.add(IssueCode.LINKED_TABLE_SKIPPED, linked.name(), null, linkMessage(link));
            }
            tables.add(new TableModel(linked.name(), link, List.of(), null, List.of(), null, null, 0));
        }
        tables.sort(Comparator.comparing(TableModel::name, AccessSource.NAME_ORDER));

        List<Decoded> decoded = new ArrayList<>();
        Optional<Table> msysRelationships = source.systemTable(AccessSource.RELATIONSHIPS_TABLE);
        if (msysRelationships.isPresent()) {
            decoded.addAll(RelationshipDecoder.decode(rows(source, msysRelationships.get())));
        } else {
            issues.add(
                    IssueCode.RELATIONSHIPS_UNREADABLE,
                    null,
                    null,
                    AccessSource.RELATIONSHIPS_TABLE + " is missing from the catalog: no relationship can be read");
        }
        for (AccessSource.Backend backend : source.backends()) {
            decoded.addAll(BackendRelationships.of(backend, decoded, issues));
        }
        decoded.sort(Comparator.comparing(Decoded::name, AccessSource.NAME_ORDER));
        List<ForeignKeyModel> relationships =
                RelationshipResolver.resolve(decoded, tables, source.systemTableNames(), excluded, issues);

        return new SchemaModel(
                new SchemaModel.Source(
                        source.file().getFileName().toString(),
                        source.fileFormat(),
                        source.codePage(),
                        source.charset().name()),
                tables,
                relationships);
    }

    /**
     * A linked table read from its back-end ({@code --linked resolve}), under its name here; null when it stays
     * unread: a link to a link, or, with {@code --on-table-error continue}, a back-end or table that can't be read.
     */
    private static TableModel resolve(AccessSource source, LinkedTable linked, ExtractOptions options, Issues issues)
            throws IOException {
        try {
            Optional<ResolvedLink> resolved = source.resolve(linked);
            if (resolved.isEmpty()) {
                issues.add(
                        IssueCode.LINKED_TABLE_SKIPPED,
                        linked.name(),
                        null,
                        "linked to " + linked.remoteTable() + " in " + linked.database()
                                + ", which is itself a linked table there; links of links are not followed: convert"
                                + " the database it links to");
                return null;
            }
            String file = resolved.get().file().toString();
            TableModel table = TableReader.read(
                    resolved.get().file(),
                    resolved.get().table(),
                    linked.name(),
                    new TableModel.LinkInfo(linked.database(), linked.remoteTable(), false, file),
                    issues);
            issues.add(
                    IssueCode.LINKED_TABLE_RESOLVED,
                    linked.name(),
                    null,
                    "linked to " + linked.remoteTable() + " in " + linked.database() + "; read from " + file);
            return table;
        } catch (SourceException e) {
            if (!options.continueOnTableError()) {
                throw e;
            }
            issues.add(IssueCode.TABLE_READ_FAILED, linked.name(), null, e.getMessage() + "; the table is left out");
            return null;
        }
    }

    static List<Row> rows(AccessSource source, Table msysRelationships) throws SourceException {
        List<Row> rows = new ArrayList<>();
        try {
            msysRelationships.forEach(rows::add);
        } catch (RuntimeException e) {
            throw SourceException.readFailed(source.file(), AccessSource.RELATIONSHIPS_TABLE, e);
        }
        return rows;
    }

    /** What a linked table is, and what to convert instead: the back-end file, or nothing for an ODBC source. */
    private static String linkMessage(TableModel.LinkInfo link) {
        return link.odbc()
                ? "linked through ODBC to " + link.remoteTable() + " in " + link.displayDatabase()
                        + "; its data lives on that server and is not read"
                : "linked to " + link.remoteTable() + " in " + link.displayDatabase()
                        + "; its data lives in that database and is not read: convert that file directly, or pass"
                        + " --linked resolve";
    }
}
