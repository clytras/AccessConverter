package com.lytrax.accessconverter.extract;

import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.AccessSource.LinkedTable;
import com.lytrax.accessconverter.source.SourceException;
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
            tables.add(new TableModel(
                    linked.name(),
                    new TableModel.LinkInfo(linked.database(), linked.remoteTable(), linked.odbc()),
                    List.of(),
                    null,
                    List.of(),
                    null,
                    null,
                    0));
            issues.add(
                    IssueCode.LINKED_TABLE_SKIPPED,
                    linked.name(),
                    null,
                    linkMessage(new TableModel.LinkInfo(linked.database(), linked.remoteTable(), linked.odbc())));
        }
        tables.sort(Comparator.comparing(TableModel::name, AccessSource.NAME_ORDER));

        List<ForeignKeyModel> relationships = List.of();
        Optional<Table> msysRelationships = source.systemTable(AccessSource.RELATIONSHIPS_TABLE);
        if (msysRelationships.isPresent()) {
            List<Row> rows = new ArrayList<>();
            try {
                msysRelationships.get().forEach(rows::add);
            } catch (RuntimeException e) {
                throw SourceException.readFailed(source.file(), AccessSource.RELATIONSHIPS_TABLE, e);
            }
            relationships = RelationshipResolver.resolve(
                    RelationshipDecoder.decode(rows), tables, source.systemTableNames(), excluded, issues);
        } else {
            issues.add(
                    IssueCode.RELATIONSHIPS_UNREADABLE,
                    null,
                    null,
                    AccessSource.RELATIONSHIPS_TABLE + " is missing from the catalog: no relationship can be read");
        }

        return new SchemaModel(
                new SchemaModel.Source(
                        source.file().getFileName().toString(),
                        source.fileFormat(),
                        source.codePage(),
                        source.charset().name()),
                tables,
                relationships);
    }

    /** What a linked table is, and what to convert instead: the back-end file, or nothing for an ODBC source. */
    private static String linkMessage(TableModel.LinkInfo link) {
        return link.odbc()
                ? "linked through ODBC to " + link.remoteTable() + " in " + link.displayDatabase()
                        + "; its data lives on that server and is not read"
                : "linked to " + link.remoteTable() + " in " + link.displayDatabase()
                        + "; its data lives in that database and is not read: convert that file directly";
    }
}
