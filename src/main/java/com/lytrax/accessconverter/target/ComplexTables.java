package com.lytrax.accessconverter.target;

import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.IndexModel.IndexColumn;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.model.TableModel.ComplexSource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * The SQL targets' shape for complex columns (08): each attachment and multi-value column, and each version-history
 * column when {@code --include-version-history} asks for it, becomes a child table {@code <Table>_<Column>} keyed by
 * Access's complex value id, with {@code <Column>_ref} referring to the complex id the parent column holds, under a
 * cascading foreign key. The parent column keeps its complex id and Access's hidden unique index on it, which is now a
 * real key.
 *
 * <p>The child tables are ordinary tables of the expanded model, so planning, profiling, writing and verifying them is
 * what every table gets; only their rows come from somewhere else ({@link TableModel#complex()}). JSON doesn't expand:
 * it inlines the values as arrays.
 */
public final class ComplexTables {

    private static final Comparator<String> NAME_ORDER =
            String.CASE_INSENSITIVE_ORDER.thenComparing(Comparator.naturalOrder());

    private ComplexTables() {}

    /** Whether a column's values become a child table under these options. */
    public static boolean expands(ColumnModel column, ConvertOptions options) {
        return switch (column.type()) {
            case ATTACHMENT, MULTI_VALUE -> true;
            case VERSION_HISTORY -> options.versionHistory();
            default -> false;
        };
    }

    /** The model with a child table and its foreign key for every complex column that is written. */
    public static SchemaModel expand(SchemaModel model, ConvertOptions options) {
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        model.tables().forEach(t -> names.add(t.name()));
        List<TableModel> tables = new ArrayList<>(model.tables());
        List<ForeignKeyModel> relationships = new ArrayList<>(model.relationships());
        Set<String> relationshipNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        model.relationships().forEach(r -> relationshipNames.add(r.name()));
        for (TableModel table : model.tables()) {
            if (table.isLinked()) {
                continue;
            }
            for (ColumnModel column : table.columns()) {
                if ((column.hidden() && !options.includeHidden() && column.type() != AccessType.VERSION_HISTORY)
                        || !expands(column, options)) {
                    continue;
                }
                String name = unique(table.name() + "_" + column.name(), names);
                TableModel child = child(name, table, column);
                tables.add(child);
                IndexModel key = parentKey(table, column);
                relationships.add(new ForeignKeyModel(
                        unique(name, relationshipNames),
                        table.name(),
                        List.of(column.name()),
                        name,
                        List.of(child.complex().refColumn()),
                        true,
                        ForeignKeyModel.Action.CASCADE,
                        ForeignKeyModel.Action.CASCADE,
                        false,
                        ForeignKeyModel.Join.INNER,
                        0,
                        key == null ? ForeignKeyModel.Status.SKIPPED_PARENT_KEY_MISSING : ForeignKeyModel.Status.EMIT,
                        key == null ? null : key.name()));
            }
        }
        if (tables.size() == model.tables().size()) {
            return model;
        }
        tables.sort(Comparator.comparing(TableModel::name, NAME_ORDER));
        relationships.sort(Comparator.comparing(ForeignKeyModel::name, NAME_ORDER));
        return new SchemaModel(model.source(), tables, relationships);
    }

    /** Access's hidden unique index on the complex column: the key the child's foreign key refers to. */
    private static IndexModel parentKey(TableModel table, ColumnModel column) {
        return table.allIndexes().stream()
                .filter(i -> i.unique()
                        && i.columns().size() == 1
                        && i.columnNames().get(0).equalsIgnoreCase(column.name()))
                .findFirst()
                .orElse(null);
    }

    private static TableModel child(String name, TableModel parent, ColumnModel column) {
        ComplexSource source = new ComplexSource(parent.name(), column.name(), column.type(), parent.primaryKey());
        List<ColumnModel> columns = new ArrayList<>();
        columns.add(column(ComplexSource.ID, columns.size(), AccessType.LONG, null, true));
        columns.add(column(source.refColumn(), columns.size(), AccessType.LONG, null, true));
        switch (column.type()) {
            case ATTACHMENT -> {
                columns.add(column(ComplexSource.FILE_NAME, columns.size(), AccessType.TEXT, 255, false));
                columns.add(column(ComplexSource.FILE_TYPE, columns.size(), AccessType.TEXT, 255, false));
                // Binary without a length: bytes of any size (LONGBLOB, BLOB)
                columns.add(column(ComplexSource.FILE_DATA, columns.size(), AccessType.BINARY, null, false));
                columns.add(column(ComplexSource.FILE_SIZE, columns.size(), AccessType.BIG_INT, null, false));
                columns.add(column(ComplexSource.FILE_URL, columns.size(), AccessType.MEMO, null, false));
                columns.add(
                        column(ComplexSource.FILE_TIMESTAMP, columns.size(), AccessType.SHORT_DATE_TIME, null, false));
                columns.add(column(ComplexSource.FILE_FLAGS, columns.size(), AccessType.LONG, null, false));
            }
            case MULTI_VALUE -> {
                ColumnModel element = column.element();
                columns.add(
                        element == null
                                ? column(ComplexSource.VALUE, columns.size(), AccessType.MEMO, null, false)
                                : new ColumnModel(
                                        ComplexSource.VALUE,
                                        columns.size(),
                                        element.type(),
                                        element.length(),
                                        element.precision(),
                                        element.scale(),
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
                                        null));
            }
            case VERSION_HISTORY -> {
                columns.add(column(ComplexSource.VALUE, columns.size(), AccessType.MEMO, null, false));
                columns.add(column(ComplexSource.MODIFIED, columns.size(), AccessType.SHORT_DATE_TIME, null, false));
            }
            default -> throw new IllegalArgumentException("no child table for " + column.type());
        }
        IndexModel primaryKey = new IndexModel(
                "PrimaryKey",
                List.of(new IndexColumn(ComplexSource.ID, true)),
                true,
                true,
                false,
                true,
                IndexModel.Origin.USER,
                List.of());
        IndexModel refIndex = new IndexModel(
                source.refColumn(),
                List.of(new IndexColumn(source.refColumn(), true)),
                false,
                false,
                false,
                false,
                IndexModel.Origin.RELATIONSHIP,
                List.of());
        String description = "The " + label(column.type()) + " of " + parent.name() + "." + column.name();
        return new TableModel(name, null, columns, primaryKey, List.of(refIndex), description, null, 0, source);
    }

    private static String label(AccessType kind) {
        return switch (kind) {
            case ATTACHMENT -> "attachments";
            case MULTI_VALUE -> "values";
            default -> "version history";
        };
    }

    private static ColumnModel column(String name, int ordinal, AccessType type, Integer length, boolean required) {
        return new ColumnModel(
                name, ordinal, type, length, null, null, required, true, null, null, null, null, null, false, null,
                false, false, null);
    }

    /** {@code wanted}, or {@code wanted_2}, {@code wanted_3}, … when a name is taken; the result is taken too. */
    private static String unique(String wanted, Set<String> taken) {
        String name = wanted;
        for (int n = 2; taken.contains(name); n++) {
            name = wanted + "_" + n;
        }
        taken.add(name);
        return name;
    }
}
