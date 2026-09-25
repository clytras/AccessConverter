package io.lytrax.accessconverter.target;

import io.lytrax.accessconverter.model.AccessType;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * {@code --add-primary-key}, for the SQL targets: every Access table without a primary key gets one, an AutoNumber
 * column first in the table that numbers the rows 1, 2, 3, ... in the order they are stored. The key has no Access
 * index behind it ({@link IndexModel.Origin#GENERATED}); the source reads such a table in physical order and fills the
 * column in, and from then on the planners, writers and verifiers treat it like any AutoNumber primary key. Nothing is
 * invented without the option: a table Access keeps without a key has none by default.
 */
public final class GeneratedKeys {

    /** The column name when {@code --add-primary-key} names none. */
    public static final String DEFAULT_COLUMN = "id";

    private GeneratedKeys() {}

    /** @param column the new column's name; made unique in each table, case-insensitively, with {@code _2} and on */
    public static SchemaModel add(SchemaModel model, String column, Issues issues) {
        List<TableModel> tables = new ArrayList<>(model.tables().size());
        for (TableModel table : model.tables()) {
            if (table.isLinked() || table.isComplexChild() || table.primaryKey() != null) {
                tables.add(table);
                continue;
            }
            Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            table.columns().forEach(c -> names.add(c.name()));
            String name = column;
            for (int n = 2; names.contains(name); n++) {
                name = column + "_" + n;
            }
            List<ColumnModel> columns = new ArrayList<>(table.columns().size() + 1);
            columns.add(new ColumnModel(
                    name,
                    -1,
                    AccessType.AUTONUMBER_LONG,
                    null,
                    null,
                    null,
                    true,
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
            columns.addAll(table.columns());
            IndexModel key = new IndexModel(
                    "PrimaryKey",
                    List.of(new IndexModel.IndexColumn(name, true)),
                    true,
                    true,
                    false,
                    true,
                    IndexModel.Origin.GENERATED,
                    List.of());
            tables.add(new TableModel(
                    table.name(),
                    table.link(),
                    columns,
                    key,
                    table.indexes(),
                    table.description(),
                    table.validation(),
                    table.rowCount(),
                    table.complex()));
            issues.remove(IssueCode.NO_PRIMARY_KEY, table.name(), null);
            issues.add(
                    IssueCode.PRIMARY_KEY_ADDED,
                    table.name(),
                    name,
                    "no primary key in Access; " + name + " was added (--add-primary-key): an AutoNumber numbering the"
                            + " rows 1, 2, 3, ... in the order Access stores them"
                            + (name.equals(column) ? "" : ", named " + name + " because " + column + " is taken"));
        }
        return new SchemaModel(model.source(), tables, model.relationships());
    }
}
