package io.lytrax.accessconverter.target.sqlite;

import static io.lytrax.accessconverter.target.IdentifierPolicy.quote;

import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.ForeignKeyModel;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;

/**
 * The opt-in {@code --sqlite-metadata} tables (06): the Access column properties and relationships that the SQLite
 * schema itself can't express, so a consumer can read the whole Access schema from inside the file. They describe
 * the source database, including the columns and relationships the conversion left out.
 */
final class SqliteMetadata {

    private static final List<String> COLUMN_FIELDS = List.of(
            "table_name",
            "column_name",
            "ordinal",
            "access_type",
            "length",
            "precision",
            "scale",
            "required",
            "allow_zero_length",
            "description",
            "format",
            "decimal_places",
            "default_expr",
            "validation_rule",
            "validation_text",
            "calculated_expr",
            "append_only",
            "hidden",
            "written_as");

    private static final List<String> RELATIONSHIP_FIELDS = List.of(
            "name",
            "parent_table",
            "parent_columns",
            "child_table",
            "child_columns",
            "enforced",
            "on_update",
            "on_delete",
            "one_to_one",
            "join_type",
            "status",
            "written_as_foreign_key");

    /** One row: what wrote the file, from which database. No time, so the output stays deterministic. */
    private static final List<String> EXPORT_FIELDS =
            List.of("producer", "format_version", "source_file", "source_format", "source_code_page", "source_charset");

    private SqliteMetadata() {}

    static void create(Connection db, SqlitePlan plan) throws SQLException {
        execute(db, createTable(plan.metadata().columnsTable(), COLUMN_FIELDS));
        execute(db, createTable(plan.metadata().relationshipsTable(), RELATIONSHIP_FIELDS));
        execute(db, createTable(plan.metadata().exportTable(), EXPORT_FIELDS));
    }

    static void fill(Connection db, SqlitePlan plan, String producer) throws SQLException {
        try (PreparedStatement insert =
                db.prepareStatement(insert(plan.metadata().exportTable(), EXPORT_FIELDS))) {
            SchemaModel.Source source = plan.model().source();
            int at = 0;
            insert.setString(++at, producer);
            insert.setInt(++at, SqliteWriter.FORMAT_VERSION);
            insert.setString(++at, source.fileName());
            insert.setString(++at, source.fileFormat());
            set(insert, ++at, source.codePage());
            insert.setString(++at, source.charset());
            insert.executeUpdate();
        }
        try (PreparedStatement insert =
                db.prepareStatement(insert(plan.metadata().columnsTable(), COLUMN_FIELDS))) {
            for (TableModel table : plan.model().tables()) {
                SqlitePlan.PlannedTable planned = plan.table(table.name()).orElse(null);
                for (ColumnModel column : table.columns()) {
                    if (column.name().equals(table.generatedKeyColumn())) {
                        continue; // not an Access column: --add-primary-key added it
                    }
                    String writtenAs = planned == null
                            ? null
                            : planned.column(column.name())
                                    .map(SqlitePlan.PlannedColumn::declaredType)
                                    .orElse(null);
                    int at = 0;
                    insert.setString(++at, table.name());
                    insert.setString(++at, column.name());
                    insert.setInt(++at, column.ordinal());
                    insert.setString(++at, column.type().name());
                    set(insert, ++at, column.length());
                    set(insert, ++at, column.precision());
                    set(insert, ++at, column.scale());
                    insert.setInt(++at, column.required() ? 1 : 0);
                    insert.setInt(++at, column.allowZeroLength() ? 1 : 0);
                    insert.setString(++at, column.description());
                    insert.setString(++at, column.format());
                    set(insert, ++at, column.decimalPlaces());
                    insert.setString(
                            ++at,
                            column.defaultValue() == null
                                    ? null
                                    : column.defaultValue().raw());
                    insert.setString(
                            ++at,
                            column.validation() == null
                                    ? null
                                    : column.validation().raw());
                    insert.setString(
                            ++at,
                            column.validation() == null
                                    ? null
                                    : column.validation().validationText());
                    insert.setString(++at, column.calculatedExpression());
                    insert.setInt(++at, column.appendOnly() ? 1 : 0);
                    insert.setInt(++at, column.hidden() ? 1 : 0);
                    insert.setString(++at, writtenAs);
                    insert.addBatch();
                }
            }
            insert.executeBatch();
        }
        try (PreparedStatement insert =
                db.prepareStatement(insert(plan.metadata().relationshipsTable(), RELATIONSHIP_FIELDS))) {
            for (ForeignKeyModel fk : plan.model().relationships()) {
                boolean written = plan.table(fk.childTable())
                        .map(t -> t.foreignKeys().stream()
                                .anyMatch(k -> k.source().name().equals(fk.name())))
                        .orElse(false);
                int at = 0;
                insert.setString(++at, fk.name());
                insert.setString(++at, fk.parentTable());
                insert.setString(++at, String.join(", ", fk.parentColumns()));
                insert.setString(++at, fk.childTable());
                insert.setString(++at, String.join(", ", fk.childColumns()));
                insert.setInt(++at, fk.enforced() ? 1 : 0);
                insert.setString(++at, action(fk.onUpdate()));
                insert.setString(++at, action(fk.onDelete()));
                insert.setInt(++at, fk.oneToOne() ? 1 : 0);
                insert.setString(++at, lower(fk.join()));
                insert.setString(++at, lower(fk.status()));
                insert.setInt(++at, written ? 1 : 0);
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    private static String createTable(String table, List<String> fields) {
        StringBuilder sql =
                new StringBuilder("CREATE TABLE ").append(quote(table)).append(" (\n");
        sql.append("  -- Access metadata, added by --sqlite-metadata; it describes the source database\n");
        for (int i = 0; i < fields.size(); i++) {
            sql.append("  ").append(quote(fields.get(i))).append(i < fields.size() - 1 ? ",\n" : "\n");
        }
        return sql.append(')').toString();
    }

    private static String insert(String table, List<String> fields) {
        return "INSERT INTO " + quote(table) + " ("
                + String.join(", ", fields.stream().map(f -> quote(f)).toList()) + ") VALUES ("
                + "?, ".repeat(fields.size() - 1) + "?)";
    }

    private static void set(PreparedStatement statement, int at, Integer value) throws SQLException {
        if (value == null) {
            statement.setNull(at, java.sql.Types.NULL);
        } else {
            statement.setInt(at, value);
        }
    }

    private static String action(ForeignKeyModel.Action action) {
        return switch (action) {
            case NO_ACTION -> "no action";
            case CASCADE -> "cascade";
            case SET_NULL -> "set null";
        };
    }

    private static String lower(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT).replace('_', ' ');
    }

    private static void execute(Connection db, String sql) throws SQLException {
        try (Statement statement = db.createStatement()) {
            statement.execute(sql);
        }
    }
}
