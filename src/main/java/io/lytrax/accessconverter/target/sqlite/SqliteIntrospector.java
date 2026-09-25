package io.lytrax.accessconverter.target.sqlite;

import static io.lytrax.accessconverter.target.IdentifierPolicy.quote;

import io.lytrax.accessconverter.model.IndexModel.IndexColumn;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Reads a finished SQLite file's schema back, the way {@code verify} needs it (09, step 2): {@code table_xinfo},
 * {@code index_list}, {@code index_xinfo}, {@code foreign_key_list} and the statements in {@code sqlite_schema}.
 */
public final class SqliteIntrospector {

    private SqliteIntrospector() {}

    /** Reads a table's rows in rowid order, which is the order they were written in. */
    public static String selectAll(String table, List<String> columns) {
        return "SELECT " + String.join(", ", columns.stream().map(c -> quote(c)).toList()) + " FROM " + quote(table)
                + " ORDER BY _rowid_";
    }

    /** The whole schema of an existing file, tables ordered by name. */
    public static Schema read(Connection db) throws SQLException {
        TreeMap<String, Table> tables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        TreeMap<String, String> statements = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<String> names = new ArrayList<>();
        try (Statement statement = db.createStatement();
                ResultSet rows = statement.executeQuery("SELECT type, name, tbl_name, sql FROM sqlite_schema"
                        + " WHERE name NOT LIKE 'sqlite_%' ORDER BY name")) {
            while (rows.next()) {
                String type = rows.getString(1);
                String name = rows.getString(2);
                statements.put(type + " " + name, rows.getString(4));
                if ("table".equals(type)) {
                    names.add(name);
                }
            }
        }
        for (String name : names) {
            tables.put(name, table(db, name, statements.get("table " + name), statements));
        }
        return new Schema(tables, statements);
    }

    private static Table table(Connection db, String name, String sql, TreeMap<String, String> statements)
            throws SQLException {
        List<Column> columns = new ArrayList<>();
        try (PreparedStatement statement = db.prepareStatement("SELECT name, type, \"notnull\", dflt_value, pk, hidden"
                + " FROM pragma_table_xinfo(?) ORDER BY cid")) {
            statement.setString(1, name);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    columns.add(new Column(
                            rows.getString(1),
                            rows.getString(2),
                            rows.getInt(3) != 0,
                            rows.getString(4),
                            rows.getInt(5),
                            rows.getInt(6)));
                }
            }
        }
        List<Index> indexes = new ArrayList<>();
        List<IndexColumn> primaryKey = new ArrayList<>();
        try (PreparedStatement statement = db.prepareStatement(
                "SELECT name, \"unique\", origin, partial FROM pragma_index_list(?) ORDER BY name")) {
            statement.setString(1, name);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    String index = rows.getString(1);
                    boolean unique = rows.getInt(2) != 0;
                    String origin = rows.getString(3);
                    boolean partial = rows.getInt(4) != 0;
                    List<IndexColumn> indexColumns = indexColumns(db, index);
                    if ("pk".equals(origin)) {
                        primaryKey.addAll(indexColumns);
                    } else {
                        indexes.add(new Index(
                                index, unique, partial, origin, indexColumns, statements.get("index " + index)));
                    }
                }
            }
        }
        if (primaryKey.isEmpty()) {
            // A rowid alias has no index of its own; table_xinfo still marks it
            columns.stream()
                    .filter(c -> c.primaryKeyPosition() > 0)
                    .sorted(Comparator.comparingInt(Column::primaryKeyPosition))
                    .forEach(c -> primaryKey.add(new IndexColumn(c.name(), true)));
        }
        List<ForeignKey> foreignKeys = foreignKeys(db, name);
        indexes.sort(Comparator.comparing(Index::name, String.CASE_INSENSITIVE_ORDER));
        return new Table(name, sql, columns, primaryKey, indexes, foreignKeys);
    }

    private static List<IndexColumn> indexColumns(Connection db, String index) throws SQLException {
        List<IndexColumn> columns = new ArrayList<>();
        try (PreparedStatement statement = db.prepareStatement(
                "SELECT name, desc, coll, key FROM pragma_index_xinfo(?) WHERE key = 1 ORDER BY seqno")) {
            statement.setString(1, index);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    columns.add(new IndexColumn(rows.getString(1), rows.getInt(2) == 0));
                }
            }
        }
        return columns;
    }

    private static List<ForeignKey> foreignKeys(Connection db, String table) throws SQLException {
        TreeMap<Integer, ForeignKey> byId = new TreeMap<>();
        try (PreparedStatement statement = db.prepareStatement("SELECT id, seq, \"table\", \"from\", \"to\","
                + " on_update, on_delete FROM pragma_foreign_key_list(?) ORDER BY id, seq")) {
            statement.setString(1, table);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    int id = rows.getInt(1);
                    ForeignKey key = byId.get(id);
                    if (key == null) {
                        key = new ForeignKey(
                                id,
                                rows.getString(3),
                                new ArrayList<>(),
                                new ArrayList<>(),
                                rows.getString(6),
                                rows.getString(7));
                        byId.put(id, key);
                    }
                    key.childColumns().add(rows.getString(4));
                    key.parentColumns().add(rows.getString(5));
                }
            }
        }
        return List.copyOf(byId.values());
    }

    /** @param statements every {@code sqlite_schema} statement, keyed {@code "<type> <name>"} */
    public record Schema(TreeMap<String, Table> tables, TreeMap<String, String> statements) {

        public Optional<Table> table(String name) {
            return Optional.ofNullable(tables.get(name));
        }

        public List<String> tableNames() {
            return List.copyOf(tables.keySet());
        }
    }

    /** @param primaryKey the key columns in key order, with their direction */
    public record Table(
            String name,
            String sql,
            List<Column> columns,
            List<IndexColumn> primaryKey,
            List<Index> indexes,
            List<ForeignKey> foreignKeys) {

        public Table {
            columns = List.copyOf(columns);
            primaryKey = List.copyOf(primaryKey);
            indexes = List.copyOf(indexes);
            foreignKeys = List.copyOf(foreignKeys);
        }

        public Optional<Column> column(String columnName) {
            return columns.stream()
                    .filter(c -> c.name().equalsIgnoreCase(columnName))
                    .findFirst();
        }
    }

    /**
     * @param primaryKeyPosition 1-based position in the primary key, 0 when the column isn't part of it
     * @param hidden 0 for an ordinary column; other values mark generated and hidden columns
     */
    public record Column(
            String name,
            String declaredType,
            boolean notNull,
            String defaultValue,
            int primaryKeyPosition,
            int hidden) {}

    /** @param origin {@code c} for {@code CREATE INDEX}, {@code u} for a UNIQUE constraint's index */
    public record Index(
            String name, boolean unique, boolean partial, String origin, List<IndexColumn> columns, String sql) {

        public Index {
            columns = List.copyOf(columns);
        }

        /** The {@code WHERE} clause of a partial index, as it stands in the {@code CREATE INDEX} statement. */
        public Optional<String> where() {
            if (!partial || sql == null) {
                return Optional.empty();
            }
            int at = sql.lastIndexOf(" WHERE ");
            return at < 0 ? Optional.empty() : Optional.of(sql.substring(at + " WHERE ".length()));
        }
    }

    public record ForeignKey(
            int id,
            String parentTable,
            List<String> childColumns,
            List<String> parentColumns,
            String onUpdate,
            String onDelete) {}
}
