package io.lytrax.accessconverter.target.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Reads the schema of a loaded MySQL/MariaDB database back from {@code information_schema} (09, step 2): tables,
 * columns, indexes, CHECKs and foreign keys of the connection's current database, as the server reports them.
 */
public final class MySqlIntrospector {

    private MySqlIntrospector() {}

    public record Schema(String version, Map<String, Table> tables) {
        public Optional<Table> table(String name) {
            return Optional.ofNullable(tables.get(name));
        }
    }

    public record Table(
            String name,
            String collation,
            String comment,
            List<Column> columns,
            Map<String, Index> indexes,
            List<String> checks,
            Map<String, ForeignKey> foreignKeys) {

        public Optional<Column> column(String columnName) {
            return columns.stream()
                    .filter(c -> c.name().equalsIgnoreCase(columnName))
                    .findFirst();
        }
    }

    /**
     * @param type {@code COLUMN_TYPE} as the server spells it ({@code int}, {@code int(11)}, {@code varchar(50)})
     * @param defaultValue {@code COLUMN_DEFAULT}: MySQL gives a literal's value and an expression's text, MariaDB
     *     quotes a string literal
     * @param extra {@code EXTRA}: {@code auto_increment}, {@code DEFAULT_GENERATED}
     */
    public record Column(
            String name,
            String type,
            boolean nullable,
            String defaultValue,
            String extra,
            String comment,
            String collation) {}

    public record Index(String name, boolean unique, List<Part> parts) {}

    /** @param prefix the key's prefix length, or null for the whole value */
    public record Part(String column, boolean ascending, Integer prefix) {}

    public record ForeignKey(
            String name,
            List<String> columns,
            String parentTable,
            List<String> parentColumns,
            String updateRule,
            String deleteRule) {}

    public static Schema read(Connection db) throws SQLException {
        String version = single(db, "SELECT VERSION()");
        Map<String, Table> tables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, List<Column>> columns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Map<String, Index>> indexes = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, List<String>> checks = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Map<String, ForeignKey>> foreignKeys = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        query(
                db,
                """
                SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, COLUMN_COMMENT,
                       COLLATION_NAME
                FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE()
                ORDER BY TABLE_NAME, ORDINAL_POSITION""",
                rs -> columns.computeIfAbsent(rs.getString(1), k -> new ArrayList<>())
                        .add(new Column(
                                rs.getString(2),
                                rs.getString(3),
                                "YES".equals(rs.getString(4)),
                                rs.getString(5),
                                rs.getString(6),
                                rs.getString(7),
                                rs.getString(8))));
        query(db, """
                SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, COLUMN_NAME, COLLATION, SUB_PART
                FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE()
                ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX""", rs -> {
            Map<String, Index> byName = indexes.computeIfAbsent(rs.getString(1), k -> new LinkedHashMap<>());
            Index index = byName.computeIfAbsent(
                    rs.getString(2), name -> new Index(name, safeInt(rs, 3) == 0, new ArrayList<>()));
            int length = rs.getInt(6);
            Integer prefix = rs.wasNull() ? null : length;
            index.parts().add(new Part(rs.getString(4), !"D".equals(rs.getString(5)), prefix));
        });
        query(
                db,
                """
                SELECT TABLE_NAME, CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS
                WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_TYPE = 'CHECK'
                ORDER BY TABLE_NAME, CONSTRAINT_NAME""",
                rs -> checks.computeIfAbsent(rs.getString(1), k -> new ArrayList<>())
                        .add(rs.getString(2)));
        Map<String, String[]> rules = new TreeMap<>();
        query(
                db,
                """
                SELECT TABLE_NAME, CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE, REFERENCED_TABLE_NAME
                FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE()""",
                rs -> rules.put(
                        rs.getString(1) + "\0" + rs.getString(2),
                        new String[] {rs.getString(3), rs.getString(4), rs.getString(5)}));
        query(db, """
                SELECT TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE() AND REFERENCED_TABLE_NAME IS NOT NULL
                ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION""", rs -> {
            String[] rule = rules.get(rs.getString(1) + "\0" + rs.getString(2));
            if (rule == null) {
                return;
            }
            ForeignKey fk = foreignKeys
                    .computeIfAbsent(rs.getString(1), k -> new LinkedHashMap<>())
                    .computeIfAbsent(
                            rs.getString(2),
                            name -> new ForeignKey(
                                    name, new ArrayList<>(), rule[2], new ArrayList<>(), rule[0], rule[1]));
            fk.columns().add(rs.getString(3));
            fk.parentColumns().add(rs.getString(4));
        });
        query(db, """
                SELECT TABLE_NAME, TABLE_COLLATION, TABLE_COMMENT FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'""", rs -> {
            String name = rs.getString(1);
            tables.put(
                    name,
                    new Table(
                            name,
                            rs.getString(2),
                            rs.getString(3),
                            columns.getOrDefault(name, List.of()),
                            indexes.getOrDefault(name, Map.of()),
                            checks.getOrDefault(name, List.of()),
                            foreignKeys.getOrDefault(name, Map.of())));
        });
        return new Schema(version, tables);
    }

    private interface RowHandler {
        void accept(ResultSet rs) throws SQLException;
    }

    private static void query(Connection db, String sql, RowHandler handler) throws SQLException {
        try (PreparedStatement statement = db.prepareStatement(sql);
                ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                handler.accept(rs);
            }
        }
    }

    private static String single(Connection db, String sql) throws SQLException {
        try (PreparedStatement statement = db.prepareStatement(sql);
                ResultSet rs = statement.executeQuery()) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static int safeInt(ResultSet rs, int column) {
        try {
            return rs.getInt(column);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
