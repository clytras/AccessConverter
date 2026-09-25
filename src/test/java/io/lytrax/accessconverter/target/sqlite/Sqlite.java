package io.lytrax.accessconverter.target.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Test helper: reads a converted SQLite file back, and dumps it as text that two runs can be compared by. */
public final class Sqlite implements AutoCloseable {
    private final Connection db;

    private Sqlite(Connection db) {
        this.db = db;
    }

    public static Sqlite open(Path file) {
        try {
            return new Sqlite(DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath()));
        } catch (SQLException e) {
            throw new IllegalStateException("opening " + file, e);
        }
    }

    public Connection connection() {
        return db;
    }

    /** One value, as SQLite's own types (Integer, Double, String, byte[] or null). */
    public Object value(String sql) {
        List<List<Object>> rows = query(sql);
        assertThat(rows).as(sql).hasSize(1);
        assertThat(rows.get(0)).as(sql).hasSize(1);
        return rows.get(0).get(0);
    }

    public List<List<Object>> query(String sql) {
        try (Statement statement = db.createStatement();
                ResultSet rows = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            int columns = rows.getMetaData().getColumnCount();
            while (rows.next()) {
                List<Object> row = new ArrayList<>(columns);
                for (int i = 1; i <= columns; i++) {
                    row.add(rows.getObject(i));
                }
                result.add(row);
            }
            return result;
        } catch (SQLException e) {
            throw new IllegalStateException(sql, e);
        }
    }

    /** The first column of every row, as text. */
    public List<String> strings(String sql) {
        return query(sql).stream()
                .map(row -> row.get(0) == null ? null : String.valueOf(row.get(0)))
                .toList();
    }

    public void execute(String sql) {
        try (Statement statement = db.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(sql, e);
        }
    }

    /** The CREATE statement SQLite stored for an object. */
    public String sql(String name) {
        List<String> found = strings("SELECT sql FROM sqlite_schema WHERE name = '" + name.replace("'", "''") + "'");
        assertThat(found).as("object " + name).hasSize(1);
        return found.get(0);
    }

    /** {@code PRAGMA integrity_check} must say {@code ok} and {@code foreign_key_check} must find nothing (06). */
    public void assertIsConsistent() {
        assertThat(strings("PRAGMA integrity_check")).containsExactly("ok");
        execute("PRAGMA foreign_keys = ON");
        assertThat(query("PRAGMA foreign_key_check")).isEmpty();
        execute("PRAGMA foreign_keys = OFF");
    }

    @Override
    public void close() {
        try {
            db.close();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * The whole database as text: every statement SQLite stored, then every row as SQLite's own {@code quote()}
     * renders it, which is what {@code sqlite3 .dump} writes. SQLite's internal tables are left out, except
     * {@code sqlite_sequence}, whose contents are part of the conversion; the {@code ANALYZE} statistics are not,
     * and they would tie the golden files to one SQLite version.
     */
    public static String dump(Path file) {
        return dump(file, true);
    }

    /** The statements alone, for databases whose data is too large to keep as a golden file. */
    public static String schema(Path file) {
        return dump(file, false);
    }

    private static String dump(Path file, boolean withData) {
        StringBuilder text = new StringBuilder();
        try (Sqlite sqlite = open(file)) {
            List<List<Object>> objects =
                    sqlite.query("SELECT type, name, sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY rowid");
            for (List<Object> object : objects) {
                String name = String.valueOf(object.get(1));
                if (isInternal(name)) {
                    continue;
                }
                text.append(object.get(2)).append(";\n");
            }
            for (List<Object> object : objects) {
                String name = String.valueOf(object.get(1));
                if (!withData || !"table".equals(object.get(0)) || isInternal(name)) {
                    continue;
                }
                sqlite.rows(text, name);
            }
        }
        return text.toString();
    }

    /** Whether SQLite keeps this object for itself; {@code sqlite_sequence} holds our autonumber seeds. */
    private static boolean isInternal(String name) {
        return name.startsWith("sqlite_") && !name.equals("sqlite_sequence");
    }

    private void rows(StringBuilder text, String table) {
        String quoted = "\"" + table.replace("\"", "\"\"") + "\"";
        List<String> columns = strings("SELECT name FROM pragma_table_info('" + table.replace("'", "''") + "')");
        String values = String.join(
                " || ', ' || ",
                columns.stream()
                        .map(c -> "quote(\"" + c.replace("\"", "\"\"") + "\")")
                        .toList());
        for (String row : strings("SELECT " + values + " FROM " + quoted + " ORDER BY _rowid_")) {
            text.append("INSERT INTO ")
                    .append(quoted)
                    .append(" VALUES(")
                    .append(row)
                    .append(");\n");
        }
    }

    /** Runs a real {@code sqlite3} binary's {@code .dump}, when {@code -Dsqlite3.cli=<path>} names one. */
    public static String dumpWithCli(Path file) {
        String cli = System.getProperty("sqlite3.cli");
        if (cli == null || cli.isBlank()) {
            return null;
        }
        try {
            Process process = new ProcessBuilder(cli, file.toAbsolutePath().toString(), ".dump")
                    .redirectErrorStream(true)
                    .start();
            String output =
                    new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            int code = process.waitFor();
            assertThat(code).as("sqlite3 .dump exit code, output: " + output).isZero();
            return output;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
