package com.lytrax.accessconverter.target.mysql;

import static com.lytrax.accessconverter.target.mysql.MySqlLiterals.identifier;

import com.lytrax.accessconverter.model.ForeignKeyModel.Action;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.target.BinaryCells;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.KeyPart;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedColumn;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedForeignKey;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedIndex;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedTable;
import com.lytrax.accessconverter.verify.RowComparison;
import com.lytrax.accessconverter.verify.VerifyResult;
import com.lytrax.accessconverter.verify.VerifyResult.Collector;
import com.lytrax.accessconverter.verify.VerifyResult.Difference;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Compares a MySQL/MariaDB database, loaded from a dump, with the plan that describes it and with the source data
 * (09, The verifier). The plan is re-derived from the source, so a difference means the database doesn't hold what
 * the source says it should: every documented downgrade is already part of the plan.
 *
 * <p>InnoDB gives rows back in primary-key order, which is Access's order for number and date keys but not for text
 * keys (a different collation) nor for a table without a primary key (a unique index may become its clustered index).
 * The rows are compared in order first; if that finds differences, they are compared again regardless of order.
 */
public final class MySqlVerifier {

    private static final Pattern INTEGER_WIDTH =
            Pattern.compile("\\b(tinyint|smallint|mediumint|int|bigint)\\(\\d+\\)");

    private static final Pattern NUMBER = Pattern.compile("-?\\d+(\\.\\d+)?");

    private final AccessSource source;
    private final MySqlPlan plan;
    private final Collector differences = new Collector();
    private final Path dumpDirectory;

    private MySqlVerifier(AccessSource source, MySqlPlan plan, Path dumpDirectory) {
        this.source = source;
        this.plan = plan;
        this.dumpDirectory = dumpDirectory;
    }

    /** @param db a connection whose current database holds the imported dump */
    public static VerifyResult verify(AccessSource source, MySqlPlan plan, Connection db) throws IOException {
        return verify(source, plan, db, Path.of(""));
    }

    /**
     * As {@link #verify(AccessSource, MySqlPlan, Connection)}.
     *
     * @param dumpDirectory with {@code --binary files}: the directory the dump was written in, which the stored paths
     *     are relative to
     */
    public static VerifyResult verify(AccessSource source, MySqlPlan plan, Connection db, Path dumpDirectory)
            throws IOException {
        MySqlVerifier verifier = new MySqlVerifier(source, plan, dumpDirectory);
        try {
            verifier.run(db);
        } catch (SQLException e) {
            throw new IOException("the database could not be read back: " + e.getMessage(), e);
        }
        return verifier.differences.result();
    }

    private void run(Connection db) throws SQLException, IOException {
        MySqlIntrospector.Schema schema = MySqlIntrospector.read(db);
        Set<String> expected = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        plan.tables().forEach(t -> expected.add(t.name()));
        if (!expected.equals(new TreeSet<>(schema.tables().keySet()))) {
            differences.add(
                    null,
                    null,
                    "the set of tables",
                    String.join(", ", expected),
                    String.join(", ", schema.tables().keySet()));
        }
        for (PlannedTable table : plan.tables()) {
            MySqlIntrospector.Table actual = schema.table(table.name()).orElse(null);
            if (actual == null) {
                continue;
            }
            same(table.name(), null, "the collation", plan.collation(), actual.collation());
            same(table.name(), null, "the table comment", orEmpty(table.comment()), orEmpty(actual.comment()));
            columns(table, actual);
            indexes(table, actual);
            checks(table, actual);
            foreignKeys(table, actual);
            data(db, table);
        }
    }

    // ---------------------------------------------------------------- schema

    private void columns(PlannedTable table, MySqlIntrospector.Table actual) {
        List<String> expectedNames =
                table.columns().stream().map(PlannedColumn::name).toList();
        List<String> actualNames =
                actual.columns().stream().map(MySqlIntrospector.Column::name).toList();
        if (!expectedNames.equals(actualNames)) {
            differences.add(
                    table.name(),
                    null,
                    "the columns",
                    String.join(", ", expectedNames),
                    String.join(", ", actualNames));
            return;
        }
        for (PlannedColumn column : table.columns()) {
            MySqlIntrospector.Column found = actual.column(column.name()).orElseThrow();
            same(table.name(), column.name(), "the type", expectedType(column.type()), actualType(found.type()));
            same(table.name(), column.name(), "NOT NULL", column.notNull(), !found.nullable());
            same(
                    table.name(),
                    column.name(),
                    "AUTO_INCREMENT",
                    column.autoIncrement(),
                    found.extra() != null
                            && found.extra().toLowerCase(Locale.ROOT).contains("auto_increment"));
            same(table.name(), column.name(), "the comment", orEmpty(column.comment()), orEmpty(found.comment()));
            if (found.collation() != null) {
                String collation = column.collation() != null ? column.collation() : plan.collation();
                same(table.name(), column.name(), "the collation", collation, found.collation());
            }
            if (!sameDefault(column.defaultSql(), found.defaultValue())) {
                differences.add(
                        table.name(),
                        column.name(),
                        "the default",
                        String.valueOf(column.defaultSql()),
                        String.valueOf(found.defaultValue()));
            }
        }
    }

    /** The declared type as the server reports it: lower case, BOOLEAN as {@code tinyint(1)}. */
    static String expectedType(String declared) {
        String type = declared.toLowerCase(Locale.ROOT);
        return type.equals("boolean") ? "tinyint(1)" : type;
    }

    /** MariaDB (and MySQL before 8.0.19) show integer display widths; they mean nothing, except BOOLEAN's. */
    static String actualType(String reported) {
        String type = reported.toLowerCase(Locale.ROOT);
        return type.equals("tinyint(1)") ? type : INTEGER_WIDTH.matcher(type).replaceAll("$1");
    }

    /**
     * Whether a column's default is the planned one. MySQL reports a literal's value and an expression's text;
     * MariaDB quotes a string literal and reports a nullable column without a default as {@code NULL}. Expressions
     * are compared after the servers' own spellings are undone ({@code curdate()}, {@code _utf8mb4'…'}).
     */
    static boolean sameDefault(String planned, String reported) {
        String actual = reported == null || reported.equals("NULL") ? null : reported;
        if (planned == null || actual == null) {
            return planned == null && actual == null;
        }
        if (planned.startsWith("'")) {
            String value = unescape(planned);
            return value.equals(actual) || (actual.startsWith("'") && value.equals(unquoteReported(actual)));
        }
        if (NUMBER.matcher(planned).matches()) {
            try {
                return new BigDecimal(planned).compareTo(new BigDecimal(actual.replace("'", ""))) == 0;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return expression(planned).equals(expression(actual));
    }

    /** The text of a string literal the planner wrote ({@code 'it\'s'}). */
    static String unescape(String literal) {
        StringBuilder text = new StringBuilder();
        for (int i = 1; i < literal.length() - 1; i++) {
            char c = literal.charAt(i);
            if (c == '\\' && i + 1 < literal.length() - 1) {
                char next = literal.charAt(++i);
                text.append(
                        switch (next) {
                            case '0' -> '\0';
                            case 'b' -> '\b';
                            case 'n' -> '\n';
                            case 'r' -> '\r';
                            case 't' -> '\t';
                            case 'Z' -> '\u001A';
                            default -> next;
                        });
            } else if (c == '\'' && i + 1 < literal.length() - 1 && literal.charAt(i + 1) == '\'') {
                text.append('\'');
                i++;
            } else {
                text.append(c);
            }
        }
        return text.toString();
    }

    /** A literal default as MariaDB reports it: quoted, with {@code ''} for a quote. */
    private static String unquoteReported(String reported) {
        return unescape(reported);
    }

    /** An expression default, spelled the same way whichever server reports it. */
    static String expression(String sql) {
        String text = sql.toLowerCase(Locale.ROOT)
                .replaceAll("_(utf8mb4|utf8mb3|utf8|latin1|binary)(?=\\\\?')", "")
                .replace("\\'", "'")
                .replaceAll("\\s+", "");
        while (text.startsWith("(") && text.endsWith(")") && balanced(text.substring(1, text.length() - 1))) {
            text = text.substring(1, text.length() - 1);
        }
        text = withoutCharsetConversions(text);
        return text.replace("ucase(", "upper(")
                .replace("curdate()", "current_date")
                .replace("curtime()", "current_time")
                .replace("current_timestamp()", "current_timestamp")
                .replace("now()", "current_timestamp");
    }

    /** MySQL adds {@code convert(… using utf8mb4)} around a function result it stores in a default. */
    private static String withoutCharsetConversions(String text) {
        int start = text.indexOf("convert(");
        while (start >= 0) {
            int depth = 0;
            int end = start + "convert".length();
            for (; end < text.length(); end++) {
                depth += text.charAt(end) == '(' ? 1 : text.charAt(end) == ')' ? -1 : 0;
                if (depth == 0) {
                    break;
                }
            }
            String inner = text.substring(start + "convert(".length(), end);
            java.util.regex.Matcher using =
                    Pattern.compile("^(.*)using(utf8mb4|utf8mb3|utf8|latin1)$").matcher(inner);
            if (!using.matches()) {
                start = text.indexOf("convert(", start + 1);
                continue;
            }
            text = text.substring(0, start) + using.group(1) + text.substring(end + 1);
            start = text.indexOf("convert(");
        }
        return text;
    }

    private static boolean balanced(String text) {
        int depth = 0;
        for (char c : text.toCharArray()) {
            depth += c == '(' ? 1 : c == ')' ? -1 : 0;
            if (depth < 0) {
                return false;
            }
        }
        return depth == 0;
    }

    private void indexes(PlannedTable table, MySqlIntrospector.Table actual) {
        Map<String, MySqlIntrospector.Index> found = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        found.putAll(actual.indexes());
        MySqlIntrospector.Index primary = found.remove("PRIMARY");
        if (table.primaryKey() == null) {
            if (primary != null) {
                differences.add(table.name(), null, "the primary key", "none", describe(primary.parts()));
            }
        } else if (primary == null) {
            differences.add(table.name(), null, "the primary key", describe(table.primaryKey()), "none");
        } else {
            same(table.name(), null, "the primary key", describe(table.primaryKey()), describe(primary.parts()));
        }
        Map<String, PlannedIndex> expected = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (PlannedIndex index : table.allIndexes()) {
            if (!index.primary()) {
                expected.put(index.name(), index);
            }
        }
        if (!expected.keySet().equals(found.keySet())) {
            differences.add(
                    table.name(),
                    null,
                    "the indexes",
                    String.join(", ", expected.keySet()),
                    String.join(", ", found.keySet()));
        }
        for (PlannedIndex index : expected.values()) {
            MySqlIntrospector.Index match = found.get(index.name());
            if (match == null) {
                continue;
            }
            same(table.name(), index.name(), "UNIQUE", index.unique(), match.unique());
            same(table.name(), index.name(), "the index columns", describe(index), describe(match.parts()));
        }
    }

    private static String describe(PlannedIndex index) {
        return index.parts().stream().map(MySqlVerifier::describe).collect(Collectors.joining(", "));
    }

    private static String describe(KeyPart part) {
        return part.column()
                + (part.prefix() == null ? "" : "(" + part.prefix() + ")")
                + (part.ascending() ? "" : " DESC");
    }

    private static String describe(List<MySqlIntrospector.Part> parts) {
        return parts.stream()
                .map(p -> describe(new KeyPart(p.column(), p.ascending(), p.prefix())))
                .collect(Collectors.joining(", "));
    }

    private void checks(PlannedTable table, MySqlIntrospector.Table actual) {
        Set<String> expected = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        table.checks().forEach(c -> expected.add(c.name()));
        Set<String> found = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        found.addAll(actual.checks());
        if (!expected.equals(found)) {
            differences.add(
                    table.name(), null, "the CHECK constraints", String.join(", ", expected), String.join(", ", found));
        }
    }

    private void foreignKeys(PlannedTable table, MySqlIntrospector.Table actual) {
        Map<String, MySqlIntrospector.ForeignKey> found = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        found.putAll(actual.foreignKeys());
        Set<String> expected = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        table.foreignKeys().forEach(f -> expected.add(f.name()));
        if (!expected.equals(found.keySet())) {
            differences.add(
                    table.name(),
                    null,
                    "the foreign keys",
                    String.join(", ", expected),
                    String.join(", ", found.keySet()));
        }
        for (PlannedForeignKey fk : table.foreignKeys()) {
            MySqlIntrospector.ForeignKey match = found.get(fk.name());
            if (match == null) {
                continue;
            }
            same(
                    table.name(),
                    fk.name(),
                    "the foreign key",
                    fk.childColumns() + " -> " + fk.parentTable().toLowerCase(Locale.ROOT) + " " + fk.parentColumns(),
                    match.columns() + " -> " + match.parentTable().toLowerCase(Locale.ROOT) + " "
                            + match.parentColumns());
            same(table.name(), fk.name(), "ON UPDATE", action(fk.onUpdate()), rule(match.updateRule()));
            same(table.name(), fk.name(), "ON DELETE", action(fk.onDelete()), rule(match.deleteRule()));
        }
    }

    private static String action(Action action) {
        return switch (action) {
            case NO_ACTION -> "NO ACTION";
            case CASCADE -> "CASCADE";
            case SET_NULL -> "SET NULL";
        };
    }

    /** MariaDB reports an FK without an action as RESTRICT, which InnoDB treats exactly as NO ACTION. */
    private static String rule(String reported) {
        return "RESTRICT".equals(reported) ? "NO ACTION" : reported;
    }

    private void same(String table, String object, String what, Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            differences.add(new Difference(table, object, what, String.valueOf(expected), String.valueOf(actual)));
        }
    }

    private static String orEmpty(String text) {
        return text == null ? "" : text;
    }

    // ---------------------------------------------------------------- data

    private void data(Connection db, PlannedTable table) throws SQLException, IOException {
        List<PlannedColumn> columns = table.columns();
        ConvertOptions options = plan.options();
        List<RowComparison.Column> compared = columns.stream()
                .map(c -> new RowComparison.Column(
                        c.name(),
                        BinaryCells.comparedType(c.source(), options),
                        c.sourceIndex(),
                        c.fractionDigits(),
                        value -> stored(BinaryCells.expected(c.source(), c.olePart(), value, options)),
                        value -> BinaryCells.readBack(value, c.source(), options, dumpDirectory)))
                .toList();
        if (table.source().isComplexChild()) {
            // Its rows come in the parent's order, its key is Access's value id: compare regardless of order
            try (Statement statement = db.createStatement();
                    ResultSet rows = statement.executeQuery(select(table, false))) {
                differences.rows(RowComparison.unordered(
                        table.name(),
                        compared,
                        new int[] {0},
                        source.rows(table.source()),
                        output(rows, columns),
                        differences));
            }
            return;
        }
        Collector ordered = new Collector();
        try (Statement statement = db.createStatement();
                ResultSet rows = statement.executeQuery(select(table, true))) {
            ordered.rows(RowComparison.ordered(
                    table.name(), compared, source.rows(table.source()), output(rows, columns), ordered));
        }
        if (ordered.count() == 0 || orderIsAccessOrder(table)) {
            differences.addAll(ordered);
            return;
        }
        int[] key = table.primaryKey() == null
                ? IntStream.range(0, columns.size()).toArray()
                : table.primaryKey().parts().stream()
                        .mapToInt(p -> IntStream.range(0, columns.size())
                                .filter(i -> columns.get(i).name().equals(p.column()))
                                .findFirst()
                                .orElseThrow())
                        .toArray();
        try (Statement statement = db.createStatement();
                ResultSet rows = statement.executeQuery(select(table, false))) {
            differences.rows(RowComparison.unordered(
                    table.name(), compared, key, source.rows(table.source()), output(rows, columns), differences));
        }
    }

    /** Whether rows come back in the order Access streams them: a primary key of numbers and dates only. */
    private static boolean orderIsAccessOrder(PlannedTable table) {
        if (table.primaryKey() == null) {
            return false;
        }
        for (KeyPart part : table.primaryKey().parts()) {
            PlannedColumn column = table.columns().stream()
                    .filter(c -> c.name().equals(part.column()))
                    .findFirst()
                    .orElseThrow();
            if (column.form() == MySqlPlan.ValueForm.TEXT
                    || column.form() == MySqlPlan.ValueForm.BYTES
                    || column.form() == MySqlPlan.ValueForm.PATH) {
                return false;
            }
        }
        return true;
    }

    /**
     * The select that reads a table back. A FLOAT is read as a DOUBLE: the text protocol sends a FLOAT with six
     * significant digits, a DOUBLE with every digit it needs, and every float is exactly a double. A DATETIME is read
     * as the server's own text: MariaDB Connector/J 3.5 renders {@code 16:07:28.09300} of a {@code DATETIME(5)} as
     * {@code 16:07:28.93000} (FavDatabase).
     */
    private static String select(PlannedTable table, boolean ordered) {
        String columns = table.columns().stream()
                .map(c -> switch (c.form()) {
                    case FLOAT -> "(" + identifier(c.name()) + " + 0e0)";
                    case DATETIME -> "CAST(" + identifier(c.name()) + " AS CHAR)";
                    default -> identifier(c.name());
                })
                .collect(Collectors.joining(", "));
        String sql = "SELECT " + columns + " FROM " + identifier(table.name());
        if (ordered && table.primaryKey() != null) {
            sql += " ORDER BY "
                    + table.primaryKey().parts().stream()
                            .map(p -> identifier(p.column()) + (p.ascending() ? "" : " DESC"))
                            .collect(Collectors.joining(", "));
        }
        return sql;
    }

    private static RowComparison.OutputRows output(ResultSet rows, List<PlannedColumn> columns) {
        return new RowComparison.OutputRows() {
            @Override
            public boolean next() throws SQLException {
                return rows.next();
            }

            @Override
            public Object value(int column) throws SQLException {
                return read(rows, column + 1, columns.get(column));
            }
        };
    }

    /** A value in a driver-independent form: numbers as numbers, dates as their text, bytes as bytes. */
    private static Object read(ResultSet rows, int at, PlannedColumn column) throws SQLException {
        Object value =
                switch (column.form()) {
                    case BOOLEAN, INTEGER, SIZE, COMPLEX_ID -> rows.getLong(at);
                    case DECIMAL -> rows.getBigDecimal(at);
                    case FLOAT, DOUBLE -> rows.getDouble(at);
                    case DATETIME, TEXT, PATH -> rows.getString(at);
                    case BYTES -> rows.getBytes(at);
                };
        return rows.wasNull() ? null : value;
    }

    /** What the database holds for a value the dump had to change: a date past 9999 is NULL (VALUE_OUT_OF_RANGE). */
    private static Object stored(Object value) {
        return value instanceof LocalDateTime t && !MySqlLiterals.fitsDateTime(t) ? null : value;
    }
}
