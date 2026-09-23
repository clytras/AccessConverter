package com.lytrax.accessconverter.target.sqlite;

import static com.lytrax.accessconverter.target.IdentifierPolicy.quote;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.report.ConversionReport.TableResult;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.RowStream;
import com.lytrax.accessconverter.source.SourceException;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.ConvertOptions.OnTableError;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedColumn;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedForeignKey;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedIndex;
import com.lytrax.accessconverter.target.sqlite.SqlitePlan.PlannedTable;
import com.lytrax.accessconverter.value.CanonicalText;
import com.lytrax.accessconverter.value.ComplexRef;
import com.lytrax.accessconverter.value.OleValue;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Writes the SQLite file a {@link SqlitePlan} describes, in the order 06 prescribes: the whole schema, then the data
 * one table per transaction, then the secondary indexes, the sequence seeds and the checks.
 *
 * <p>The file is built as {@code <output>.partial} with durability turned off and renamed into place only when
 * everything succeeded, so a failed conversion never leaves a half-written database behind (F-42).
 */
public final class SqliteWriter {

    private final ConvertOptions options;
    private final SqliteOptions sqlite;
    private final SqlitePlan plan;
    private final AccessSource source;
    private final Issues issues;
    private final List<TableResult> results = new ArrayList<>();
    private boolean tableFailed;

    private SqliteWriter(
            AccessSource source, SqlitePlan plan, ConvertOptions options, SqliteOptions sqlite, Issues issues) {
        this.source = source;
        this.plan = plan;
        this.options = options;
        this.sqlite = sqlite;
        this.issues = issues;
    }

    /**
     * @param integrityCheck also run {@code PRAGMA integrity_check}
     * @return what each table contributed, and whether a table failed while {@code --on-table-error continue}
     */
    public static Outcome write(
            AccessSource source,
            SqlitePlan plan,
            Path output,
            ConvertOptions options,
            SqliteOptions sqlite,
            boolean integrityCheck,
            Issues issues)
            throws IOException {
        return new SqliteWriter(source, plan, options, sqlite, issues).run(output, integrityCheck);
    }

    /** @param tableFailed a table could not be written and {@code --on-table-error continue} kept the output */
    public record Outcome(List<TableResult> tables, boolean tableFailed) {
        public Outcome {
            tables = List.copyOf(tables);
        }

        public long rowsWritten() {
            return tables.stream()
                    .mapToLong(t -> t.rowsWritten() == null ? 0 : t.rowsWritten())
                    .sum();
        }
    }

    private Outcome run(Path output, boolean integrityCheck) throws IOException {
        Path partial = output.resolveSibling(output.getFileName() + ".partial");
        Files.deleteIfExists(partial);
        try {
            try (Connection db = connect(partial)) {
                build(db, integrityCheck);
            } catch (SQLException e) {
                throw new IOException("the SQLite output could not be written: " + e.getMessage(), e);
            }
            Files.move(partial, output, ATOMIC_MOVE, REPLACE_EXISTING);
        } catch (IOException | RuntimeException e) {
            try {
                Files.deleteIfExists(partial);
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        return new Outcome(results, tableFailed);
    }

    private void build(Connection db, boolean integrityCheck) throws IOException {
        try {
            // The file is new and is deleted on failure, so durability during the build is irrelevant (06)
            pragma(db, "foreign_keys = OFF");
            pragma(db, "journal_mode = OFF");
            pragma(db, "synchronous = OFF");
            pragma(db, "locking_mode = EXCLUSIVE");
            pragma(db, "temp_store = MEMORY");
            db.setAutoCommit(false);
            for (PlannedTable table : plan.tables()) {
                execute(db, table.createTableSql());
            }
            if (plan.metadata() != null) {
                SqliteMetadata.create(db, plan);
            }
            db.commit();
            for (PlannedTable table : plan.tables()) {
                data(db, table);
            }
            for (PlannedIndex index : plan.indexes()) {
                execute(db, index.createIndexSql());
            }
            db.commit();
            if (plan.metadata() != null) {
                SqliteMetadata.fill(db, plan);
                db.commit();
            }
            foreignKeyCheck(db);
            if (integrityCheck) {
                integrityCheck(db);
            }
            db.setAutoCommit(true);
            pragma(db, "journal_mode = DELETE");
            execute(db, sqlite.analyze() ? "ANALYZE" : "PRAGMA optimize");
        } catch (SQLException e) {
            throw new IOException("writing the SQLite output failed: " + e.getMessage(), e);
        }
    }

    private static Connection connect(Path file) throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath());
    }

    private static void pragma(Connection db, String pragma) throws SQLException {
        try (Statement statement = db.createStatement()) {
            statement.execute("PRAGMA " + pragma);
        }
    }

    private static void execute(Connection db, String sql) throws SQLException {
        try (Statement statement = db.createStatement()) {
            statement.execute(sql);
        }
    }

    // ---------------------------------------------------------------- data

    private void data(Connection db, PlannedTable table) throws IOException, SQLException {
        long[] counts = {0, 0};
        try {
            counts = insert(db, table);
            db.commit();
            sequence(db, table);
            db.commit();
        } catch (IOException | SQLException | RuntimeException e) {
            db.rollback();
            tableFailed = true;
            boolean reading = e instanceof IOException || e instanceof UncheckedIOException;
            issues.add(
                    reading ? IssueCode.TABLE_READ_FAILED : IssueCode.TABLE_WRITE_FAILED,
                    table.name(),
                    null,
                    (reading ? "reading" : "writing") + " the table failed after " + counts[1] + " rows: "
                            + message(e));
            if (options.onTableError() == OnTableError.FAIL) {
                throw e instanceof SourceException source
                        ? source
                        : new IOException("table " + table.name() + " failed: " + message(e), e);
            }
        }
        results.add(new TableResult(table.name(), counts[0], counts[1]));
    }

    /** @return the rows read and the rows written */
    private long[] insert(Connection db, PlannedTable table) throws SQLException, IOException {
        List<PlannedColumn> columns = table.columns();
        String sql = "INSERT INTO " + quote(table.name()) + " ("
                + columns.stream().map(c -> quote(c.name())).collect(Collectors.joining(", ")) + ") VALUES ("
                + "?, ".repeat(columns.size() - 1) + "?)";
        long read = 0;
        long written = 0;
        try (PreparedStatement statement = db.prepareStatement(sql)) {
            RowStream rows = source.rows(table.source());
            int batch = 0;
            while (rows.hasNext()) {
                Object[] row = rows.next();
                read++;
                for (int i = 0; i < columns.size(); i++) {
                    PlannedColumn column = columns.get(i);
                    bind(statement, i + 1, table, column, row[column.sourceIndex()]);
                }
                statement.addBatch();
                if (++batch == options.batchRows()) {
                    statement.executeBatch();
                    written += batch;
                    batch = 0;
                }
            }
            if (batch > 0) {
                statement.executeBatch();
                written += batch;
            }
        }
        return new long[] {read, written};
    }

    /**
     * Binds one canonical value the way its column stores it (06, Column types). Nothing is substituted: a NULL stays
     * NULL, and a value that can't be stored fails the table instead of being changed.
     */
    private void bind(PreparedStatement statement, int at, PlannedTable table, PlannedColumn column, Object value)
            throws SQLException {
        if (value == null) {
            if (column.notNull()) {
                throw new IllegalStateException("column " + table.name() + "." + column.name()
                        + " is NOT NULL because Access requires a value, but a row holds NULL");
            }
            statement.setNull(at, Types.NULL);
            return;
        }
        switch (column.form()) {
            case BOOLEAN_INT -> statement.setInt(at, (Boolean) value ? 1 : 0);
            case INTEGER -> statement.setInt(at, ((Number) value).intValue());
            case LONG -> statement.setLong(at, ((Number) value).longValue());
            case REAL -> real(statement, at, table, column, (Double) value);
            case REAL_FROM_FLOAT ->
                // Float.toString is the shortest text that round-trips, so 2583.2092f stays 2583.2092 (F-07)
                real(statement, at, table, column, Double.parseDouble(Float.toString((Float) value)));
            case DECIMAL_NUMBER, DECIMAL_TEXT -> statement.setString(at, ((BigDecimal) value).toPlainString());
            case DATE_TEXT -> date(statement, at, table, column, (LocalDateTime) value);
            case TEXT -> statement.setString(at, (String) value);
            case BLOB -> statement.setBytes(at, value instanceof OleValue ole ? ole.raw() : (byte[]) value);
            case COMPLEX_ID -> statement.setInt(at, ((ComplexRef) value).complexId());
        }
    }

    private void real(PreparedStatement statement, int at, PlannedTable table, PlannedColumn column, double value)
            throws SQLException {
        if (!Double.isFinite(value)) {
            // No target stores NaN or ±Infinity portably; SQLite has no way to hold them at all
            issues.add(
                    IssueCode.DOUBLE_NON_FINITE,
                    table.name(),
                    column.name(),
                    "written as NULL: SQLite can't store " + value,
                    String.valueOf(value));
            statement.setNull(at, Types.NULL);
            return;
        }
        statement.setDouble(at, value);
    }

    private void date(
            PreparedStatement statement, int at, PlannedTable table, PlannedColumn column, LocalDateTime value)
            throws SQLException {
        if (SqliteValues.losesFraction(value, column.fractionDigits())) {
            issues.add(
                    IssueCode.VALUE_PRECISION_REDUCED,
                    table.name(),
                    column.name(),
                    "written with " + column.fractionDigits() + " fractional-second digits, which drops part of some"
                            + " values",
                    CanonicalText.dateTime(value));
        }
        statement.setString(at, SqliteValues.dateTime(value, column.fractionDigits()));
    }

    /** Continues Access's autonumber sequence, so generated ids never repeat one Access already used. */
    private void sequence(Connection db, PlannedTable table) throws SQLException {
        if (table.autoIncrementColumn() == null || table.sequenceSeed() == null) {
            return;
        }
        long seed = table.sequenceSeed();
        Long current = null;
        try (PreparedStatement select = db.prepareStatement("SELECT seq FROM sqlite_sequence WHERE name = ?")) {
            select.setString(1, table.name());
            try (ResultSet rows = select.executeQuery()) {
                if (rows.next()) {
                    current = rows.getLong(1);
                }
            }
        }
        if (current == null) {
            try (PreparedStatement insert =
                    db.prepareStatement("INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)")) {
                insert.setString(1, table.name());
                insert.setLong(2, seed);
                insert.executeUpdate();
            }
        } else if (current < seed) {
            try (PreparedStatement update = db.prepareStatement("UPDATE sqlite_sequence SET seq = ? WHERE name = ?")) {
                update.setLong(1, seed);
                update.setString(2, table.name());
                update.executeUpdate();
            }
        }
    }

    // ---------------------------------------------------------------- checks

    /**
     * The profiler already proved there are no orphans, so a violation here is a bug in the converter and fails the
     * conversion (06, step 6). When a table failed under {@code --on-table-error continue}, its missing rows explain
     * the violation and it is only reported.
     */
    private void foreignKeyCheck(Connection db) throws SQLException, IOException {
        long keys =
                plan.tables().stream().mapToLong(t -> t.foreignKeys().size()).sum();
        if (keys == 0) {
            return;
        }
        // A SQLite file can't switch enforcement on for its readers, so the report says who has to (06)
        issues.add(
                IssueCode.FOREIGN_KEYS_NEED_PRAGMA,
                null,
                null,
                keys + " foreign keys are in the file, but SQLite only enforces them for a connection that runs"
                        + " PRAGMA foreign_keys = ON; that is SQLite's behavior, not something the file can change");
        List<String> violations = new ArrayList<>();
        long count = 0;
        pragma(db, "foreign_keys = ON");
        try (Statement statement = db.createStatement();
                ResultSet rows = statement.executeQuery("PRAGMA foreign_key_check")) {
            while (rows.next()) {
                count++;
                if (violations.size() < Issues.MAX_SAMPLES) {
                    violations.add(describe(rows.getString(1), rows.getString(3), rows.getInt(4)));
                }
            }
        } finally {
            pragma(db, "foreign_keys = OFF");
        }
        if (count == 0) {
            return;
        }
        String message = count + " foreign key violations in the output: " + String.join("; ", violations);
        issues.add(IssueCode.FOREIGN_KEY_CHECK_FAILED, null, null, message);
        if (!tableFailed) {
            throw new IOException(message + ". The profiler found no orphans, so this is a bug in the converter");
        }
    }

    /** Names the relationship behind a {@code foreign_key_check} row, which reports it by position. */
    private String describe(String child, String parent, int fkId) {
        PlannedTable table = plan.table(child).orElse(null);
        if (table != null) {
            // SQLite numbers a table's foreign keys in reverse declaration order
            List<PlannedForeignKey> keys = table.foreignKeys();
            int index = keys.size() - 1 - fkId;
            if (index >= 0 && index < keys.size()) {
                ForeignKeyModel fk = keys.get(index).source();
                return child + " -> " + parent + " (" + fk.name() + ")";
            }
        }
        return child + " -> " + parent;
    }

    private void integrityCheck(Connection db) throws SQLException, IOException {
        List<String> problems = new ArrayList<>();
        try (Statement statement = db.createStatement();
                ResultSet rows = statement.executeQuery("PRAGMA integrity_check")) {
            while (rows.next()) {
                String line = rows.getString(1);
                if (!"ok".equals(line)) {
                    problems.add(line);
                }
            }
        }
        if (!problems.isEmpty()) {
            throw new IOException("PRAGMA integrity_check failed: " + String.join("; ", problems));
        }
    }

    private static String message(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SourceException source) {
                return source.getMessage();
            }
        }
        return e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
    }
}
