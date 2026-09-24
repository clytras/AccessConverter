package com.lytrax.accessconverter.target.mysql;

import static com.lytrax.accessconverter.target.mysql.MySqlLiterals.identifier;

import com.lytrax.accessconverter.report.ConversionReport.TableResult;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.RowStream;
import com.lytrax.accessconverter.target.AtomicOutput;
import com.lytrax.accessconverter.target.BinaryCells;
import com.lytrax.accessconverter.target.BinaryFiles;
import com.lytrax.accessconverter.target.BinaryMode;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.RowSource;
import com.lytrax.accessconverter.target.TableFailure;
import com.lytrax.accessconverter.target.WriteOutcome;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedColumn;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedForeignKey;
import com.lytrax.accessconverter.target.mysql.MySqlPlan.PlannedTable;
import com.lytrax.accessconverter.value.CanonicalText;
import com.lytrax.accessconverter.value.ComplexRef;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Writes the dump a {@link MySqlPlan} describes, in the order 05 prescribes: the session settings, every table, the
 * data one transaction per table, then the secondary indexes, the CHECKs, and the foreign keys with checks on.
 *
 * <p>Every statement is complete in memory before it is written, so an error while encoding a row can never leave
 * half a statement in the file (F-14, F-19). The dump is built as {@code <output>.partial} and renamed into place
 * only when it is complete.
 */
public final class MySqlDumpWriter {

    /**
     * The dump's session mode. Strict, so a remaining mistake is an error and not a silent truncation (F-03); without
     * {@code NO_BACKSLASH_ESCAPES}, which the string escaping relies on; and with the three modes MySQL 8 expects
     * beside strict mode, whose absence is warning 3135 on every import (measured on 8.0 and 8.4).
     */
    public static final String SQL_MODE = "STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
            + "NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION";

    /** MariaDB's default {@code max_allowed_packet} (MySQL 8's is 64 MiB), which the {@code mysql} client shares. */
    static final long PACKET_LIMIT = 16L << 20;

    private static final int WRITE_BUFFER = 1 << 16;

    private final RowSource source;
    private final MySqlPlan plan;
    private final ConvertOptions options;
    private final MySqlOptions mysql;
    private final String producer;
    private final Issues issues;
    private final List<TableResult> results = new ArrayList<>();
    private boolean tableFailed;
    private BinaryFiles files;
    private final java.util.Set<String> failedTables = new java.util.TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    private MySqlDumpWriter(
            RowSource source,
            MySqlPlan plan,
            ConvertOptions options,
            MySqlOptions mysql,
            String producer,
            Issues issues) {
        this.source = source;
        this.plan = plan;
        this.options = options;
        this.mysql = mysql;
        this.producer = producer;
        this.issues = issues;
    }

    /** @param producer the tool and its version, for the header ({@code AccessConverter 3.0.0}) */
    public static WriteOutcome write(
            AccessSource source,
            MySqlPlan plan,
            Path output,
            ConvertOptions options,
            MySqlOptions mysql,
            String producer,
            Issues issues)
            throws IOException {
        return write(source::rows, plan, output, options, mysql, producer, issues);
    }

    /** As {@link #write}, reading the rows from somewhere else; for the {@code --on-table-error} test. */
    static WriteOutcome write(
            RowSource source,
            MySqlPlan plan,
            Path output,
            ConvertOptions options,
            MySqlOptions mysql,
            String producer,
            Issues issues)
            throws IOException {
        MySqlDumpWriter writer = new MySqlDumpWriter(source, plan, options, mysql, producer, issues);
        writer.files = options.binary() == BinaryMode.FILES ? new BinaryFiles(output) : null;
        try {
            AtomicOutput.write(output, partial -> {
                try (Writer out = new BufferedWriter(
                        new OutputStreamWriter(Files.newOutputStream(partial), StandardCharsets.UTF_8.newEncoder()),
                        WRITE_BUFFER)) {
                    writer.dump(out);
                }
                return null;
            });
        } catch (IOException | RuntimeException e) {
            if (writer.files != null) {
                writer.files.discard();
            }
            throw e;
        }
        if (writer.files != null) {
            writer.files.commit();
        }
        return new WriteOutcome(writer.results, writer.tableFailed);
    }

    private void dump(Writer out) throws IOException {
        header(out);
        section(out, "1. Tables");
        for (PlannedTable table : plan.tables()) {
            statement(out, table.createTableSql());
        }
        section(out, "2. Data: one transaction per table");
        for (PlannedTable table : plan.tables()) {
            data(out, table);
        }
        section(out, "3. Secondary indexes");
        for (PlannedTable table : plan.tables()) {
            if (table.indexSql() != null) {
                statement(out, table.indexSql());
            }
        }
        section(out, "4. Checks");
        for (PlannedTable table : plan.tables()) {
            if (table.checkSql() != null) {
                statement(out, table.checkSql());
            }
        }
        section(out, "5. Foreign keys, validated by the server");
        out.write("SET SESSION foreign_key_checks = 1;\n");
        for (PlannedTable table : plan.tables()) {
            List<PlannedForeignKey> kept = foreignKeys(table);
            if (kept.size() == table.foreignKeys().size()) {
                if (table.foreignKeySql() != null) {
                    statement(out, table.foreignKeySql());
                }
            } else if (!kept.isEmpty()) {
                statement(out, MySqlPlanner.foreignKeys(table.name(), kept));
            }
        }
        out.write("\nSET SESSION sql_mode = @ac_sql_mode, foreign_key_checks = @ac_fk, autocommit = @ac_ac;\n");
    }

    /**
     * A table's foreign keys, less those whose parent table failed under {@code --on-table-error continue}: that
     * table is empty in the dump, so the server would reject the key for every child row.
     */
    private List<PlannedForeignKey> foreignKeys(PlannedTable table) {
        List<PlannedForeignKey> kept = new ArrayList<>();
        for (PlannedForeignKey fk : table.foreignKeys()) {
            if (failedTables.contains(fk.parentTable())) {
                issues.add(
                        IssueCode.FOREIGN_KEY_CHECK_FAILED,
                        table.name(),
                        fk.name(),
                        "the foreign key is left out of the dump: its parent table " + fk.parentTable()
                                + " failed and is empty, so the server would reject the child rows");
            } else {
                kept.add(fk);
            }
        }
        return kept;
    }

    private void header(Writer out) throws IOException {
        out.write(
                "-- " + producer + " - source: " + oneLine(plan.model().source().fileName()) + " ("
                        + plan.model().source().fileFormat() + ")\n");
        out.write("-- Target: " + plan.dialect().baseline() + ", collation " + plan.collation() + "\n");
        if (mysql.stamp()) {
            out.write("-- Generated: " + Instant.now().truncatedTo(ChronoUnit.SECONDS) + "\n");
        }
        out.write("\nSET NAMES utf8mb4;\n");
        out.write("SET @ac_sql_mode = @@SESSION.sql_mode, @ac_fk = @@SESSION.foreign_key_checks,"
                + " @ac_ac = @@SESSION.autocommit;\n");
        out.write("SET SESSION sql_mode = '" + SQL_MODE + "';\n");
        out.write("SET SESSION foreign_key_checks = 0, autocommit = 0;\n");
        if (mysql.database() == null && !mysql.dropExisting()) {
            return;
        }
        // "Already exists" and "unknown table" are notes, which would count as warnings on a first import
        out.write("SET @ac_notes = @@SESSION.sql_notes, SESSION sql_notes = 0;\n");
        if (mysql.database() != null) {
            out.write("CREATE DATABASE IF NOT EXISTS " + identifier(mysql.database())
                    + " CHARACTER SET utf8mb4 COLLATE " + plan.collation() + ";\n");
            out.write("USE " + identifier(mysql.database()) + ";\n");
        }
        if (mysql.dropExisting()) {
            for (PlannedTable table : plan.tables()) {
                out.write("DROP TABLE IF EXISTS " + identifier(table.name()) + ";\n");
            }
        }
        out.write("SET SESSION sql_notes = @ac_notes;\n");
    }

    private static void section(Writer out, String title) throws IOException {
        out.write("\n-- " + title + "\n\n");
    }

    private static void statement(Writer out, String sql) throws IOException {
        out.write(sql);
        out.write(";\n");
    }

    private static String oneLine(String text) {
        return text.replaceAll("[\\r\\n]+", " ");
    }

    // ---------------------------------------------------------------- data

    /**
     * One table's rows, in multi-row INSERTs capped by {@code --batch-bytes} and {@code --batch-rows}, then COMMIT. A
     * table that fails is rolled back in the dump itself, so under {@code --on-table-error continue} it loads empty
     * rather than half-filled.
     */
    private void data(Writer out, PlannedTable table) throws IOException {
        long read = 0;
        long written = 0;
        try {
            Batches batches = new Batches(out, table);
            BinaryCells cells = new BinaryCells(options, files, issues, table.source(), table.name());
            RowStream rows = source.of(table.source());
            while (rows.hasNext()) {
                Object[] row = rows.next();
                read++;
                batches.add(row(table, cells, row, read));
            }
            batches.flush();
            written = read;
            out.write("COMMIT;\n");
            batches.reportPacket();
        } catch (IOException | RuntimeException e) {
            tableFailed = true;
            failedTables.add(table.name());
            out.write("ROLLBACK;\n");
            if (files != null) {
                files.discardTable(table.name(), issues);
            }
            TableFailure.handle(issues, options, table.name(), 0, e);
        }
        results.add(new TableResult(table.name(), read, written));
    }

    /** The INSERT statements of one table: each is built whole, then written. */
    private final class Batches {
        private final Writer out;
        private final PlannedTable table;
        private final String head;
        private final long headBytes;
        private final StringBuilder statement = new StringBuilder();
        private long statementBytes;
        private int statementRows;
        private long largest;

        Batches(Writer out, PlannedTable table) {
            this.out = out;
            this.table = table;
            this.head = "INSERT INTO " + identifier(table.name()) + " ("
                    + table.columns().stream().map(c -> identifier(c.name())).collect(Collectors.joining(", "))
                    + ") VALUES\n";
            this.headBytes = MySqlLiterals.utf8Length(head);
        }

        void add(CharSequence tuple) throws IOException {
            long tupleBytes = MySqlLiterals.utf8Length(tuple);
            if (statementRows > 0
                    && (statementRows == options.batchRows()
                            || statementBytes + 2 + tupleBytes + 2 > mysql.batchBytes())) {
                flush();
            }
            if (statementRows == 0) {
                statement.append(head);
                statementBytes = headBytes;
            } else {
                statement.append(",\n");
                statementBytes += 2;
            }
            statement.append(tuple);
            statementBytes += tupleBytes;
            statementRows++;
        }

        void flush() throws IOException {
            if (statementRows == 0) {
                return;
            }
            statement.append(";\n");
            statementBytes += 2;
            largest = Math.max(largest, statementBytes);
            out.append(statement);
            statement.setLength(0);
            statementRows = 0;
        }

        /** A statement over the packet limit loads only once the server (and the client) allow it. */
        void reportPacket() {
            if (largest > PACKET_LIMIT) {
                issues.add(
                        IssueCode.STATEMENT_EXCEEDS_PACKET,
                        table.name(),
                        null,
                        "an INSERT of " + largest + " bytes is over MariaDB's default max_allowed_packet of 16 MiB"
                                + " (MySQL 8's is 64 MiB): raise max_allowed_packet to at least " + largest
                                + " on the server and the client, or convert with --binary files, which keeps binary"
                                + " values out of the dump");
            }
        }
    }

    /** One row as {@code (v1, v2, …)}. Nothing is substituted: a NULL stays NULL. */
    private StringBuilder row(PlannedTable table, BinaryCells cells, Object[] row, long ordinal) throws IOException {
        StringBuilder tuple = new StringBuilder(64).append('(');
        List<PlannedColumn> columns = table.columns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                tuple.append(", ");
            }
            PlannedColumn column = columns.get(i);
            Object value = column.olePart() != null || BinaryCells.isPayload(column.source())
                    ? cells.value(column.source(), column.olePart(), column.sourceIndex(), column.name(), row, ordinal)
                    : row[column.sourceIndex()];
            value(tuple, table, column, value);
        }
        return tuple.append(')');
    }

    private void value(StringBuilder out, PlannedTable table, PlannedColumn column, Object value) {
        if (value == null) {
            if (column.notNull()) {
                throw new IllegalStateException("column " + table.name() + "." + column.name()
                        + " is NOT NULL because Access requires a value, but a row holds NULL");
            }
            out.append("NULL");
            return;
        }
        switch (column.form()) {
            case BOOLEAN -> out.append((Boolean) value ? '1' : '0');
            case INTEGER -> out.append(((Number) value).longValue());
            case DECIMAL -> out.append(MySqlLiterals.decimal((BigDecimal) value));
            case FLOAT -> {
                float f = (Float) value;
                if (finite(table, column, f)) {
                    out.append(MySqlLiterals.floatValue(f));
                } else {
                    out.append("NULL");
                }
            }
            case DOUBLE -> {
                double d = (Double) value;
                if (finite(table, column, d)) {
                    out.append(MySqlLiterals.doubleValue(d));
                } else {
                    out.append("NULL");
                }
            }
            case DATETIME -> out.append(date(table, column, (LocalDateTime) value));
            case TEXT -> MySqlLiterals.string(out, text(table, column, (String) value));
            case BYTES -> MySqlLiterals.bytes(out, BinaryCells.bytes(value));
            case PATH -> MySqlLiterals.string(out, (String) value);
            case SIZE -> out.append((long) (Long) value);
            case COMPLEX_ID -> out.append(((ComplexRef) value).complexId());
        }
    }

    /** No target stores NaN or ±Infinity portably: written as NULL and reported. */
    private boolean finite(PlannedTable table, PlannedColumn column, double value) {
        if (Double.isFinite(value)) {
            return true;
        }
        issues.add(
                IssueCode.DOUBLE_NON_FINITE,
                table.name(),
                column.name(),
                "written as NULL: MySQL can't store " + value,
                String.valueOf(value));
        return false;
    }

    private String date(PlannedTable table, PlannedColumn column, LocalDateTime value) {
        if (!MySqlLiterals.fitsDateTime(value)) {
            // Access's own dates end in 9999; Money files use later ones as markers
            if (column.notNull()) {
                throw new IllegalStateException("column " + table.name() + "." + column.name() + " is NOT NULL, but "
                        + CanonicalText.dateTime(value) + " is outside DATETIME's range");
            }
            issues.add(
                    IssueCode.VALUE_OUT_OF_RANGE,
                    table.name(),
                    column.name(),
                    "written as NULL: DATETIME holds the years 0000 to 9999, and some values are outside them",
                    CanonicalText.dateTime(value));
            return "NULL";
        }
        LocalDateTime written = MySqlLiterals.atPrecision(value, column.fractionDigits());
        if (!written.equals(value)) {
            issues.add(
                    IssueCode.VALUE_PRECISION_REDUCED,
                    table.name(),
                    column.name(),
                    "written with " + column.fractionDigits() + " fractional-second digits, rounded as MySQL rounds;"
                            + " some values have more",
                    CanonicalText.dateTime(value));
        }
        return MySqlLiterals.dateTime(written, column.fractionDigits());
    }

    /** Text as it is, except a lone surrogate, which has no UTF-8 form: U+FFFD and reported. */
    private String text(PlannedTable table, PlannedColumn column, String value) {
        if (!MySqlLiterals.hasUnpairedSurrogate(value)) {
            return value;
        }
        issues.add(
                IssueCode.TEXT_UNPAIRED_SURROGATE,
                table.name(),
                column.name(),
                "an unpaired UTF-16 surrogate is written as U+FFFD: utf8mb4 (like any UTF-8) can't hold it");
        return MySqlLiterals.replaceUnpairedSurrogates(value);
    }
}
