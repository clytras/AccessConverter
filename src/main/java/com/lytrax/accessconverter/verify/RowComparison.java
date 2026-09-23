package com.lytrax.accessconverter.verify;

import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.source.RowStream;
import com.lytrax.accessconverter.value.CanonicalText;
import com.lytrax.accessconverter.verify.VerifyResult.Collector;
import com.lytrax.accessconverter.verify.VerifyResult.Difference;
import com.lytrax.accessconverter.verify.VerifyResult.TableRows;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

/**
 * Compares a table's source rows with the rows read back from an output (09, step 4), value by value through
 * {@link ValueComparator}: in the same order, or, when the output can't give them back in Access's order, as two
 * multisets of rows.
 */
public final class RowComparison {

    /**
     * A compared column.
     *
     * @param sourceIndex where its value sits in a row of the source stream
     * @param fractionDigits date/time: the fractional-second digits the output keeps
     * @param stored what the output holds for a canonical value it can't store as it is (after
     *     {@link #expectedValue}): a MySQL {@code DATETIME} has no year 10000
     */
    public record Column(
            String name, AccessType type, int sourceIndex, int fractionDigits, UnaryOperator<Object> stored) {

        public Column(String name, AccessType type, int sourceIndex, int fractionDigits) {
            this(name, type, sourceIndex, fractionDigits, UnaryOperator.identity());
        }

        Object expected(Object[] row) {
            return stored.apply(expectedValue(row[sourceIndex]));
        }
    }

    /** The output's rows, read one at a time. */
    public interface OutputRows {
        boolean next() throws SQLException;

        /** The value of the {@code column}-th compared column (from 0) of the current row. */
        Object value(int column) throws SQLException;
    }

    private RowComparison() {}

    /** Walks both sides together; a row only one side has is a difference, and so is every unequal value. */
    public static TableRows ordered(
            String table, List<Column> columns, RowStream expected, OutputRows actual, Collector differences)
            throws SQLException {
        long expectedRows = 0;
        long actualRows = 0;
        boolean hasActual = actual.next();
        while (expected.hasNext() || hasActual) {
            if (!expected.hasNext()) {
                actualRows++;
                differences.add(new Difference(table, null, "row " + actualRows, "no row", "a row"));
                hasActual = actual.next();
                continue;
            }
            Object[] row = expected.next();
            expectedRows++;
            if (!hasActual) {
                differences.add(new Difference(table, null, "row " + expectedRows, "a row", "no row"));
                continue;
            }
            actualRows++;
            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                Object stored = column.expected(row);
                Object read = actual.value(i);
                if (!ValueComparator.same(column.type(), stored, read, column.fractionDigits())) {
                    differences.add(new Difference(
                            table,
                            column.name(),
                            "row " + expectedRows,
                            CanonicalText.of(stored),
                            CanonicalText.of(read)));
                }
            }
            hasActual = actual.next();
        }
        return new TableRows(table, expectedRows, actualRows);
    }

    /**
     * Compares the rows regardless of their order: every row is reduced to a digest of its normalized values
     * ({@link ValueComparator#normalized}), and the two sides must hold the same digests the same number of times. A
     * row the output lacks, or holds in addition, is a difference named by its key columns. Memory grows with the
     * number of distinct rows, so this is for tables whose order the output can't reproduce.
     *
     * @param key the positions in {@code columns} that name a row in a difference (the primary key), or all of them
     */
    public static TableRows unordered(
            String table, List<Column> columns, int[] key, RowStream expected, OutputRows actual, Collector differences)
            throws SQLException {
        Map<Long, Tally> tallies = new HashMap<>();
        long expectedRows = 0;
        long actualRows = 0;
        Object[] values = new Object[columns.size()];
        while (expected.hasNext()) {
            Object[] row = expected.next();
            expectedRows++;
            for (int i = 0; i < columns.size(); i++) {
                values[i] = columns.get(i).expected(row);
            }
            tallies.computeIfAbsent(digest(columns, values), d -> new Tally(describe(columns, key, values))).count++;
        }
        List<String> extra = new ArrayList<>();
        long extraRows = 0;
        while (actual.next()) {
            actualRows++;
            for (int i = 0; i < columns.size(); i++) {
                values[i] = actual.value(i);
            }
            Tally tally = tallies.get(digest(columns, values));
            if (tally == null || tally.count == 0) {
                extraRows++;
                if (extra.size() < VerifyResult.MAX_DIFFERENCES) {
                    extra.add(describe(columns, key, values));
                }
            } else {
                tally.count--;
            }
        }
        for (Tally tally : tallies.values()) {
            for (long n = 0; n < tally.count; n++) {
                differences.add(new Difference(table, null, "the row " + tally.sample, "present", "missing"));
            }
        }
        for (String row : extra) {
            differences.add(new Difference(table, null, "the row " + row, "absent", "present"));
        }
        for (long n = extra.size(); n < extraRows; n++) {
            differences.add(new Difference(table, null, "a row", "absent", "present"));
        }
        return new TableRows(table, expectedRows, actualRows);
    }

    private static final class Tally {
        final String sample;
        long count;

        Tally(String sample) {
            this.sample = sample;
        }
    }

    private static long digest(List<Column> columns, Object[] values) {
        MessageDigest sha;
        try {
            sha = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            String normalized = ValueComparator.normalized(column.type(), values[i], column.fractionDigits());
            byte[] bytes = normalized == null ? new byte[0] : normalized.getBytes(StandardCharsets.UTF_8);
            sha.update(ByteBuffer.allocate(5)
                    .putInt(bytes.length)
                    .put((byte) (normalized == null ? 0 : 1))
                    .array());
            sha.update(bytes);
        }
        return ByteBuffer.wrap(sha.digest()).getLong();
    }

    private static String describe(List<Column> columns, int[] key, Object[] values) {
        StringBuilder text = new StringBuilder("(");
        for (int i = 0; i < key.length; i++) {
            text.append(i == 0 ? "" : ", ").append(CanonicalText.of(values[key[i]]));
        }
        return text.append(')').toString();
    }

    /** What an output holds for a canonical value: the value itself, except where no target can store it. */
    public static Object expectedValue(Object canonical) {
        if (canonical instanceof Double d && !Double.isFinite(d)) {
            return null; // written as NULL and reported (DOUBLE_NON_FINITE)
        }
        if (canonical instanceof Float f && !Float.isFinite(f)) {
            return null;
        }
        return canonical;
    }
}
