package com.lytrax.accessconverter.verify;

import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.value.CanonicalText;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Compares a table's source rows with the rows read back from a target, both in the same order (primary-key order,
 * or ordinal when there's no key). Every difference is counted; the first {@value #MAX_DIFFERENCES} are kept.
 */
public final class TableComparison {
    public static final int MAX_DIFFERENCES = 20;

    public enum Kind {
        MISSING_ROW,
        EXTRA_ROW,
        VALUE
    }

    /** @param column null for a missing or extra row */
    public record Difference(Kind kind, String rowKey, String column, String expected, String actual) {}

    public record Result(
            String table, long expectedRows, long actualRows, long differenceCount, List<Difference> differences) {
        public Result {
            differences = List.copyOf(differences);
        }

        public boolean matches() {
            return differenceCount == 0;
        }
    }

    private TableComparison() {}

    /**
     * @param expected canonical source rows, all columns in column order
     * @param actual target rows, same columns and order
     * @param fractionDigits per column: the target's fractional-second digits for dates
     */
    public static Result compare(
            TableModel table, Iterator<Object[]> expected, Iterator<Object[]> actual, int[] fractionDigits) {
        List<ColumnModel> columns = table.columns();
        List<Difference> kept = new ArrayList<>();
        long differences = 0;
        long expectedRows = 0;
        long actualRows = 0;
        while (expected.hasNext() || actual.hasNext()) {
            Object[] e = expected.hasNext() ? expected.next() : null;
            Object[] a = actual.hasNext() ? actual.next() : null;
            if (e != null) {
                expectedRows++;
            }
            if (a != null) {
                actualRows++;
            }
            String key = key(table, e != null ? e : a, e != null ? expectedRows : actualRows);
            if (a == null || e == null) {
                differences++;
                keep(kept, new Difference(a == null ? Kind.MISSING_ROW : Kind.EXTRA_ROW, key, null, null, null));
                continue;
            }
            for (int i = 0; i < columns.size(); i++) {
                ColumnModel column = columns.get(i);
                if (!ValueComparator.same(column.type(), e[i], a[i], fractionDigits[i])) {
                    differences++;
                    keep(
                            kept,
                            new Difference(
                                    Kind.VALUE, key, column.name(), CanonicalText.of(e[i]), CanonicalText.of(a[i])));
                }
            }
        }
        return new Result(table.name(), expectedRows, actualRows, differences, kept);
    }

    private static String key(TableModel table, Object[] row, long ordinal) {
        if (table.primaryKey() == null) {
            return "row " + ordinal;
        }
        return table.primaryKey().columnNames().stream()
                .map(name -> CanonicalText.of(
                        row[table.columns().indexOf(table.column(name).orElseThrow())]))
                .collect(Collectors.joining(", ", "(", ")"));
    }

    private static void keep(List<Difference> kept, Difference difference) {
        if (kept.size() < MAX_DIFFERENCES) {
            kept.add(difference);
        }
    }
}
