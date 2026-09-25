package io.lytrax.accessconverter.source;

import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.Row;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.value.AccessValues;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Streams a table's rows as canonical values, one array per row in the order of the requested columns. Nothing is
 * accumulated: memory doesn't grow with the table (03, Streaming).
 */
public final class RowStream implements Iterator<Object[]> {

    /** Produces the next canonical row, or null after the last one. */
    @FunctionalInterface
    interface Producer {
        Object[] next() throws IOException;
    }

    private final Path file;
    private final String what;
    private final List<ColumnModel> columns;
    private final Producer producer;
    private Object[] next;
    private boolean done;

    RowStream(Path file, String what, List<ColumnModel> columns, Producer producer) {
        this.file = file;
        this.what = what;
        this.columns = List.copyOf(columns);
        this.producer = producer;
    }

    /**
     * The rows of a cursor, each value made canonical.
     *
     * @param complexValues read the values of attachment, multi-value and version-history cells too ({@link
     *     ComplexReader}), instead of only their complex id
     * @param generatedKey the column {@code --add-primary-key} added, which isn't in Access and is filled with the row
     *     number (1, 2, 3, ... in cursor order); null when there is none
     */
    static RowStream of(
            Path file, Cursor cursor, List<ColumnModel> columns, boolean complexValues, String generatedKey) {
        Set<String> names = new LinkedHashSet<>();
        columns.forEach(c -> names.add(c.name()));
        if (generatedKey != null) {
            names.remove(generatedKey);
        }
        List<ColumnModel> copy = List.copyOf(columns);
        long[] number = {0};
        return new RowStream(file, "table " + cursor.getTable().getName(), copy, () -> {
            Row row = cursor.getNextRow(names);
            if (row == null) {
                return null;
            }
            number[0]++;
            Object[] values = new Object[copy.size()];
            for (int i = 0; i < values.length; i++) {
                ColumnModel column = copy.get(i);
                if (column.name().equals(generatedKey)) {
                    values[i] = AccessValues.canonical(column.type(), (int) number[0]);
                    continue;
                }
                Object raw = row.get(column.name());
                values[i] = complexValues && ComplexReader.readsValues(column.type())
                        ? ComplexReader.cell(column, raw)
                        : AccessValues.canonical(column.type(), raw);
            }
            return values;
        });
    }

    public List<ColumnModel> columns() {
        return columns;
    }

    @Override
    public boolean hasNext() {
        if (next == null && !done) {
            try {
                next = producer.next();
            } catch (IOException | RuntimeException e) {
                throw new UncheckedIOException(SourceException.readFailed(file, what, e));
            }
            done = next == null;
        }
        return next != null;
    }

    @Override
    public Object[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Object[] values = next;
        next = null;
        return values;
    }
}
