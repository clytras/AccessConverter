package com.lytrax.accessconverter.source;

import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.Row;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.value.AccessValues;
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
    private final Path file;
    private final Cursor cursor;
    private final List<ColumnModel> columns;
    private final Set<String> names = new LinkedHashSet<>();
    private Row next;
    private boolean done;

    RowStream(Path file, Cursor cursor, List<ColumnModel> columns) {
        this.file = file;
        this.cursor = cursor;
        this.columns = List.copyOf(columns);
        this.columns.forEach(c -> names.add(c.name()));
    }

    public List<ColumnModel> columns() {
        return columns;
    }

    @Override
    public boolean hasNext() {
        if (next == null && !done) {
            try {
                next = cursor.getNextRow(names);
            } catch (IOException | RuntimeException e) {
                throw new UncheckedIOException(SourceException.readFailed(
                        file, "table " + cursor.getTable().getName(), e));
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
        Object[] values = new Object[columns.size()];
        for (int i = 0; i < values.length; i++) {
            ColumnModel column = columns.get(i);
            values[i] = AccessValues.canonical(column.type(), next.get(column.name()));
        }
        next = null;
        return values;
    }
}
