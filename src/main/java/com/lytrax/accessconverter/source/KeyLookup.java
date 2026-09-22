package com.lytrax.accessconverter.source;

import com.healthmarketscience.jackcess.IndexCursor;
import com.healthmarketscience.jackcess.Row;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.value.AccessValues;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Looks rows up through an Access index, so matching follows Access's rules: text keys compare case-insensitively,
 * as the index sort order does. The stored key is returned, so callers can tell exact matches from inexact ones.
 */
public final class KeyLookup {
    private final IndexCursor cursor;
    private final List<ColumnModel> columns;
    private final Set<String> names = new LinkedHashSet<>();

    KeyLookup(IndexCursor cursor, List<ColumnModel> columns) {
        this.cursor = cursor;
        this.columns = List.copyOf(columns);
        this.columns.forEach(c -> names.add(c.name()));
    }

    /** The index columns, in the order {@link #find} expects its key. */
    public List<ColumnModel> columns() {
        return columns;
    }

    /** The stored key (canonical values) of the first row matching {@code key}, or empty. */
    public Optional<Object[]> find(Object... key) throws IOException {
        if (key.length != columns.size()) {
            throw new IllegalArgumentException("expected " + columns.size() + " key values, got " + key.length);
        }
        Object[] entry = new Object[key.length];
        for (int i = 0; i < key.length; i++) {
            entry[i] = AccessValues.toJackcess(columns.get(i).type(), key[i]);
        }
        if (!cursor.findFirstRowByEntry(entry)) {
            return Optional.empty();
        }
        Row row = cursor.getCurrentRow(names);
        Object[] stored = new Object[columns.size()];
        for (int i = 0; i < stored.length; i++) {
            ColumnModel column = columns.get(i);
            stored[i] = AccessValues.canonical(column.type(), row.get(column.name()));
        }
        return Optional.of(stored);
    }
}
