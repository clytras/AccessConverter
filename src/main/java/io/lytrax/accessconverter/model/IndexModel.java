package io.lytrax.accessconverter.model;

import java.util.List;
import java.util.Objects;

/**
 * A normalized index: the logical indexes Access reports, with hidden relationship-backing indexes and duplicates
 * folded in (04, Index normalization).
 *
 * @param ignoreNulls Access leaves rows whose key columns are all NULL out of the index
 * @param required the index columns can't be NULL
 * @param sourceNames every Access index name merged into this one, the kept name first
 */
public record IndexModel(
        String name,
        List<IndexColumn> columns,
        boolean primaryKey,
        boolean unique,
        boolean ignoreNulls,
        boolean required,
        Origin origin,
        List<String> sourceNames) {

    public enum Origin {
        /** The primary key or an index the user created. */
        USER,
        /** The child side of a relationship: Access's hidden index on the foreign-key columns. */
        RELATIONSHIP,
        /**
         * A primary key Access doesn't have, added with {@code --add-primary-key}: its one column numbers the rows in
         * the order they are read, and it has no Access index behind it.
         */
        GENERATED
    }

    public IndexModel {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(origin, "origin");
        columns = List.copyOf(columns);
        sourceNames = List.copyOf(sourceNames);
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("index " + name + " has no columns");
        }
    }

    public List<String> columnNames() {
        return columns.stream().map(IndexColumn::name).toList();
    }

    public record IndexColumn(String name, boolean ascending) {
        public IndexColumn {
            Objects.requireNonNull(name, "name");
        }
    }
}
