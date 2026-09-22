package com.lytrax.accessconverter.model;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * An Access table. A linked table has a {@link LinkInfo} and no columns or indexes.
 *
 * @param primaryKey null when the table has none (targets then emit none, never an invented one)
 * @param indexes the normalized secondary indexes, ordered by name
 * @param validation the table-level validation rule, which can span columns
 * @param rowCount Access's row count, for progress only; written counts come from the row stream
 */
public record TableModel(
        String name,
        LinkInfo link,
        List<ColumnModel> columns,
        IndexModel primaryKey,
        List<IndexModel> indexes,
        String description,
        CheckRule validation,
        long rowCount) {

    public TableModel {
        Objects.requireNonNull(name, "name");
        columns = List.copyOf(columns);
        indexes = List.copyOf(indexes);
    }

    public boolean isLinked() {
        return link != null;
    }

    /** Column lookup ignoring case, like Access. */
    public Optional<ColumnModel> column(String columnName) {
        return columns.stream()
                .filter(c -> c.name().equalsIgnoreCase(columnName))
                .findFirst();
    }

    /** The PK followed by the secondary indexes. */
    public List<IndexModel> allIndexes() {
        if (primaryKey == null) {
            return indexes;
        }
        return Stream.concat(Stream.of(primaryKey), indexes.stream()).toList();
    }

    /**
     * A linked table's stored target.
     *
     * @param database the linked database path as Access stored it, or the ODBC connection string
     * @param remoteTable the table's name in that database
     */
    public record LinkInfo(String database, String remoteTable, boolean odbc) {}
}
