package com.lytrax.accessconverter.target.sqlite;

import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.ForeignKeyModel.Action;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.IndexModel.IndexColumn;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.model.TableModel;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * What {@link SqlitePlanner} decided: the exact DDL to run and, per column, how its values are stored. The writer
 * only executes this, and {@code verify} compares the finished file with it (09, steps 1–3).
 *
 * @param profile null when profiling was skipped, which forces the conservative choice everywhere
 * @param metadata null unless {@code --sqlite-metadata} asked for the Access metadata tables
 */
public record SqlitePlan(
        SchemaModel model, boolean strict, List<PlannedTable> tables, Metadata metadata, boolean profiled) {

    public SqlitePlan {
        tables = List.copyOf(tables);
    }

    public Optional<PlannedTable> table(String name) {
        return tables.stream().filter(t -> t.name().equalsIgnoreCase(name)).findFirst();
    }

    /** Every index of every table, in table order. */
    public List<PlannedIndex> indexes() {
        return tables.stream().flatMap(t -> t.indexes().stream()).toList();
    }

    /** The names of the opt-in metadata tables. */
    public record Metadata(String columnsTable, String relationshipsTable) {}

    /**
     * @param name the table's name in the output, which is the Access name unless it had to be renamed
     * @param columns the columns that are written, in Access column order; dropped ones are absent
     * @param autoIncrementColumn the {@code INTEGER PRIMARY KEY AUTOINCREMENT} column, or null
     * @param sequenceSeed the value to seed {@code sqlite_sequence} with, or null when it isn't known
     */
    public record PlannedTable(
            TableModel source,
            String name,
            List<PlannedColumn> columns,
            PlannedKey primaryKey,
            List<PlannedIndex> indexes,
            List<PlannedForeignKey> foreignKeys,
            String autoIncrementColumn,
            Long sequenceSeed,
            String createTableSql) {

        public PlannedTable {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(createTableSql, "createTableSql");
            columns = List.copyOf(columns);
            indexes = List.copyOf(indexes);
            foreignKeys = List.copyOf(foreignKeys);
        }

        public Optional<PlannedColumn> column(String columnName) {
            return columns.stream()
                    .filter(c -> c.source().name().equalsIgnoreCase(columnName))
                    .findFirst();
        }
    }

    /**
     * @param sourceIndex the column's position in {@link PlannedTable#source()}'s column list, which is the
     *     position of its value in a row of the source stream
     * @param fractionDigits date/time columns: the fractional-second digits written
     */
    public record PlannedColumn(
            ColumnModel source,
            int sourceIndex,
            String name,
            String declaredType,
            boolean notNull,
            boolean collateNocase,
            String defaultSql,
            ValueForm form,
            int fractionDigits) {

        public PlannedColumn {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(form, "form");
        }
    }

    /** How a canonical value is stored, and so how it is bound and read back. */
    public enum ValueForm {
        /** 0 or 1. */
        BOOLEAN_INT,
        INTEGER,
        LONG,
        /** A Double, bound as it is. */
        REAL,
        /** A Single, bound as {@code Double.parseDouble(Float.toString(v))} so its text form stays short. */
        REAL_FROM_FLOAT,
        /** An exact decimal a NUMERIC column stores losslessly (at most 15 significant digits). */
        DECIMAL_NUMBER,
        /** An exact decimal held as text, because NUMERIC affinity would round it (F-06). */
        DECIMAL_TEXT,
        /** ISO-8601 {@code 'YYYY-MM-DD HH:MM:SS[.fff…]'} text (F-05). */
        DATE_TEXT,
        TEXT,
        BLOB,
        /** Access's per-row complex id; its values become child tables in a later phase (08). */
        COMPLEX_ID
    }

    /** @param rowidAlias the key is a single {@code INTEGER PRIMARY KEY} column, so it is the table's rowid */
    public record PlannedKey(IndexModel source, List<IndexColumn> columns, boolean rowidAlias) {
        public PlannedKey {
            columns = List.copyOf(columns);
        }

        public List<String> columnNames() {
            return columns.stream().map(IndexColumn::name).toList();
        }
    }

    /**
     * @param source the Access index, or null for an index the planner added for a relationship Access doesn't back
     *     with one (D7)
     * @param where the partial-index predicate mirroring Access's IgnoreNulls, or null
     */
    public record PlannedIndex(
            IndexModel source,
            String table,
            String name,
            List<IndexColumn> columns,
            boolean unique,
            String where,
            String createIndexSql) {

        public PlannedIndex {
            columns = List.copyOf(columns);
        }

        public List<String> columnNames() {
            return columns.stream().map(IndexColumn::name).toList();
        }
    }

    public record PlannedForeignKey(
            ForeignKeyModel source,
            List<String> childColumns,
            String parentTable,
            List<String> parentColumns,
            Action onUpdate,
            Action onDelete) {

        public PlannedForeignKey {
            childColumns = List.copyOf(childColumns);
            parentColumns = List.copyOf(parentColumns);
        }
    }
}
