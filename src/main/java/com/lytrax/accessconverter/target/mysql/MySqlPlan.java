package com.lytrax.accessconverter.target.mysql;

import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.ForeignKeyModel;
import com.lytrax.accessconverter.model.ForeignKeyModel.Action;
import com.lytrax.accessconverter.model.IndexModel;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.PlanRules;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * What {@link MySqlPlanner} decided: the exact statements of every dump section and, per column, how its values are
 * written. The writer only emits this, and {@code verify} compares a loaded database with it (09, steps 1–3).
 *
 * @param collation the table collation every text column inherits
 * @param profiled false when profiling was skipped, which forces the conservative choice everywhere
 * @param options the options planned with, which decide how binary values are stored (08)
 */
public record MySqlPlan(
        SchemaModel model,
        MySqlDialect dialect,
        String collation,
        List<PlannedTable> tables,
        boolean profiled,
        ConvertOptions options) {

    public MySqlPlan {
        tables = List.copyOf(tables);
    }

    public Optional<PlannedTable> table(String name) {
        return tables.stream().filter(t -> t.name().equalsIgnoreCase(name)).findFirst();
    }

    /**
     * @param name the table's name in the output, which is the Access name unless it had to be renamed
     * @param columns the columns that are written, in Access column order; dropped ones are absent
     * @param primaryKey null when the table has none in the output
     * @param inlineIndexes indexes created with the table: the key an AUTO_INCREMENT column needs (F-35)
     * @param indexes the secondary indexes, added after the data (section 3)
     * @param comment the table comment, or null
     * @param autoIncrementSeed the {@code AUTO_INCREMENT} table option, or null
     * @param indexSql the section 3 statement, or null when the table has no secondary indexes
     * @param checkSql the section 4 statement, or null
     * @param foreignKeySql the section 5 statement, or null
     */
    public record PlannedTable(
            TableModel source,
            String name,
            List<PlannedColumn> columns,
            PlannedIndex primaryKey,
            List<PlannedIndex> inlineIndexes,
            List<PlannedIndex> indexes,
            List<PlannedCheck> checks,
            List<PlannedForeignKey> foreignKeys,
            String comment,
            Long autoIncrementSeed,
            String createTableSql,
            String indexSql,
            String checkSql,
            String foreignKeySql) {

        public PlannedTable {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(createTableSql, "createTableSql");
            columns = List.copyOf(columns);
            inlineIndexes = List.copyOf(inlineIndexes);
            indexes = List.copyOf(indexes);
            checks = List.copyOf(checks);
            foreignKeys = List.copyOf(foreignKeys);
        }

        public Optional<PlannedColumn> column(String accessName) {
            return columns.stream()
                    .filter(c -> c.source().name().equalsIgnoreCase(accessName))
                    .findFirst();
        }

        /** The primary key, then the indexes created with the table, then the rest. */
        public List<PlannedIndex> allIndexes() {
            return Stream.concat(Stream.ofNullable(primaryKey), Stream.concat(inlineIndexes.stream(), indexes.stream()))
                    .toList();
        }
    }

    /**
     * @param sourceIndex the column's position in {@link PlannedTable#source()}'s column list, which is the position
     *     of its value in a row of the source stream
     * @param type the declared type, e.g. {@code VARCHAR(50)}
     * @param collation the column's own collation, or null when it has the table's
     * @param defaultSql the {@code DEFAULT} value as written, or null for none
     * @param comment the column comment, or null
     * @param fractionDigits date/time columns: the fractional-second digits the column keeps
     * @param olePart for an OLE column's companion ({@code --ole-extract}): the part of the decoded value at
     *     {@code sourceIndex} it holds; null otherwise
     */
    public record PlannedColumn(
            ColumnModel source,
            int sourceIndex,
            String name,
            String type,
            String collation,
            ValueForm form,
            boolean notNull,
            boolean autoIncrement,
            String defaultSql,
            String comment,
            int fractionDigits,
            PlanRules.OlePart olePart) {

        public PlannedColumn {
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(form, "form");
        }
    }

    /** How a canonical value is written. */
    public enum ValueForm {
        /** 0 or 1. */
        BOOLEAN,
        /** A Short, Integer or Long, in digits. */
        INTEGER,
        /** A BigDecimal, all digits. */
        DECIMAL,
        /** A Float, rounding-safe for a FLOAT column. */
        FLOAT,
        DOUBLE,
        /** A LocalDateTime at the column's precision. */
        DATETIME,
        TEXT,
        /** A {@code byte[]} (or an OLE value's raw bytes) as {@code X'…'}. */
        BYTES,
        /** {@code --binary files}: a file's path relative to the dump's directory (08). */
        PATH,
        /** {@code --binary omit}: the value's size in bytes (08). */
        SIZE,
        /** Access's per-row complex id, which the complex column's child table refers to (08). */
        COMPLEX_ID
    }

    /**
     * @param source the Access index, or one the planner made (D7, a foreign key's or an AUTO_INCREMENT column's)
     * @param unique false for an index Access has as unique when the key is too long for MySQL (05)
     */
    public record PlannedIndex(IndexModel source, String name, List<KeyPart> parts, boolean primary, boolean unique) {
        public PlannedIndex {
            parts = List.copyOf(parts);
        }

        public List<String> columnNames() {
            return parts.stream().map(KeyPart::column).toList();
        }
    }

    /**
     * @param column the column's output name
     * @param prefix the key's prefix length (characters for text, bytes for binary), or null for the whole value
     */
    public record KeyPart(String column, boolean ascending, Integer prefix) {}

    /** @param column the Access column the rule belongs to, or null for the table's rule */
    public record PlannedCheck(String name, String sql, String column) {}

    public record PlannedForeignKey(
            ForeignKeyModel source,
            String name,
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
