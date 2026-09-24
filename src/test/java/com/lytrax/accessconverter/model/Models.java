package com.lytrax.accessconverter.model;

import com.lytrax.accessconverter.extract.ExpressionTranslator;
import com.lytrax.accessconverter.model.ForeignKeyModel.Action;
import com.lytrax.accessconverter.model.ForeignKeyModel.Join;
import com.lytrax.accessconverter.model.ForeignKeyModel.Status;
import com.lytrax.accessconverter.model.IndexModel.IndexColumn;
import com.lytrax.accessconverter.model.IndexModel.Origin;
import java.util.ArrayList;
import java.util.List;

/**
 * Test helper: hand-built schema models, for the target rules that no corpus database exercises (NULLs in a
 * Required column, a default that doesn't fit its column, an Ignore-Nulls unique index used as a parent key, …).
 */
public final class Models {

    private Models() {}

    public static SchemaModel schema(List<TableModel> tables, List<ForeignKeyModel> relationships) {
        return new SchemaModel(new SchemaModel.Source("test.accdb", "V2010", null, "UTF-16LE"), tables, relationships);
    }

    public static SchemaModel schema(TableModel... tables) {
        return schema(List.of(tables), List.of());
    }

    public static Column column(String name, AccessType type) {
        return new Column(name, type);
    }

    /** A primary key on these columns; a name ending in {@code  DESC} makes that column descending. */
    public static IndexModel primaryKey(String... columns) {
        return new IndexModel(
                "PrimaryKey", indexColumns(columns), true, true, false, true, Origin.USER, List.of("PrimaryKey"));
    }

    public static IndexModel index(String name, String... columns) {
        return new IndexModel(name, indexColumns(columns), false, false, false, false, Origin.USER, List.of(name));
    }

    public static IndexModel unique(String name, String... columns) {
        return new IndexModel(name, indexColumns(columns), false, true, false, false, Origin.USER, List.of(name));
    }

    /** A unique index Access leaves all-NULL keys out of. */
    public static IndexModel uniqueIgnoringNulls(String name, String... columns) {
        return new IndexModel(name, indexColumns(columns), false, true, true, false, Origin.USER, List.of(name));
    }

    public static TableModel table(String name, IndexModel primaryKey, List<IndexModel> indexes, Column... columns) {
        List<ColumnModel> models = new ArrayList<>();
        for (int i = 0; i < columns.length; i++) {
            models.add(columns[i].at(i));
        }
        return new TableModel(name, null, models, primaryKey, indexes, null, null, 0);
    }

    public static TableModel table(String name, IndexModel primaryKey, Column... columns) {
        return table(name, primaryKey, List.of(), columns);
    }

    /** An enforced relationship whose parent key is {@code parentKey}: a candidate foreign key. */
    public static ForeignKeyModel foreignKey(
            String name,
            String parentTable,
            String parentColumn,
            String childTable,
            String childColumn,
            String parentKey,
            Action onUpdate,
            Action onDelete) {
        return new ForeignKeyModel(
                name,
                parentTable,
                List.of(parentColumn),
                childTable,
                List.of(childColumn),
                true,
                onUpdate,
                onDelete,
                false,
                Join.INNER,
                0,
                Status.EMIT,
                parentKey);
    }

    /** A join line only: Access allows orphans, so it is never a foreign key (F-31). */
    public static ForeignKeyModel notEnforced(
            String name, String parentTable, String parentColumn, String childTable, String childColumn) {
        return new ForeignKeyModel(
                name,
                parentTable,
                List.of(parentColumn),
                childTable,
                List.of(childColumn),
                false,
                Action.NO_ACTION,
                Action.NO_ACTION,
                false,
                Join.INNER,
                0,
                Status.NOT_ENFORCED,
                null);
    }

    private static List<IndexColumn> indexColumns(String... columns) {
        List<IndexColumn> indexColumns = new ArrayList<>();
        for (String column : columns) {
            boolean descending = column.endsWith(" DESC");
            indexColumns.add(
                    new IndexColumn(descending ? column.substring(0, column.length() - 5) : column, !descending));
        }
        return indexColumns;
    }

    /** A column under construction; Access's own defaults apply until something is set. */
    public static final class Column {
        private final String name;
        private final AccessType type;
        private Integer length;
        private Integer precision;
        private Integer scale;
        private boolean required;
        private boolean allowZeroLength = true;
        private DefaultValue defaultValue;
        private CheckRule validation;
        private String description;
        private String calculated;
        private boolean hidden;
        private ColumnModel element;

        private Column(String name, AccessType type) {
            this.name = name;
            this.type = type;
            if (type == AccessType.TEXT) {
                this.length = 255;
            }
            if (type == AccessType.MONEY) {
                this.precision = 19;
                this.scale = 4;
            }
        }

        public Column length(int characters) {
            this.length = characters;
            return this;
        }

        public Column decimal(int precisionDigits, int scaleDigits) {
            this.precision = precisionDigits;
            this.scale = scaleDigits;
            return this;
        }

        public Column required() {
            this.required = true;
            return this;
        }

        public Column noEmptyString() {
            this.allowZeroLength = false;
            return this;
        }

        public Column defaultValue(String access) {
            this.defaultValue = ExpressionTranslator.translateDefault(access);
            return this;
        }

        public Column validation(String access) {
            this.validation = ExpressionTranslator.translateColumnRule(access, null, name);
            return this;
        }

        public Column description(String text) {
            this.description = text;
            return this;
        }

        public Column calculated(String expression) {
            this.calculated = expression;
            return this;
        }

        public Column hidden() {
            this.hidden = true;
            return this;
        }

        /** A multi-value column's element type. */
        public Column element(AccessType elementType) {
            this.element = new Column("Value", elementType).at(0);
            return this;
        }

        public ColumnModel at(int ordinal) {
            return new ColumnModel(
                    name,
                    ordinal,
                    type,
                    length,
                    precision,
                    scale,
                    required,
                    allowZeroLength,
                    defaultValue,
                    validation,
                    description,
                    null,
                    null,
                    false,
                    calculated,
                    false,
                    hidden,
                    element);
        }
    }
}
