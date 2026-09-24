package com.lytrax.accessconverter.extract;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.PropertyMap;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.impl.complex.MultiValueColumnInfoImpl;
import com.lytrax.accessconverter.extract.IndexNormalizer.RawIndex;
import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.model.CheckRule;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.DefaultValue;
import com.lytrax.accessconverter.model.IndexModel.IndexColumn;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.source.SourceException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Reads one local table's metadata: columns with their properties, and normalized indexes (04). No rows. */
final class TableReader {
    private TableReader() {}

    /** Reads a table's metadata; anything Jackcess fails on becomes a typed error naming the table. */
    static TableModel read(Path file, Table table, Issues issues) throws SourceException {
        try {
            return read(table, issues);
        } catch (RuntimeException e) {
            throw SourceException.readFailed(file, "table " + table.getName(), e);
        }
    }

    private static TableModel read(Table table, Issues issues) {
        String name = table.getName();
        List<ColumnModel> columns = new ArrayList<>();
        for (Column column : table.getColumns()) {
            columns.add(column(name, column, issues));
        }

        List<RawIndex> raw = new ArrayList<>();
        for (Index index : table.getIndexes()) {
            List<IndexColumn> indexColumns = index.getColumns().stream()
                    .map(c -> new IndexColumn(c.getName(), c.isAscending()))
                    .toList();
            raw.add(new RawIndex(
                    index.getName(),
                    indexColumns,
                    index.isPrimaryKey(),
                    index.isForeignKey(),
                    index.isUnique(),
                    index.shouldIgnoreNulls(),
                    index.isRequired()));
        }
        IndexNormalizer.Result indexes = IndexNormalizer.normalize(name, raw, issues);
        if (indexes.primaryKey() == null) {
            issues.add(IssueCode.NO_PRIMARY_KEY, name, null, "table has no primary key; targets create none");
        }

        Properties props = Properties.of(name, null, table::getProperties, issues);
        String rule = props.string(PropertyMap.VALIDATION_RULE_PROP);
        CheckRule validation = null;
        if (rule != null) {
            validation = ExpressionTranslator.translateTableRule(rule, props.string(PropertyMap.VALIDATION_TEXT_PROP));
            reportUntranslated(validation, name, null, issues);
        }
        return new TableModel(
                name,
                null,
                columns,
                indexes.primaryKey(),
                indexes.indexes(),
                props.string(PropertyMap.DESCRIPTION_PROP),
                validation,
                table.getRowCount());
    }

    private static ColumnModel column(String table, Column column, Issues issues) {
        String name = column.getName();
        AccessType type = type(column);
        if (type == AccessType.UNSUPPORTED || type == AccessType.COMPLEX_UNSUPPORTED) {
            issues.add(
                    IssueCode.UNSUPPORTED_COLUMN_TYPE,
                    table,
                    name,
                    "Jackcess can't decode this column type (" + column.getType() + "); its raw bytes are kept");
        }

        Properties props = Properties.of(table, name, column::getProperties, issues);
        DefaultValue defaultValue = null;
        String defaultText = props.string(PropertyMap.DEFAULT_VALUE_PROP);
        if (defaultText != null) {
            defaultValue = ExpressionTranslator.translateDefault(defaultText);
            if (!defaultValue.isTranslated()) {
                issues.add(
                        IssueCode.DEFAULT_UNTRANSLATABLE,
                        table,
                        name,
                        "default " + defaultText + " is not translated: " + defaultValue.unsupportedReason());
            }
        }
        CheckRule validation = null;
        String ruleText = props.string(PropertyMap.VALIDATION_RULE_PROP);
        if (ruleText != null) {
            validation = ExpressionTranslator.translateColumnRule(
                    ruleText, props.string(PropertyMap.VALIDATION_TEXT_PROP), name);
            reportUntranslated(validation, table, name, issues);
        }
        String calculated = column.isCalculated() ? props.string(PropertyMap.EXPRESSION_PROP) : null;
        if (column.isCalculated()) {
            issues.add(
                    IssueCode.CALCULATED_AS_VALUE,
                    table,
                    name,
                    "calculated column exported as its stored values; expression " + calculated);
        }

        Integer textFormat = props.integer(PropertyMap.TEXT_FORMAT_PROP);
        return new ColumnModel(
                name,
                column.getColumnIndex(),
                type,
                length(type, column),
                precision(type, column),
                scale(type, column),
                props.bool(PropertyMap.REQUIRED_PROP, false),
                !type.isText() || props.bool(PropertyMap.ALLOW_ZERO_LEN_PROP, true),
                defaultValue,
                validation,
                props.string(PropertyMap.DESCRIPTION_PROP),
                props.string(PropertyMap.FORMAT_PROP),
                props.integer(PropertyMap.DECIMAL_PLACES_PROP),
                textFormat != null && textFormat == 1,
                calculated,
                column.isAppendOnly(),
                column.isHidden(),
                type == AccessType.MULTI_VALUE ? element(table, column, issues) : null);
    }

    /**
     * A multi-value column's element: the {@code Value} column of Access's hidden value table, whose type the values
     * have (a lookup of text, numbers, …). Only its storage matters; Access keeps no properties there.
     */
    private static ColumnModel element(String table, Column column, Issues issues) {
        if (!(column.getComplexInfo() instanceof MultiValueColumnInfoImpl info) || info.getValueColumn() == null) {
            issues.add(
                    IssueCode.UNSUPPORTED_COLUMN_TYPE,
                    table,
                    column.getName(),
                    "the multi-value column's value table can't be read; its values are exported as text");
            return null;
        }
        Column value = info.getValueColumn();
        AccessType type = type(value);
        return new ColumnModel(
                value.getName(),
                0,
                type,
                length(type, value),
                precision(type, value),
                scale(type, value),
                false,
                true,
                null,
                null,
                null,
                null,
                null,
                false,
                null,
                false,
                false,
                null);
    }

    private static Integer length(AccessType type, Column column) {
        return switch (type) {
            case TEXT -> (int) column.getLengthInUnits();
            case BINARY -> (int) column.getLength();
            default -> null;
        };
    }

    private static Integer precision(AccessType type, Column column) {
        return switch (type) {
            case NUMERIC -> (int) column.getPrecision();
            case MONEY -> 19;
            default -> null;
        };
    }

    private static Integer scale(AccessType type, Column column) {
        return switch (type) {
            case NUMERIC -> (int) column.getScale();
            case MONEY -> 4;
            default -> null;
        };
    }

    /** Jackcess's type, refined. Complex columns also report isAutoNumber(): that never makes them identities (F-15). */
    static AccessType type(Column column) {
        DataType type = column.getType();
        if (type == DataType.COMPLEX_TYPE) {
            if (column.getComplexInfo() == null) {
                return AccessType.COMPLEX_UNSUPPORTED;
            }
            return switch (column.getComplexInfo().getType()) {
                case ATTACHMENT -> AccessType.ATTACHMENT;
                case MULTI_VALUE -> AccessType.MULTI_VALUE;
                case VERSION_HISTORY -> AccessType.VERSION_HISTORY;
                case UNSUPPORTED -> AccessType.COMPLEX_UNSUPPORTED;
            };
        }
        return switch (type) {
            case BOOLEAN -> AccessType.BOOLEAN;
            case BYTE -> AccessType.BYTE;
            case INT -> AccessType.INT;
            case LONG -> column.isAutoNumber() ? AccessType.AUTONUMBER_LONG : AccessType.LONG;
            case BIG_INT -> AccessType.BIG_INT;
            case MONEY -> AccessType.MONEY;
            case FLOAT -> AccessType.FLOAT;
            case DOUBLE -> AccessType.DOUBLE;
            case NUMERIC -> AccessType.NUMERIC;
            case SHORT_DATE_TIME -> AccessType.SHORT_DATE_TIME;
            case EXT_DATE_TIME -> AccessType.EXT_DATE_TIME;
            case TEXT -> AccessType.TEXT;
            case MEMO -> column.isHyperlink() ? AccessType.HYPERLINK : AccessType.MEMO;
            case GUID -> column.isAutoNumber() ? AccessType.AUTONUMBER_GUID : AccessType.GUID;
            case BINARY, BIG_BINARY -> AccessType.BINARY;
            case OLE -> AccessType.OLE;
            case COMPLEX_TYPE, UNSUPPORTED_FIXEDLEN, UNSUPPORTED_VARLEN -> AccessType.UNSUPPORTED;
        };
    }

    private static void reportUntranslated(CheckRule rule, String table, String column, Issues issues) {
        if (!rule.isTranslated()) {
            issues.add(
                    IssueCode.CHECK_UNTRANSLATABLE,
                    table,
                    column,
                    "validation rule " + rule.raw() + " is not translated: " + rule.unsupportedReason());
        }
    }

    /** A property map that may be unreadable in old files: then the defaults apply and it's reported (04). */
    private record Properties(PropertyMap map) {
        interface Source {
            PropertyMap get() throws IOException;
        }

        static Properties of(String table, String column, Source source, Issues issues) {
            try {
                return new Properties(source.get());
            } catch (IOException | RuntimeException e) {
                issues.add(
                        IssueCode.PROPERTIES_UNREADABLE,
                        table,
                        column,
                        "properties unreadable (" + e.getMessage() + "); Access defaults assumed");
                return new Properties(null);
            }
        }

        String string(String name) {
            Object value = map == null ? null : map.getValue(name);
            if (value == null) {
                return null;
            }
            String text = value.toString();
            return text.isEmpty() ? null : text;
        }

        boolean bool(String name, boolean absent) {
            Object value = map == null ? null : map.getValue(name);
            return value instanceof Boolean b ? b : absent;
        }

        Integer integer(String name) {
            Object value = map == null ? null : map.getValue(name);
            return switch (value) {
                case Byte b -> Byte.toUnsignedInt(b);
                case Number n -> n.intValue();
                case null, default -> null;
            };
        }
    }
}
