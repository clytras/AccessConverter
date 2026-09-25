package io.lytrax.accessconverter.source;

import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.complex.Attachment;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.complex.SingleValue;
import com.healthmarketscience.jackcess.complex.Version;
import io.lytrax.accessconverter.model.AccessType;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.model.TableModel.ComplexSource;
import io.lytrax.accessconverter.value.AccessValues;
import io.lytrax.accessconverter.value.AttachmentValue;
import io.lytrax.accessconverter.value.ComplexValues;
import io.lytrax.accessconverter.value.VersionValue;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Set;

/**
 * Reads the values behind attachment, multi-value and version-history cells (08). Access keeps them in hidden tables
 * keyed by the cell's complex id; Jackcess looks them up per cell. Values come in Access's value-id order.
 */
final class ComplexReader {

    private ComplexReader() {}

    /** Complex columns whose values can be read. An unsupported complex type keeps only its id. */
    static boolean readsValues(AccessType type) {
        return type == AccessType.ATTACHMENT || type == AccessType.MULTI_VALUE || type == AccessType.VERSION_HISTORY;
    }

    /** A cell with its values, or null for a NULL cell. */
    static ComplexValues cell(ColumnModel column, Object raw) throws IOException {
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof ComplexValueForeignKey key)) {
            throw new IllegalStateException("column type " + column.type() + " expects ComplexValueForeignKey from"
                    + " Jackcess, got " + raw.getClass().getName());
        }
        return new ComplexValues(key.get(), items(key, column.type(), column.element()));
    }

    /** The values of one cell as canonical items, in value-id order. */
    private static List<Object> items(ComplexValueForeignKey key, AccessType kind, ColumnModel element)
            throws IOException {
        List<ComplexValue> sorted = values(key, kind);
        List<Object> items = new ArrayList<>(sorted.size());
        for (ComplexValue value : sorted) {
            items.add(item(value, element));
        }
        return items;
    }

    /** A cell's values, in value-id order. */
    private static List<ComplexValue> values(ComplexValueForeignKey key, AccessType kind) throws IOException {
        List<ComplexValue> values = new ArrayList<>(
                switch (kind) {
                    case ATTACHMENT -> key.getAttachments();
                    case MULTI_VALUE -> key.getMultiValues();
                    case VERSION_HISTORY -> key.getVersions();
                    default -> throw new IllegalArgumentException("no values to read for " + kind);
                });
        values.sort(Comparator.comparingInt(v -> v.getId().get()));
        return values;
    }

    private static Object item(ComplexValue value, ColumnModel element) throws IOException {
        int id = value.getId().get();
        return switch (value) {
            case Attachment a ->
                new AttachmentValue(
                        id,
                        a.getFileName(),
                        a.getFileType(),
                        a.getFileData(),
                        a.getFileUrl(),
                        a.getFileLocalTimeStamp(),
                        a.getFileFlags());
            case Version v -> new VersionValue(id, v.getValue(), v.getModifiedLocalDate());
            case SingleValue s -> element(s.get(), element);
            default ->
                throw new IllegalStateException(
                        "unexpected complex value " + value.getClass().getName());
        };
    }

    /** A multi-value element, canonical for its type; as text when the value table couldn't be read. */
    private static Object element(Object raw, ColumnModel element) {
        if (element == null) {
            return raw == null ? null : raw.toString();
        }
        return AccessValues.canonical(element.type(), raw);
    }

    /**
     * The rows of a complex child table: for each parent row (in the parent cursor's order), one row per value of the
     * complex column, with the child's columns in the order asked for. Child rows are made from the parent's own
     * cells, so every one refers to a parent row.
     */
    static RowStream childRows(Path file, Cursor parent, TableModel child, List<ColumnModel> columns) {
        ComplexSource source = child.complex();
        Set<String> names = Set.of(source.parentColumn());
        Deque<Object[]> pending = new ArrayDeque<>();
        return new RowStream(
                file, "the values of " + source.parentTable() + "." + source.parentColumn(), columns, () -> {
                    while (pending.isEmpty()) {
                        Row row = parent.getNextRow(names);
                        if (row == null) {
                            return null;
                        }
                        Object raw = row.get(source.parentColumn());
                        if (raw == null) {
                            continue;
                        }
                        ComplexValueForeignKey key = (ComplexValueForeignKey) raw;
                        ColumnModel element = child.column(ComplexSource.VALUE)
                                .filter(c -> source.kind() == AccessType.MULTI_VALUE)
                                .orElse(null);
                        for (ComplexValue value : values(key, source.kind())) {
                            Object item = item(value, element);
                            Object[] out = new Object[columns.size()];
                            for (int i = 0; i < out.length; i++) {
                                out[i] = field(
                                        columns.get(i).name(),
                                        source,
                                        key.get(),
                                        value.getId().get(),
                                        item);
                            }
                            pending.add(out);
                        }
                    }
                    return pending.poll();
                });
    }

    private static Object field(String column, ComplexSource source, int ref, int id, Object item) {
        if (column.equals(ComplexSource.ID)) {
            return id;
        }
        if (column.equals(source.refColumn())) {
            return ref;
        }
        return switch (item) {
            case AttachmentValue a ->
                switch (column) {
                    case ComplexSource.FILE_NAME -> a.fileName();
                    case ComplexSource.FILE_TYPE -> a.fileType();
                    case ComplexSource.FILE_DATA -> a.data();
                    case ComplexSource.FILE_SIZE -> a.size();
                    case ComplexSource.FILE_URL -> a.url();
                    case ComplexSource.FILE_TIMESTAMP -> a.timestamp();
                    case ComplexSource.FILE_FLAGS -> a.flags();
                    default -> throw unknown(column, source);
                };
            case VersionValue v ->
                switch (column) {
                    case ComplexSource.VALUE -> v.value();
                    case ComplexSource.MODIFIED -> v.modified();
                    default -> throw unknown(column, source);
                };
            case null, default -> {
                if (!column.equals(ComplexSource.VALUE)) {
                    throw unknown(column, source);
                }
                yield item; // a multi-value element
            }
        };
    }

    private static IllegalStateException unknown(String column, ComplexSource source) {
        return new IllegalStateException("no column " + column + " in the values of " + source.parentColumn());
    }
}
