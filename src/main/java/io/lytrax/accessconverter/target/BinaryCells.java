package io.lytrax.accessconverter.target;

import io.lytrax.accessconverter.model.AccessType;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.model.TableModel.ComplexSource;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.OleDecoder;
import io.lytrax.accessconverter.target.PlanRules.OlePart;
import io.lytrax.accessconverter.value.CanonicalText;
import io.lytrax.accessconverter.value.OleContent;
import io.lytrax.accessconverter.value.OleValue;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * What a SQL target stores for a cell whose value is bytes (08): the bytes themselves ({@code --binary inline}), a
 * file's path ({@code files}) or the byte count ({@code omit}), and for an OLE column's companions
 * ({@code --ole-extract}) the decoded part. One instance per table being written; {@code verify} uses the same rules
 * the other way round.
 */
public final class BinaryCells {

    private final ConvertOptions options;
    private final BinaryFiles files;
    private final Issues issues;
    private final TableModel table;
    private final String tableName;
    private final int[] keyPositions;
    private final int fileNamePosition;

    /**
     * @param files where files go in {@code --binary files}; null otherwise
     * @param tableName the table as the output names it
     */
    public BinaryCells(ConvertOptions options, BinaryFiles files, Issues issues, TableModel table, String tableName) {
        this.options = options;
        this.files = files;
        this.issues = issues;
        this.table = table;
        this.tableName = tableName;
        this.keyPositions = table.primaryKey() == null
                ? new int[0]
                : table.primaryKey().columnNames().stream()
                        .mapToInt(name ->
                                table.columns().indexOf(table.column(name).orElseThrow()))
                        .toArray();
        this.fileNamePosition = table.isComplexChild() && table.complex().kind() == AccessType.ATTACHMENT
                ? table.columns().indexOf(table.column(ComplexSource.FILE_NAME).orElseThrow())
                : -1;
    }

    /** Whether a column holds bytes, which {@code --binary} decides the storage of. */
    public static boolean isPayload(ColumnModel column) {
        return column.type() == AccessType.BINARY
                || column.type() == AccessType.OLE
                || column.type() == AccessType.UNSUPPORTED;
    }

    /** How a column's values are stored, which decides its type. */
    public enum Storage {
        /** Not bytes, or bytes inline. */
        VALUE,
        /** A relative path, text. */
        PATH,
        /** A byte count, a 64-bit integer. */
        SIZE
    }

    public static Storage storage(ColumnModel column, ConvertOptions options) {
        if (!isPayload(column)) {
            return Storage.VALUE;
        }
        return switch (options.binary()) {
            case INLINE -> Storage.VALUE;
            case FILES -> Storage.PATH;
            case OMIT -> Storage.SIZE;
        };
    }

    /** The longest path a {@link Storage#PATH} column holds: {@code VARCHAR(1024)} (08). */
    public static final int PATH_LENGTH = 1024;

    /**
     * The value to write for a column of this row.
     *
     * @param column the written column (its source model, and for a companion the OLE part)
     * @param outputColumn the column as the output names it (its files directory)
     * @param row the source row
     * @param ordinal the row's number in the table, from 1 (the file name of a row without a primary key)
     */
    public Object value(
            ColumnModel column, OlePart part, int sourceIndex, String outputColumn, Object[] row, long ordinal)
            throws IOException {
        Object cell = row[sourceIndex];
        Object value = part == null ? cell : olePart(part, cell, row, ordinal);
        if (value == null || !isPayload(column)) {
            return value;
        }
        byte[] bytes = bytes(value);
        return switch (options.binary()) {
            case INLINE -> bytes;
            case OMIT -> {
                issues.add(
                        IssueCode.BINARY_OMITTED,
                        table.name(),
                        column.name(),
                        "the bytes are not written (--binary omit); the column holds each value's size in bytes");
                yield (long) bytes.length;
            }
            case FILES -> files.write(tableName, outputColumn, rowKey(row, ordinal), fileName(part, cell, row), bytes);
        };
    }

    /** The part of an OLE value a companion column holds; reports an undecodable value once, at its kind. */
    private Object olePart(OlePart part, Object cell, Object[] row, long ordinal) {
        if (cell == null) {
            return null;
        }
        OleContent content = decoded(cell);
        if (!content.decoded()) {
            if (part == OlePart.KIND) {
                issues.add(
                        IssueCode.OLE_UNDECODABLE,
                        table.name(),
                        null,
                        "OLE values that can't be decoded keep only their raw bytes; the first: " + content.problem(),
                        rowKey(row, ordinal));
            }
            return null;
        }
        return part(part, content);
    }

    /** A decoded part: the kind's label, the name, the MIME type or the content bytes. */
    public static Object part(OlePart part, OleContent content) {
        if (!content.decoded()) {
            return null;
        }
        return switch (part) {
            case KIND -> content.kind().label();
            case NAME -> content.name();
            case MIME -> content.mime();
            case CONTENT -> content.content();
        };
    }

    public static OleContent decoded(Object cell) {
        return ((OleValue) cell).decoded(OleDecoder::decode);
    }

    /** The data's own file name for a file: an attachment's, or an OLE package's for its content. */
    private String fileName(OlePart part, Object cell, Object[] row) {
        if (part == OlePart.CONTENT && cell != null) {
            OleContent content = decoded(cell);
            return content.kind() == OleContent.Kind.PACKAGE ? content.name() : null;
        }
        if (fileNamePosition >= 0 && part == null) {
            return (String) row[fileNamePosition];
        }
        return null;
    }

    /**
     * A row's key for a file name: its primary-key values joined with {@code _}, or its ordinal without a primary key
     * (08).
     */
    public String rowKey(Object[] row, long ordinal) {
        return rowKey(keyPositions, row, ordinal);
    }

    static String rowKey(int[] keyPositions, Object[] row, long ordinal) {
        if (keyPositions.length == 0) {
            return String.valueOf(ordinal);
        }
        return java.util.Arrays.stream(keyPositions)
                .mapToObj(i -> keyText(row[i]))
                .collect(Collectors.joining("_"))
                .replaceAll("[/\\\\]", "_");
    }

    private static String keyText(Object value) {
        return switch (value) {
            case null -> "null";
            case String s -> s;
            case BigDecimal d -> d.toPlainString();
            case LocalDateTime t -> CanonicalText.dateTime(t);
            default -> CanonicalText.of(value);
        };
    }

    public static byte[] bytes(Object value) {
        return switch (value) {
            case byte[] b -> b;
            case OleValue ole -> ole.raw();
            default ->
                throw new IllegalStateException("not bytes: " + value.getClass().getName());
        };
    }

    // ---------------------------------------------------------------- verify

    /**
     * What the output should hold for a source cell, compared by {@link #comparedType}: the OLE part, then the byte
     * count in {@code omit}; the bytes otherwise (a path is read back by {@link #readBack}).
     */
    public static Object expected(ColumnModel column, OlePart part, Object cell, ConvertOptions options) {
        Object value = part == null || cell == null ? cell : part(part, decoded(cell));
        if (value == null || !isPayload(column) || options.binary() != BinaryMode.OMIT) {
            return value;
        }
        return (long) bytes(value).length;
    }

    /** The type a column's values compare as: a byte count compares as a number. */
    public static AccessType comparedType(ColumnModel column, ConvertOptions options) {
        return storage(column, options) == Storage.SIZE ? AccessType.BIG_INT : column.type();
    }

    /**
     * An output value read back for comparison: a path becomes the file's bytes, read from under the output's
     * directory; a path that leads anywhere else, or to no file, becomes a marker that equals no source value.
     *
     * @param outputDirectory the directory the output is in, which paths are relative to
     */
    public static Object readBack(Object actual, ColumnModel column, ConvertOptions options, Path outputDirectory) {
        if (actual == null || storage(column, options) != Storage.PATH) {
            return actual;
        }
        Path base = outputDirectory.toAbsolutePath().normalize();
        Path file = base.resolve(actual.toString()).normalize();
        if (!file.startsWith(base) || !Files.isRegularFile(file)) {
            return "no file at " + actual;
        }
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            return "unreadable file " + actual;
        }
    }

    /** The positions of a table's primary-key columns in its rows. */
    public static int[] keyPositions(TableModel table) {
        if (table.primaryKey() == null) {
            return new int[0];
        }
        List<ColumnModel> columns = table.columns();
        return table.primaryKey().columnNames().stream()
                .mapToInt(name -> columns.indexOf(table.column(name).orElseThrow()))
                .toArray();
    }
}
