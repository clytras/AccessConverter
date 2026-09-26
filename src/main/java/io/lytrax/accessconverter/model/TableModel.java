package io.lytrax.accessconverter.model;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * An Access table. A linked table has a {@link LinkInfo}: without {@code --linked resolve} it has no columns or indexes
 * and no data ({@link #isLinked()}); read from its back-end, it is a table like any other, under its name here, and
 * its {@link LinkInfo} says where it came from.
 *
 * @param primaryKey null when the table has none (targets then emit none, never an invented one)
 * @param indexes the normalized secondary indexes, ordered by name
 * @param validation the table-level validation rule, which can span columns
 * @param rowCount Access's row count, for progress only; written counts come from the row stream
 * @param complex for a child table the SQL targets make of an attachment, multi-value or version-history column
 *     (08): where its rows come from; null for an Access table
 */
public record TableModel(
        String name,
        LinkInfo link,
        List<ColumnModel> columns,
        IndexModel primaryKey,
        List<IndexModel> indexes,
        String description,
        CheckRule validation,
        long rowCount,
        ComplexSource complex) {

    public TableModel {
        Objects.requireNonNull(name, "name");
        columns = List.copyOf(columns);
        indexes = List.copyOf(indexes);
    }

    public TableModel(
            String name,
            LinkInfo link,
            List<ColumnModel> columns,
            IndexModel primaryKey,
            List<IndexModel> indexes,
            String description,
            CheckRule validation,
            long rowCount) {
        this(name, link, columns, primaryKey, indexes, description, validation, rowCount, null);
    }

    /** A linked table that isn't read: its data lives elsewhere and nothing is written for it. */
    public boolean isLinked() {
        return link != null && link.readFrom() == null;
    }

    /** A linked table read from its back-end ({@code --linked resolve}): a table like any other. */
    public boolean isResolvedLink() {
        return link != null && link.readFrom() != null;
    }

    /** The column {@code --add-primary-key} added, which numbers the rows; null for a key Access has. */
    public String generatedKeyColumn() {
        return primaryKey != null && primaryKey.origin() == IndexModel.Origin.GENERATED
                ? primaryKey.columns().get(0).name()
                : null;
    }

    /** A child table made of a complex column's values, not a table of the Access database. */
    public boolean isComplexChild() {
        return complex != null;
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
     * @param readFrom the back-end file the table was read from ({@code --linked resolve}), or null when it isn't read
     */
    public record LinkInfo(String database, String remoteTable, boolean odbc, String readFrom) {
        private static final Pattern PASSWORD = Pattern.compile("(?i)\\b(PWD|PASSWORD)=[^;]*");

        public LinkInfo(String database, String remoteTable, boolean odbc) {
            this(database, remoteTable, odbc, null);
        }

        /** Whether the stored database, an ODBC connection string, holds a password. */
        public boolean hasPassword() {
            return database != null && PASSWORD.matcher(database).find();
        }

        /** The database for messages and inspect: an ODBC connection string's password is masked. */
        public String displayDatabase() {
            return database == null ? null : PASSWORD.matcher(database).replaceAll("$1=***");
        }
    }

    /**
     * Where a complex child table's rows come from: the values of {@code parentColumn} in each row of
     * {@code parentTable}, one child row per value, keyed by the complex id the parent column holds (08).
     *
     * @param kind {@code ATTACHMENT}, {@code MULTI_VALUE} or {@code VERSION_HISTORY}
     * @param parentKey the parent's primary key, whose order the child rows follow; null when it has none
     */
    public record ComplexSource(String parentTable, String parentColumn, AccessType kind, IndexModel parentKey) {

        /** The child's key: Access's complex value id. */
        public static final String ID = "id";

        // An attachment's fields (08)
        public static final String FILE_NAME = "file_name";
        public static final String FILE_TYPE = "file_type";
        public static final String FILE_DATA = "file_data";
        public static final String FILE_SIZE = "file_size";
        public static final String FILE_URL = "file_url";
        public static final String FILE_TIMESTAMP = "file_timestamp";
        public static final String FILE_FLAGS = "file_flags";

        /** A multi-value's element, or a version's text. */
        public static final String VALUE = "value";

        /** When a version was written. */
        public static final String MODIFIED = "modified";

        public ComplexSource {
            Objects.requireNonNull(parentTable, "parentTable");
            Objects.requireNonNull(parentColumn, "parentColumn");
            if (!kind.isComplex() || kind == AccessType.COMPLEX_UNSUPPORTED) {
                throw new IllegalArgumentException("not a complex column kind with values: " + kind);
            }
        }

        /** The column holding the parent's complex id, {@code <Column>_ref}. */
        public String refColumn() {
            return parentColumn + "_ref";
        }
    }
}
