package io.lytrax.accessconverter.target.json;

import io.lytrax.accessconverter.model.AccessType;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.ForeignKeyModel;
import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.target.ConvertOptions;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * What {@link JsonPlanner} decided (07): the tables and columns written with each column's JSON type, the keys and
 * indexes as Access has them, and every relationship between two written tables, enforced or not.
 *
 * @param linked linked tables, which are listed but have no data
 * @param profiled whether the data was profiled; without it {@code nullable} follows Access's own guarantees only
 * @param options the options planned with: {@code --binary} and {@code --ole-extract} decide how values are spelled
 */
public record JsonPlan(
        SchemaModel model,
        List<PlannedTable> tables,
        List<ForeignKeyModel> relationships,
        List<TableModel> linked,
        boolean profiled,
        ConvertOptions options) {

    public JsonPlan {
        tables = List.copyOf(tables);
        relationships = List.copyOf(relationships);
        linked = List.copyOf(linked);
    }

    public Optional<PlannedTable> table(String name) {
        return tables.stream().filter(t -> t.name().equalsIgnoreCase(name)).findFirst();
    }

    /**
     * A written table.
     *
     * @param primaryKey null when Access has none
     * @param indexes the normalized indexes whose columns are all written, by name
     * @param file its file in the ndjson layout, safe on every OS and unique in the directory
     */
    public record PlannedTable(
            TableModel source,
            List<PlannedColumn> columns,
            IndexModel primaryKey,
            List<IndexModel> indexes,
            String file) {

        public PlannedTable {
            columns = List.copyOf(columns);
            indexes = List.copyOf(indexes);
        }

        public String name() {
            return source.name();
        }

        public Optional<PlannedColumn> column(String name) {
            return columns.stream().filter(c -> c.name().equalsIgnoreCase(name)).findFirst();
        }
    }

    /**
     * A written column.
     *
     * @param sourceIndex where its value sits in a row of the source stream
     * @param nullable whether a value may be null: false only where Access guarantees a value (Yes/No, autonumbers,
     *     primary keys) or requires one and the profile found no NULL
     */
    public record PlannedColumn(ColumnModel source, int sourceIndex, JsonType type, boolean nullable) {
        public String name() {
            return source.name();
        }

        /**
         * A multi-value column's element as a column of its own (its values' type); a text element when Access's value
         * table couldn't be read, since those values are then read as text.
         */
        public PlannedColumn element() {
            ColumnModel element = source.element() != null
                    ? source.element()
                    : new ColumnModel(
                            "Value",
                            0,
                            AccessType.MEMO,
                            null,
                            null,
                            null,
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
            return new PlannedColumn(element, 0, JsonType.of(element.type()), true);
        }
    }

    /** How a column's values are spelled in JSON; the name is what the schema section says. */
    public enum JsonType {
        BOOLEAN,
        /** Byte: 0–255 (F-01). */
        UINT8,
        INT16,
        INT32,
        /** Large Number: a number, or a string with {@code --json-bigint string}. */
        INT64,
        /** Currency and Decimal: exact, as a number or a string ({@code --json-decimals}). */
        DECIMAL,
        /** Single: the shortest text that reads back as the same float. */
        FLOAT32,
        /** Double: the shortest text that reads back as the same double. */
        FLOAT64,
        /** ISO-8601 local date-time; Date/Time Extended always has 7 fraction digits. */
        DATETIME,
        STRING,
        /** A string as Access stores it, or an object with {@code --json-hyperlinks object}. */
        HYPERLINK,
        /** {@code {XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}}. */
        GUID,
        /** Base64 of the stored bytes. */
        BINARY,
        /**
         * The OLE object's exact stored bytes (08), spelled as {@code binary}; with {@code --ole-extract} an object
         * that adds the decoded kind, name, content type and content.
         */
        OLE,
        /** An attachment cell: an array of {@code {fileName, fileType, size, data|file, url, timestamp, flags}}. */
        ATTACHMENTS,
        /** A multi-value cell: an array of values, each spelled as the column's {@code element} type. */
        MULTI_VALUE,
        /** An append-only memo's history ({@code --include-version-history}): an array of {@code {value, modified}}. */
        VERSION_HISTORY,
        /** A complex column of a kind Jackcess can't read: Access's complex id. */
        COMPLEX_ID;

        public String wireName() {
            return switch (this) {
                case COMPLEX_ID -> "complexId";
                case MULTI_VALUE -> "multiValue";
                case VERSION_HISTORY -> "versionHistory";
                default -> name().toLowerCase(Locale.ROOT);
            };
        }

        public static JsonType of(AccessType type) {
            return switch (type) {
                case BOOLEAN -> BOOLEAN;
                case BYTE -> UINT8;
                case INT -> INT16;
                case LONG, AUTONUMBER_LONG -> INT32;
                case BIG_INT -> INT64;
                case MONEY, NUMERIC -> DECIMAL;
                case FLOAT -> FLOAT32;
                case DOUBLE -> FLOAT64;
                case SHORT_DATE_TIME, EXT_DATE_TIME -> DATETIME;
                case TEXT, MEMO -> STRING;
                case HYPERLINK -> HYPERLINK;
                case GUID, AUTONUMBER_GUID -> GUID;
                case BINARY, UNSUPPORTED -> BINARY;
                case OLE -> OLE;
                case ATTACHMENT -> ATTACHMENTS;
                case MULTI_VALUE -> MULTI_VALUE;
                case VERSION_HISTORY -> VERSION_HISTORY;
                case COMPLEX_UNSUPPORTED -> COMPLEX_ID;
            };
        }
    }
}
