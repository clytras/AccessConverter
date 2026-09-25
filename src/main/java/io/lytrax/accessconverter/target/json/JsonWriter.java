package io.lytrax.accessconverter.target.json;

import io.lytrax.accessconverter.model.AccessType;
import io.lytrax.accessconverter.model.CheckRule;
import io.lytrax.accessconverter.model.ColumnModel;
import io.lytrax.accessconverter.model.DefaultValue;
import io.lytrax.accessconverter.model.ForeignKeyModel;
import io.lytrax.accessconverter.model.IndexModel;
import io.lytrax.accessconverter.model.SchemaModel;
import io.lytrax.accessconverter.model.TableModel;
import io.lytrax.accessconverter.model.expr.Expr;
import io.lytrax.accessconverter.report.ConversionReport.TableResult;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.report.Issues;
import io.lytrax.accessconverter.source.AccessSource;
import io.lytrax.accessconverter.source.RowStream;
import io.lytrax.accessconverter.target.AtomicOutput;
import io.lytrax.accessconverter.target.BinaryCells;
import io.lytrax.accessconverter.target.BinaryFiles;
import io.lytrax.accessconverter.target.BinaryMode;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.PlanRules;
import io.lytrax.accessconverter.target.RowSource;
import io.lytrax.accessconverter.target.TableFailure;
import io.lytrax.accessconverter.target.WriteOutcome;
import io.lytrax.accessconverter.target.json.JsonOptions.Hyperlinks;
import io.lytrax.accessconverter.target.json.JsonOptions.Layout;
import io.lytrax.accessconverter.target.json.JsonOptions.NumberForm;
import io.lytrax.accessconverter.target.json.JsonOptions.Rows;
import io.lytrax.accessconverter.target.json.JsonPlan.PlannedColumn;
import io.lytrax.accessconverter.target.json.JsonPlan.PlannedTable;
import io.lytrax.accessconverter.target.json.JsonValues.Hyperlink;
import io.lytrax.accessconverter.value.AttachmentValue;
import io.lytrax.accessconverter.value.ComplexRef;
import io.lytrax.accessconverter.value.ComplexValues;
import io.lytrax.accessconverter.value.OleContent;
import io.lytrax.accessconverter.value.OleValue;
import io.lytrax.accessconverter.value.VersionValue;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.ObjectWriteContext;

/**
 * Writes the JSON export a {@link JsonPlan} describes (07), as one document or as an ndjson directory.
 *
 * <p>Everything streams: one pass per table, one row in memory at a time, and the largest allocation is one value
 * (03, Memory). A table's rows are written by their own compact generator, one row per line, after the enclosing
 * structure is flushed. So when a table fails, the file is cut back to where its rows began and the table is left
 * empty, as the SQL targets roll it back: a JSON file never holds part of a table without saying so. The output is
 * built as {@code <output>.partial} and renamed into place only when it is complete (F-42).
 */
public final class JsonWriter {

    private static final int BUFFER = 1 << 16;

    private final RowSource source;
    private final JsonPlan plan;
    private final ConvertOptions options;
    private final JsonOptions json;
    private final String producer;
    private final Issues issues;
    private final List<TableResult> results = new ArrayList<>();
    private boolean tableFailed;
    private BinaryFiles files;
    /** The table and row being written, for file names and reports. */
    private BinaryCells cells;

    private Object[] currentRow;
    private long currentOrdinal;

    private JsonWriter(
            RowSource source, JsonPlan plan, ConvertOptions options, JsonOptions json, String producer, Issues issues) {
        this.source = source;
        this.plan = plan;
        this.options = options;
        this.json = json;
        this.producer = producer;
        this.issues = issues;
    }

    /**
     * @param output the document, or the ndjson directory
     * @param producer the tool and its version ({@code AccessConverter 3.0.0})
     */
    public static WriteOutcome write(
            AccessSource source,
            JsonPlan plan,
            Path output,
            ConvertOptions options,
            JsonOptions json,
            String producer,
            Issues issues)
            throws IOException {
        // JSON inlines attachment, multi-value and version-history values, so their cells are read with them (08)
        return write(t -> source.rows(t, true), plan, output, options, json, producer, issues);
    }

    /** As {@link #write}, reading the rows from somewhere else; for the {@code --on-table-error} test. */
    static WriteOutcome write(
            RowSource source,
            JsonPlan plan,
            Path output,
            ConvertOptions options,
            JsonOptions json,
            String producer,
            Issues issues)
            throws IOException {
        JsonWriter writer = new JsonWriter(source, plan, options, json, producer, issues);
        writer.files = options.binary() == BinaryMode.FILES ? new BinaryFiles(output) : null;
        try {
            if (json.layout() == Layout.DOCUMENT) {
                AtomicOutput.write(output, partial -> {
                    writer.document(partial);
                    return null;
                });
            } else {
                AtomicOutput.writeDirectory(output, JsonWriter::isNdjsonFile, partial -> {
                    writer.ndjson(partial);
                    return null;
                });
            }
        } catch (IOException | RuntimeException e) {
            if (writer.files != null) {
                writer.files.discard();
            }
            throw e;
        }
        if (writer.files != null) {
            writer.files.commit();
        }
        return new WriteOutcome(writer.results, writer.tableFailed);
    }

    /** Whether a file name is one an ndjson export consists of. */
    public static boolean isNdjsonFile(String fileName) {
        return fileName.equals(JsonFormat.NDJSON_SCHEMA_FILE) || fileName.endsWith(JsonFormat.NDJSON_EXTENSION);
    }

    // ---------------------------------------------------------------- layouts

    private void document(Path file) throws IOException {
        try (Output out = new Output(file)) {
            JsonFormat.Printer printer = new JsonFormat.Printer();
            JsonGenerator g = JsonFormat.FACTORY.createGenerator(JsonFormat.pretty(printer), out.stream);
            g.writeStartObject();
            header(g, "document");
            g.writeObjectPropertyStart("data");
            for (PlannedTable table : plan.tables()) {
                g.writeName(table.name());
                g.writeStartArray();
                g.flush();
                long rows = rows(table, out, printer.rowIndent(), ",");
                if (rows > 0) {
                    printer.valuesWrittenElsewhere();
                }
                g.writeEndArray();
            }
            g.writeEndObject();
            g.writeEndObject();
            g.writeRaw('\n');
            g.close();
        }
    }

    private void ndjson(Path directory) throws IOException {
        for (PlannedTable table : plan.tables()) {
            try (Output out = new Output(directory.resolve(table.file()))) {
                rows(table, out, "", "");
            }
        }
        try (Output out = new Output(directory.resolve(JsonFormat.NDJSON_SCHEMA_FILE))) {
            JsonGenerator g =
                    JsonFormat.FACTORY.createGenerator(JsonFormat.pretty(new JsonFormat.Printer()), out.stream);
            g.writeStartObject();
            header(g, "ndjson");
            g.writeObjectPropertyStart("files");
            for (PlannedTable table : plan.tables()) {
                g.writeStringProperty(table.name(), table.file());
            }
            g.writeEndObject();
            g.writeEndObject();
            g.writeRaw('\n');
            g.close();
        }
    }

    /** A file written through a channel, so that it can be cut back to an earlier length. */
    private static final class Output implements AutoCloseable {
        final FileChannel channel;
        final OutputStream stream;

        Output(Path file) throws IOException {
            channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            stream = new BufferedOutputStream(Channels.newOutputStream(channel), BUFFER);
        }

        /** Everything written so far is in the file; the position is its length. */
        long position() throws IOException {
            stream.flush();
            return channel.position();
        }

        void truncate(long length) throws IOException {
            stream.flush();
            channel.truncate(length);
            channel.position(length);
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }

    // ---------------------------------------------------------------- header and schema

    private void header(JsonGenerator g, String layout) {
        g.writeStringProperty("format", JsonFormat.NAME);
        g.writeNumberProperty("formatVersion", JsonFormat.VERSION);
        g.writeStringProperty("producer", producer);
        g.writeStringProperty("layout", layout);
        g.writeObjectPropertyStart("encoding");
        g.writeStringProperty("rows", lower(json.rows()));
        g.writeStringProperty("bigint", lower(json.bigint()));
        g.writeStringProperty("decimals", lower(json.decimals()));
        g.writeStringProperty("hyperlinks", lower(json.hyperlinks()));
        g.writeStringProperty("binary", binaryEncoding(options.binary()));
        g.writeStringProperty("ole", options.oleExtract() ? "extracted" : "raw");
        g.writeEndObject();
        SchemaModel.Source src = plan.model().source();
        g.writeObjectPropertyStart("source");
        g.writeStringProperty("file", src.fileName());
        g.writeStringProperty("fileFormat", src.fileFormat());
        if (src.codePage() != null) {
            g.writeNumberProperty("codePage", src.codePage());
        }
        g.writeStringProperty("charset", src.charset());
        if (json.stamp()) {
            g.writeStringProperty(
                    "exported", Instant.now().truncatedTo(ChronoUnit.SECONDS).toString());
        }
        g.writeEndObject();
        if (json.schema()) {
            schema(g, plan);
            for (TableModel linked : plan.linked()) {
                if (linked.link().hasPassword()) {
                    // Kept verbatim, as Access stores it; whoever shares the file should know what it carries
                    issues.add(
                            IssueCode.LINKED_CONNECTION_PASSWORD,
                            linked.name(),
                            null,
                            "the schema's linkedTables entry holds this table's ODBC connection string as Access"
                                    + " stores it, password included; remove it before sharing the file, or"
                                    + " export with --no-schema");
                }
            }
        }
    }

    /** The {@code schema} property: every written table, the linked tables, and the relationships between tables. */
    static void schema(JsonGenerator g, JsonPlan plan) {
        g.writeObjectPropertyStart("schema");
        g.writeArrayPropertyStart("tables");
        for (PlannedTable table : plan.tables()) {
            table(g, table);
        }
        g.writeEndArray();
        g.writeArrayPropertyStart("linkedTables");
        for (TableModel linked : plan.linked()) {
            g.writeStartObject();
            g.writeStringProperty("name", linked.name());
            g.writeStringProperty("database", linked.link().database());
            g.writeStringProperty("remoteTable", linked.link().remoteTable());
            g.writeBooleanProperty("odbc", linked.link().odbc());
            g.writeEndObject();
        }
        g.writeEndArray();
        g.writeArrayPropertyStart("relationships");
        for (ForeignKeyModel fk : plan.relationships()) {
            relationship(g, fk);
        }
        g.writeEndArray();
        g.writeEndObject();
    }

    private static void table(JsonGenerator g, PlannedTable table) {
        TableModel source = table.source();
        g.writeStartObject();
        g.writeStringProperty("name", table.name());
        g.writeStringProperty("description", source.description());
        g.writeArrayPropertyStart("columns");
        for (PlannedColumn column : table.columns()) {
            column(g, column);
        }
        g.writeEndArray();
        g.writeName("primaryKey");
        if (table.primaryKey() == null) {
            g.writeNull();
        } else {
            index(g, table.primaryKey());
        }
        g.writeArrayPropertyStart("indexes");
        for (IndexModel index : table.indexes()) {
            index(g, index);
        }
        g.writeEndArray();
        g.writeName("validationRule");
        rule(g, source.validation());
        g.writeEndObject();
    }

    private static void column(JsonGenerator g, PlannedColumn planned) {
        ColumnModel c = planned.source();
        g.writeStartObject();
        g.writeStringProperty("name", c.name());
        g.writeStringProperty("type", planned.type().wireName());
        g.writeStringProperty("accessType", accessType(c.type()));
        g.writeBooleanProperty("nullable", planned.nullable());
        g.writeBooleanProperty("required", c.required());
        if (c.type() == AccessType.AUTONUMBER_LONG) {
            g.writeStringProperty("autoNumber", c.isRandomAutoNumber() ? "random" : "increment");
        } else if (c.type() == AccessType.AUTONUMBER_GUID) {
            g.writeStringProperty("autoNumber", "guid");
        }
        if (c.length() != null) {
            g.writeNumberProperty("length", c.length());
        }
        if (c.type() == AccessType.MONEY) {
            g.writeNumberProperty("precision", 19);
            g.writeNumberProperty("scale", 4);
        } else if (c.type() == AccessType.NUMERIC) {
            if (c.precision() != null) {
                g.writeNumberProperty("precision", c.precision());
            }
            if (c.scale() != null) {
                g.writeNumberProperty("scale", c.scale());
            }
        }
        if (c.type().isText()) {
            g.writeBooleanProperty("allowZeroLength", c.allowZeroLength());
        }
        // A Random autonumber's GenUniqueID() is its New Values setting, which autoNumber already says
        if (c.defaultValue() != null && !c.isRandomAutoNumber()) {
            g.writeName("default");
            defaultValue(g, c.defaultValue());
        }
        if (c.validation() != null) {
            g.writeName("validationRule");
            rule(g, c.validation());
        }
        optional(g, "description", c.description());
        optional(g, "format", c.format());
        if (c.decimalPlaces() != null) {
            g.writeNumberProperty("decimalPlaces", c.decimalPlaces());
        }
        if (c.richText()) {
            g.writeBooleanProperty("richText", true);
        }
        optional(g, "calculatedExpression", c.calculatedExpression());
        if (c.appendOnly()) {
            g.writeBooleanProperty("appendOnly", true);
        }
        if (c.hidden()) {
            g.writeBooleanProperty("hidden", true);
        }
        if (planned.type() == JsonPlan.JsonType.MULTI_VALUE) {
            PlannedColumn element = planned.element();
            g.writeObjectPropertyStart("element");
            g.writeStringProperty("type", element.type().wireName());
            g.writeStringProperty("accessType", accessType(element.source().type()));
            if (element.source().length() != null) {
                g.writeNumberProperty("length", element.source().length());
            }
            if (element.source().precision() != null) {
                g.writeNumberProperty("precision", element.source().precision());
            }
            if (element.source().scale() != null) {
                g.writeNumberProperty("scale", element.source().scale());
            }
            g.writeEndObject();
        }
        g.writeEndObject();
    }

    /** The {@code encoding.binary} spelling of a {@code --binary} mode. */
    static String binaryEncoding(BinaryMode mode) {
        return mode == BinaryMode.INLINE ? "base64" : mode.label();
    }

    /** Jackcess's type name. An autonumber is its storage type, LONG or GUID; {@code autoNumber} says which kind. */
    static String accessType(AccessType type) {
        return switch (type) {
            case AUTONUMBER_LONG -> "LONG";
            case AUTONUMBER_GUID -> "GUID";
            default -> type.name();
        };
    }

    /**
     * A default as Access wrote it, plus what it means where that is known: a literal's value in its own JSON form
     * (not converted to the column's type), or the current date/time or a new GUID. Nothing is evaluated.
     */
    static void defaultValue(JsonGenerator g, DefaultValue d) {
        g.writeStartObject();
        g.writeStringProperty("access", d.raw());
        if (!d.isTranslated()) {
            g.writeStringProperty("kind", "unsupported");
            g.writeStringProperty("reason", d.unsupportedReason());
        } else {
            switch (d.expr()) {
                case Expr.NullLiteral n -> g.writeStringProperty("kind", "null");
                case Expr.BooleanLiteral b -> {
                    g.writeStringProperty("kind", "literal");
                    g.writeBooleanProperty("value", b.value());
                }
                case Expr.NumberLiteral n -> {
                    g.writeStringProperty("kind", "literal");
                    g.writeName("value");
                    g.writeNumber(n.value());
                }
                case Expr.StringLiteral s -> {
                    g.writeStringProperty("kind", "literal");
                    g.writeName("value");
                    string(g, s.value());
                }
                case Expr.DateTimeLiteral t -> {
                    g.writeStringProperty("kind", "literal");
                    g.writeStringProperty("value", JsonValues.dateTime(t.value()));
                }
                case Expr.CurrentDateTime now ->
                    g.writeStringProperty(
                            "kind",
                            switch (now.part()) {
                                case NOW -> "currentTimestamp";
                                case DATE -> "currentDate";
                                case TIME -> "currentTime";
                            });
                case Expr.NewGuid guid -> g.writeStringProperty("kind", "newGuid");
                default -> g.writeStringProperty("kind", "expression");
            }
        }
        g.writeEndObject();
    }

    private static void rule(JsonGenerator g, CheckRule rule) {
        if (rule == null) {
            g.writeNull();
            return;
        }
        g.writeStartObject();
        g.writeStringProperty("access", rule.raw());
        g.writeStringProperty("validationText", rule.validationText());
        g.writeEndObject();
    }

    private static void index(JsonGenerator g, IndexModel index) {
        g.writeStartObject();
        g.writeStringProperty("name", index.name());
        g.writeArrayPropertyStart("columns");
        for (IndexModel.IndexColumn c : index.columns()) {
            g.writeStartObject();
            g.writeStringProperty("name", c.name());
            g.writeStringProperty("order", c.ascending() ? "asc" : "desc");
            g.writeEndObject();
        }
        g.writeEndArray();
        g.writeBooleanProperty("unique", index.unique());
        g.writeBooleanProperty("ignoreNulls", index.ignoreNulls());
        g.writeBooleanProperty("required", index.required());
        g.writeArrayPropertyStart("accessNames");
        index.sourceNames().forEach(g::writeString);
        g.writeEndArray();
        g.writeEndObject();
    }

    private static void relationship(JsonGenerator g, ForeignKeyModel fk) {
        g.writeStartObject();
        g.writeStringProperty("name", fk.name());
        side(g, "parent", fk.parentTable(), fk.parentColumns());
        side(g, "child", fk.childTable(), fk.childColumns());
        g.writeBooleanProperty("enforced", fk.enforced());
        g.writeStringProperty("onUpdate", camel(fk.onUpdate()));
        g.writeStringProperty("onDelete", camel(fk.onDelete()));
        g.writeBooleanProperty("oneToOne", fk.oneToOne());
        g.writeStringProperty("join", camel(fk.join()));
        g.writeEndObject();
    }

    private static void side(JsonGenerator g, String name, String table, List<String> columns) {
        g.writeObjectPropertyStart(name);
        g.writeStringProperty("table", table);
        g.writeArrayPropertyStart("columns");
        columns.forEach(g::writeString);
        g.writeEndArray();
        g.writeEndObject();
    }

    private static void optional(JsonGenerator g, String name, String value) {
        if (value != null) {
            g.writeStringProperty(name, value);
        }
    }

    // ---------------------------------------------------------------- data

    /**
     * Writes one table's rows after what is already in {@code out}, each preceded by {@code indent} and separated by
     * {@code separator}; an ndjson row ends with a line feed instead. On failure the file is cut back to where the
     * rows began, so the table is empty.
     *
     * @return the rows written
     */
    private long rows(PlannedTable table, Output out, String indent, String separator) throws IOException {
        long start = out.position();
        long read = 0;
        long written = 0;
        boolean ndjson = json.layout() == Layout.NDJSON;
        JsonGenerator g = JsonFormat.FACTORY.createGenerator(ObjectWriteContext.empty(), out.stream);
        cells = new BinaryCells(options, files, issues, table.source(), table.name());
        try {
            RowStream rows = source.of(table.source());
            while (rows.hasNext()) {
                Object[] row = rows.next();
                read++;
                currentRow = row;
                currentOrdinal = read;
                if (!ndjson) {
                    g.writeRaw(read == 1 ? indent : separator + indent);
                }
                row(g, table, row);
                if (ndjson) {
                    g.writeRaw('\n');
                }
            }
            g.close();
            written = read;
        } catch (IOException | RuntimeException e) {
            tableFailed = true;
            g.close(); // AUTO_CLOSE_CONTENT is off: this only flushes, and the cut below removes it
            out.truncate(start);
            if (files != null) {
                files.discardTable(table.name(), issues);
            }
            TableFailure.handle(issues, options, table.name(), 0, e);
        }
        results.add(new TableResult(table.name(), read, written));
        return written;
    }

    private void row(JsonGenerator g, PlannedTable table, Object[] row) {
        List<PlannedColumn> columns = table.columns();
        if (json.rows() == Rows.OBJECT) {
            g.writeStartObject();
            for (PlannedColumn column : columns) {
                g.writeName(column.name());
                value(g, table, column, row[column.sourceIndex()]);
            }
            g.writeEndObject();
        } else {
            g.writeStartArray();
            for (PlannedColumn column : columns) {
                value(g, table, column, row[column.sourceIndex()]);
            }
            g.writeEndArray();
        }
    }

    /** One value, spelled as 07's table says. Nothing is substituted: a NULL stays null. */
    private void value(JsonGenerator g, PlannedTable table, PlannedColumn column, Object value) {
        if (value == null) {
            if (!column.nullable()) {
                throw new IllegalStateException("column " + table.name() + "." + column.name()
                        + " is not nullable because Access requires a value, but a row holds NULL");
            }
            g.writeNull();
            return;
        }
        switch (column.type()) {
            case BOOLEAN -> g.writeBoolean((Boolean) value);
            case UINT8, INT16, INT32 -> g.writeNumber(((Number) value).intValue());
            case INT64 -> {
                long l = (Long) value;
                if (json.bigint() == NumberForm.STRING) {
                    g.writeString(Long.toString(l));
                } else {
                    g.writeNumber(l);
                }
            }
            case DECIMAL -> {
                BigDecimal d = (BigDecimal) value;
                if (json.decimals() == NumberForm.STRING) {
                    g.writeString(d.toPlainString());
                } else {
                    g.writeNumber(d);
                }
            }
            case FLOAT32 -> {
                float f = (Float) value;
                if (finite(table, column, f)) {
                    g.writeNumber(Float.toString(f)); // the shortest text that reads back as f (JDK 19+)
                } else {
                    g.writeNull();
                }
            }
            case FLOAT64 -> {
                double d = (Double) value;
                if (finite(table, column, d)) {
                    g.writeNumber(Double.toString(d));
                } else {
                    g.writeNull();
                }
            }
            case DATETIME ->
                g.writeString(
                        column.source().type() == AccessType.EXT_DATE_TIME
                                ? JsonValues.extendedDateTime((LocalDateTime) value)
                                : JsonValues.dateTime((LocalDateTime) value));
            case STRING, GUID -> text(g, table, column, (String) value);
            case HYPERLINK -> {
                if (json.hyperlinks() == Hyperlinks.STRING) {
                    text(g, table, column, (String) value);
                } else {
                    Hyperlink link = Hyperlink.parse((String) value);
                    g.writeStartObject();
                    g.writeName("display");
                    nullableText(g, table, column, link.display());
                    g.writeName("address");
                    nullableText(g, table, column, link.address());
                    g.writeName("subAddress");
                    nullableText(g, table, column, link.subAddress());
                    g.writeName("screenTip");
                    nullableText(g, table, column, link.screenTip());
                    g.writeEndObject();
                }
            }
            case BINARY -> binary(g, table, column.name(), column.name(), rowKey(), null, (byte[]) value);
            case OLE -> ole(g, table, column, (OleValue) value);
            case ATTACHMENTS -> attachments(g, table, column, (ComplexValues) value);
            case MULTI_VALUE -> {
                PlannedColumn element = column.element();
                g.writeStartArray();
                for (Object item : ((ComplexValues) value).items()) {
                    value(g, table, element, item);
                }
                g.writeEndArray();
            }
            case VERSION_HISTORY -> {
                g.writeStartArray();
                for (Object item : ((ComplexValues) value).items()) {
                    VersionValue version = (VersionValue) item;
                    g.writeStartObject();
                    g.writeName("value");
                    nullableText(g, table, column, version.value());
                    g.writeName("modified");
                    dateTime(g, version.modified());
                    g.writeEndObject();
                }
                g.writeEndArray();
            }
            case COMPLEX_ID -> g.writeNumber(((ComplexRef) value).complexId());
        }
    }

    private String rowKey() {
        return cells.rowKey(currentRow, currentOrdinal);
    }

    private static void dateTime(JsonGenerator g, LocalDateTime value) {
        if (value == null) {
            g.writeNull();
        } else {
            g.writeString(JsonValues.dateTime(value));
        }
    }

    /**
     * Bytes as {@code --binary} says (08): base64 inline, {@code {file, size}} in files mode, {@code {size}} in omit
     * mode. Null stays null.
     *
     * @param directory the column's directory in files mode
     * @param stem the file name's row part
     * @param name the data's own file name, or null
     */
    private void binary(
            JsonGenerator g,
            PlannedTable table,
            String column,
            String directory,
            String stem,
            String name,
            byte[] bytes) {
        if (bytes == null) {
            g.writeNull();
            return;
        }
        switch (options.binary()) {
            case INLINE -> g.writeBinary(bytes);
            case FILES -> {
                g.writeStartObject();
                g.writeStringProperty("file", file(table, column, directory, stem, name, bytes));
                g.writeNumberProperty("size", bytes.length);
                g.writeEndObject();
            }
            case OMIT -> {
                omitted(table, column);
                g.writeStartObject();
                g.writeNumberProperty("size", bytes.length);
                g.writeEndObject();
            }
        }
    }

    private String file(PlannedTable table, String column, String directory, String stem, String name, byte[] bytes) {
        try {
            return files.write(table.name(), directory, stem, name, bytes);
        } catch (IOException e) {
            // A write failure, not a read failure: TABLE_WRITE_FAILED
            throw new IllegalStateException(
                    "writing a file for " + table.name() + "." + column + " failed: " + e.getMessage(), e);
        }
    }

    private void omitted(PlannedTable table, String column) {
        issues.add(
                IssueCode.BINARY_OMITTED,
                table.name(),
                column,
                "the bytes are not written (--binary omit); the output holds each value's size in bytes");
    }

    /** An OLE value: its raw bytes, or with {@code --ole-extract} an object with what decoding found (08). */
    private void ole(JsonGenerator g, PlannedTable table, PlannedColumn column, OleValue value) {
        if (!options.oleExtract()) {
            binary(g, table, column.name(), column.name(), rowKey(), null, value.raw());
            return;
        }
        OleContent content = BinaryCells.decoded(value);
        if (!content.decoded()) {
            issues.add(
                    IssueCode.OLE_UNDECODABLE,
                    table.name(),
                    column.name(),
                    "OLE values that can't be decoded keep only their raw bytes; the first: " + content.problem(),
                    rowKey());
        }
        g.writeStartObject();
        g.writeName("raw");
        binary(g, table, column.name(), column.name(), rowKey(), null, value.raw());
        g.writeName("kind");
        if (content.decoded()) {
            g.writeString(content.kind().label());
        } else {
            g.writeNull();
        }
        g.writeName("name");
        nullableText(g, table, column, content.name());
        g.writeName("mime");
        nullableText(g, table, column, content.mime());
        g.writeName("content");
        binary(
                g,
                table,
                column.name(),
                column.name() + PlanRules.OlePart.CONTENT.suffix(),
                rowKey(),
                content.kind() == OleContent.Kind.PACKAGE ? content.name() : null,
                content.content());
        g.writeEndObject();
    }

    /**
     * An attachment cell: one object per file, in Access's order (08). The file's bytes are {@code data} (base64),
     * {@code file} (a path) or absent ({@code --binary omit}); {@code size} is always there.
     */
    private void attachments(JsonGenerator g, PlannedTable table, PlannedColumn column, ComplexValues value) {
        g.writeStartArray();
        int n = 0;
        for (Object item : value.items()) {
            AttachmentValue attachment = (AttachmentValue) item;
            n++;
            g.writeStartObject();
            g.writeName("fileName");
            nullableText(g, table, column, attachment.fileName());
            g.writeName("fileType");
            nullableText(g, table, column, attachment.fileType());
            g.writeName("size");
            if (attachment.size() == null) {
                g.writeNull();
            } else {
                g.writeNumber(attachment.size());
            }
            switch (options.binary()) {
                case INLINE -> {
                    g.writeName("data");
                    if (attachment.data() == null) {
                        g.writeNull();
                    } else {
                        g.writeBinary(attachment.data());
                    }
                }
                case FILES -> {
                    g.writeName("file");
                    if (attachment.data() == null) {
                        g.writeNull();
                    } else {
                        g.writeString(file(
                                table,
                                column.name(),
                                column.name(),
                                rowKey() + "-" + n,
                                attachment.fileName(),
                                attachment.data()));
                    }
                }
                case OMIT -> {
                    if (attachment.data() != null) {
                        omitted(table, column.name());
                    }
                }
            }
            g.writeName("url");
            nullableText(g, table, column, attachment.url());
            g.writeName("timestamp");
            dateTime(g, attachment.timestamp());
            g.writeName("flags");
            if (attachment.flags() == null) {
                g.writeNull();
            } else {
                g.writeNumber(attachment.flags());
            }
            g.writeEndObject();
        }
        g.writeEndArray();
    }

    /** JSON has no NaN or Infinity: written as null and reported. */
    private boolean finite(PlannedTable table, PlannedColumn column, double value) {
        if (Double.isFinite(value)) {
            return true;
        }
        issues.add(
                IssueCode.DOUBLE_NON_FINITE,
                table.name(),
                column.name(),
                "written as null: JSON has no NaN or Infinity",
                String.valueOf(value));
        return false;
    }

    private void nullableText(JsonGenerator g, PlannedTable table, PlannedColumn column, String value) {
        if (value == null) {
            g.writeNull();
        } else {
            text(g, table, column, value);
        }
    }

    /** Text as it is; a lone surrogate, which UTF-8 has no bytes for, as a {@code \\uXXXX} escape, reported. */
    private void text(JsonGenerator g, PlannedTable table, PlannedColumn column, String value) {
        if (!JsonValues.hasUnpairedSurrogate(value)) {
            g.writeString(value);
            return;
        }
        issues.add(
                IssueCode.TEXT_UNPAIRED_SURROGATE,
                table.name(),
                column.name(),
                "an unpaired UTF-16 surrogate is written as a \\uXXXX escape: valid JSON that reads back as the same"
                        + " text, though some strict parsers reject it");
        g.writeRawValue(JsonValues.quotedKeepingSurrogates(value));
    }

    /** A string in the schema section, where a lone surrogate is kept the same way but not reported. */
    private static void string(JsonGenerator g, String value) {
        if (JsonValues.hasUnpairedSurrogate(value)) {
            g.writeRawValue(JsonValues.quotedKeepingSurrogates(value));
        } else {
            g.writeString(value);
        }
    }

    private static String lower(Enum<?> value) {
        return value.name().toLowerCase(Locale.ROOT);
    }

    /** {@code NO_ACTION} → {@code noAction}. */
    static String camel(Enum<?> value) {
        String[] words = value.name().toLowerCase(Locale.ROOT).split("_");
        StringBuilder text = new StringBuilder(words[0]);
        for (int i = 1; i < words.length; i++) {
            text.append(Character.toUpperCase(words[i].charAt(0))).append(words[i].substring(1));
        }
        return text.toString();
    }
}
