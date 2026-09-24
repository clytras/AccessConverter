package com.lytrax.accessconverter.target.json;

import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.target.json.JsonPlan.PlannedColumn;
import com.lytrax.accessconverter.target.json.JsonPlan.PlannedTable;
import com.lytrax.accessconverter.target.json.JsonValues.Hyperlink;
import com.lytrax.accessconverter.verify.RowComparison;
import com.lytrax.accessconverter.verify.VerifyResult;
import com.lytrax.accessconverter.verify.VerifyResult.Difference;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectWriteContext;

/**
 * Compares a JSON export with the plan that describes it and with the source data (09, The verifier), reading it back
 * as a stream so memory stays flat however large it is.
 *
 * <ul>
 *   <li>The header: format, version and layout, and the source it names.
 *   <li>The schema section, which must be exactly what the source says it should be (the plan, rendered); a
 *       difference is named by its path, such as {@code schema.tables[2].columns[0].nullable}.
 *   <li>Every value of every row, in order. Values are decoded here from the JSON, independently of the writer, as 07
 *       specifies them (a Single read back as a float, a decimal read exactly, a date parsed, base64 decoded), after
 *       checking that each has the spelling the file's {@code encoding} promises (a Date/Time Extended with 7 fraction
 *       digits, a Large Number as a string with {@code bigint: string}).
 * </ul>
 */
public final class JsonVerifier {

    private static final Pattern DATE_TIME =
            Pattern.compile("[+]?\\d{4,}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d(\\.\\d{3}|\\.\\d{6}|\\.\\d{9})?");
    private static final Pattern EXTENDED_DATE_TIME =
            Pattern.compile("[+]?\\d{4,}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{7}");
    private static final Pattern PLAIN_NUMBER = Pattern.compile("-?\\d+(\\.\\d+)?");

    private final AccessSource source;
    private final JsonPlan plan;
    private final VerifyResult.Collector differences = new VerifyResult.Collector();
    private Encoding encoding = Encoding.DEFAULT;

    private JsonVerifier(AccessSource source, JsonPlan plan) {
        this.source = source;
        this.plan = plan;
    }

    /** @param output a JSON document, or an ndjson directory */
    public static VerifyResult verify(AccessSource source, JsonPlan plan, Path output) throws IOException {
        JsonVerifier verifier = new JsonVerifier(source, plan);
        try {
            if (Files.isDirectory(output)) {
                verifier.ndjson(output);
            } else {
                verifier.document(output);
            }
        } catch (JacksonException e) {
            verifier.differences.add(new Difference(
                    null, null, "the output", "valid JSON", "not valid JSON (" + e.getOriginalMessage() + ")"));
        } catch (Malformed e) {
            verifier.differences.add(new Difference(null, null, "the output's structure", e.expected, e.found));
        }
        return verifier.differences.result();
    }

    /** Whether a file looks like a JSON document: its first non-blank byte opens an object. */
    public static boolean isJson(Path file) throws IOException {
        try (InputStream in = Files.newInputStream(file)) {
            for (int b = in.read(); b != -1; b = in.read()) {
                if (!Character.isWhitespace(b)) {
                    return b == '{';
                }
            }
        }
        return false;
    }

    /** Whether a directory holds an ndjson export's {@code schema.json}. */
    public static boolean isNdjson(Path directory) {
        return Files.isRegularFile(directory.resolve(JsonFormat.NDJSON_SCHEMA_FILE));
    }

    /** The spellings the file says it uses. */
    private record Encoding(String rows, String bigint, String decimals, String hyperlinks) {
        static final Encoding DEFAULT = new Encoding("object", "number", "number", "string");

        static Encoding of(Object tree) {
            if (!(tree instanceof Map<?, ?> map)) {
                return DEFAULT;
            }
            return new Encoding(
                    text(map.get("rows"), DEFAULT.rows),
                    text(map.get("bigint"), DEFAULT.bigint),
                    text(map.get("decimals"), DEFAULT.decimals),
                    text(map.get("hyperlinks"), DEFAULT.hyperlinks));
        }

        private static String text(Object value, String otherwise) {
            return value instanceof String s ? s : otherwise;
        }
    }

    // ---------------------------------------------------------------- layouts

    private void document(Path file) throws IOException {
        try (JsonParser p = JsonFormat.FACTORY.createParser(JsonFormat.reading(), file)) {
            expect(p, JsonToken.START_OBJECT, "the document");
            Map<String, Object> header = new LinkedHashMap<>();
            boolean data = false;
            while (p.nextToken() == JsonToken.PROPERTY_NAME) {
                String name = p.currentName();
                p.nextToken();
                if (name.equals("data")) {
                    checkHeader(header, "document");
                    data(p);
                    data = true;
                } else {
                    if (data) {
                        differences.add(null, null, "the property " + name, "before data", "after data");
                    }
                    header.put(name, tree(p));
                }
            }
            if (!data) {
                checkHeader(header, "document");
                differences.add(null, null, "the data property", "present", "missing");
            }
            if (p.nextToken() != null) {
                differences.add(null, null, "the document", "one JSON object", "more after it");
            }
        }
    }

    private void ndjson(Path directory) throws IOException {
        Path schemaFile = directory.resolve(JsonFormat.NDJSON_SCHEMA_FILE);
        if (!Files.isRegularFile(schemaFile)) {
            differences.add(null, null, JsonFormat.NDJSON_SCHEMA_FILE, "present", "missing");
            return;
        }
        Map<String, Object> header;
        try (JsonParser p = JsonFormat.FACTORY.createParser(JsonFormat.reading(), schemaFile)) {
            p.nextToken();
            Object tree = tree(p);
            header = tree instanceof Map<?, ?> map ? cast(map) : new LinkedHashMap<>();
        }
        Map<String, Object> files = header.get("files") instanceof Map<?, ?> map ? cast(map) : Map.of();
        header.remove("files");
        checkHeader(header, "ndjson");
        List<String> expected = new ArrayList<>();
        for (PlannedTable table : plan.tables()) {
            expected.add(table.name() + " -> " + table.file());
        }
        List<String> actual = new ArrayList<>();
        files.forEach((table, file) -> actual.add(table + " -> " + file));
        if (!expected.equals(actual)) {
            differences.add(null, null, "the files", String.join(", ", expected), String.join(", ", actual));
        }
        try (var entries = Files.list(directory)) {
            List<String> extra = entries.map(f -> f.getFileName().toString())
                    .filter(f -> !f.equals(JsonFormat.NDJSON_SCHEMA_FILE))
                    .filter(f -> plan.tables().stream().noneMatch(t -> t.file().equals(f)))
                    .sorted()
                    .toList();
            if (!extra.isEmpty()) {
                differences.add(null, null, "the directory", "only the export's files", String.join(", ", extra));
            }
        }
        for (PlannedTable table : plan.tables()) {
            Path file = directory.resolve(table.file());
            if (!Files.isRegularFile(file)) {
                differences.add(table.name(), null, "the file " + table.file(), "present", "missing");
                continue;
            }
            try (BufferedReader lines = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                compareRows(table, new NdjsonRows(table, lines));
            }
        }
    }

    // ---------------------------------------------------------------- header and schema

    private void checkHeader(Map<String, Object> header, String layout) {
        same(null, "format", JsonFormat.NAME, header.get("format"));
        same(null, "formatVersion", String.valueOf(JsonFormat.VERSION), text(header.get("formatVersion")));
        same(null, "layout", layout, header.get("layout"));
        encoding = Encoding.of(header.get("encoding"));
        if (header.get("source") instanceof Map<?, ?> src) {
            SchemaModel.Source expected = plan.model().source();
            same(null, "source.fileFormat", expected.fileFormat(), src.get("fileFormat"));
            same(null, "source.charset", expected.charset(), src.get("charset"));
            same(null, "source.codePage", Objects.toString(expected.codePage(), null), text(src.get("codePage")));
        } else {
            differences.add(null, null, "source", "an object", String.valueOf(header.get("source")));
        }
        if (header.containsKey("schema")) {
            compareTrees("schema", expectedSchema(), header.get("schema"));
        }
    }

    /** The schema section the source calls for, rendered as the writer renders it and read back as a tree. */
    private Object expectedSchema() {
        StringWriter text = new StringWriter();
        try (JsonGenerator g = JsonFormat.FACTORY.createGenerator(ObjectWriteContext.empty(), text)) {
            g.writeStartObject();
            JsonWriter.schema(g, plan);
            g.writeEndObject();
        }
        try (JsonParser p = JsonFormat.FACTORY.createParser(JsonFormat.reading(), text.toString())) {
            p.nextToken();
            return ((Map<?, ?>) tree(p)).get("schema");
        }
    }

    private void compareTrees(String path, Object expected, Object actual) {
        if (expected instanceof Map<?, ?> e && actual instanceof Map<?, ?> a) {
            if (!new ArrayList<>(e.keySet()).equals(new ArrayList<>(a.keySet()))) {
                differences.add(
                        null,
                        null,
                        path + "'s properties",
                        e.keySet().toString(),
                        a.keySet().toString());
            }
            for (Map.Entry<?, ?> entry : e.entrySet()) {
                if (a.containsKey(entry.getKey())) {
                    compareTrees(path + "." + entry.getKey(), entry.getValue(), a.get(entry.getKey()));
                }
            }
        } else if (expected instanceof List<?> e && actual instanceof List<?> a) {
            if (e.size() != a.size()) {
                differences.add(null, null, path + "'s length", String.valueOf(e.size()), String.valueOf(a.size()));
            }
            for (int i = 0; i < Math.min(e.size(), a.size()); i++) {
                compareTrees(path + "[" + i + "]", e.get(i), a.get(i));
            }
        } else if (!Objects.equals(expected, actual)) {
            differences.add(null, null, path, String.valueOf(expected), String.valueOf(actual));
        }
    }

    private void same(String table, String what, String expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            differences.add(table, null, what, String.valueOf(expected), String.valueOf(actual));
        }
    }

    // ---------------------------------------------------------------- data

    private void data(JsonParser p) throws IOException {
        expect(p, JsonToken.START_OBJECT, "data");
        int next = 0;
        while (p.nextToken() == JsonToken.PROPERTY_NAME) {
            String name = p.currentName();
            p.nextToken();
            PlannedTable table = next < plan.tables().size() ? plan.tables().get(next) : null;
            if (table == null || !table.name().equals(name)) {
                differences.add(
                        name, null, "the table in data", table == null ? "no further table" : table.name(), name);
                p.skipChildren();
                continue;
            }
            next++;
            expect(p, JsonToken.START_ARRAY, "the rows of " + name);
            compareRows(table, new DocumentRows(table, p));
        }
        for (int i = next; i < plan.tables().size(); i++) {
            differences.add(plan.tables().get(i).name(), null, "the table in data", "present", "missing");
        }
    }

    private void compareRows(PlannedTable table, Rows rows) throws IOException {
        List<RowComparison.Column> columns = new ArrayList<>();
        for (PlannedColumn column : table.columns()) {
            UnaryOperator<Object> stored = column.type() == JsonPlan.JsonType.HYPERLINK
                            && encoding.hyperlinks().equals("object")
                    ? v -> v == null ? null : Hyperlink.parse((String) v)
                    : UnaryOperator.identity();
            columns.add(
                    new RowComparison.Column(column.name(), column.source().type(), column.sourceIndex(), 9, stored));
        }
        try {
            differences.rows(
                    RowComparison.ordered(table.name(), columns, source.rows(table.source()), rows, differences));
        } catch (SQLException e) {
            throw new IllegalStateException("not reached: nothing here is SQL", e);
        }
    }

    /** One table's rows read from the output, each decoded into the values {@link RowComparison} compares. */
    private abstract class Rows implements RowComparison.OutputRows {
        final PlannedTable table;
        final Object[] values;
        long row;

        Rows(PlannedTable table) {
            this.table = table;
            this.values = new Object[table.columns().size()];
        }

        @Override
        public Object value(int column) {
            return values[column];
        }

        /** Decodes one row, already read as a tree. */
        void decode(Object tree) {
            row++;
            Arrays.fill(values, null);
            List<PlannedColumn> columns = table.columns();
            if (encoding.rows().equals("array")) {
                if (!(tree instanceof List<?> list) || list.size() != columns.size()) {
                    differences.add(table.name(), null, "row " + row, columns.size() + " values", describe(tree));
                    Arrays.fill(values, WrongKind.MISSING);
                    return;
                }
                for (int i = 0; i < columns.size(); i++) {
                    values[i] = JsonVerifier.this.decode(columns.get(i), list.get(i));
                }
                return;
            }
            if (!(tree instanceof Map<?, ?> map)) {
                differences.add(table.name(), null, "row " + row, "an object", describe(tree));
                Arrays.fill(values, WrongKind.MISSING);
                return;
            }
            List<String> expectedKeys =
                    columns.stream().map(PlannedColumn::name).toList();
            if (!expectedKeys.equals(new ArrayList<>(map.keySet()))) {
                differences.add(
                        table.name(),
                        null,
                        "the keys of row " + row,
                        String.join(", ", expectedKeys),
                        map.keySet().toString());
            }
            for (int i = 0; i < columns.size(); i++) {
                String name = columns.get(i).name();
                values[i] = map.containsKey(name)
                        ? JsonVerifier.this.decode(columns.get(i), map.get(name))
                        : WrongKind.MISSING;
            }
        }

        private static String describe(Object tree) {
            return tree instanceof List<?> list ? list.size() + " values" : String.valueOf(tree);
        }
    }

    /** The rows of one table's array in the document. */
    private final class DocumentRows extends Rows {
        private final JsonParser p;

        DocumentRows(PlannedTable table, JsonParser p) {
            super(table);
            this.p = p;
        }

        @Override
        public boolean next() {
            JsonToken token = p.nextToken();
            if (token == JsonToken.END_ARRAY) {
                return false;
            }
            decode(tree(p));
            return true;
        }
    }

    /** The lines of one table's ndjson file: exactly one row per line. */
    private final class NdjsonRows extends Rows {
        private final BufferedReader lines;

        NdjsonRows(PlannedTable table, BufferedReader lines) {
            super(table);
            this.lines = lines;
        }

        @Override
        public boolean next() {
            String line;
            try {
                line = lines.readLine();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (line == null) {
                return false;
            }
            try (JsonParser p = JsonFormat.FACTORY.createParser(JsonFormat.reading(), line)) {
                p.nextToken();
                Object tree = tree(p);
                if (p.nextToken() != null) {
                    differences.add(table.name(), null, "line " + (row + 1), "one row", "more than one value");
                }
                decode(tree);
            } catch (JacksonException e) {
                row++;
                differences.add(table.name(), null, "line " + row, "a JSON row", e.getOriginalMessage());
                Arrays.fill(values, WrongKind.MISSING);
            }
            return true;
        }
    }

    // ---------------------------------------------------------------- values

    /**
     * What a value of the output is, where it is not a value of the column's kind: it equals nothing, so the
     * comparison reports it with this description.
     */
    private record WrongKind(String description) {
        static final WrongKind MISSING = new WrongKind("missing");

        @Override
        public String toString() {
            return description;
        }
    }

    /** A JSON number as its text, so no digit is lost before the column's type says how to read it. */
    private record JsonNumber(String text) {
        @Override
        public String toString() {
            return text;
        }
    }

    /** A JSON value decoded as 07 specifies the column's type; the wrong spelling is a {@link WrongKind}. */
    private Object decode(PlannedColumn column, Object json) {
        if (json == null) {
            return null;
        }
        return switch (column.type()) {
            case BOOLEAN -> json instanceof Boolean ? json : wrong("a boolean", json);
            case UINT8, INT16, INT32, COMPLEX_ID -> integer(json);
            case INT64 ->
                encoding.bigint().equals("string")
                        ? (json instanceof String s && PLAIN_NUMBER.matcher(s).matches() ? s : wrong("a string", json))
                        : integer(json);
            case DECIMAL -> {
                String text = encoding.decimals().equals("string")
                        ? (json instanceof String s ? s : null)
                        : (json instanceof JsonNumber n ? n.text() : null);
                yield text != null && PLAIN_NUMBER.matcher(text).matches()
                        ? new BigDecimal(text)
                        : wrong("a plain decimal " + encoding.decimals(), json);
            }
            case FLOAT32 ->
                json instanceof JsonNumber n ? (Object) Float.parseFloat(n.text()) : wrong("a number", json);
            case FLOAT64 ->
                json instanceof JsonNumber n ? (Object) Double.parseDouble(n.text()) : wrong("a number", json);
            case DATETIME -> {
                Pattern format = column.source().type() == AccessType.EXT_DATE_TIME ? EXTENDED_DATE_TIME : DATE_TIME;
                yield json instanceof String s && format.matcher(s).matches()
                        ? s
                        : wrong("a date-time as " + format.pattern(), json);
            }
            case STRING, GUID -> json instanceof String ? json : wrong("a string", json);
            case HYPERLINK -> {
                if (encoding.hyperlinks().equals("object")) {
                    yield json instanceof Map<?, ?> map
                                    && new ArrayList<>(map.keySet())
                                            .equals(List.of("display", "address", "subAddress", "screenTip"))
                                    && map.values().stream().allMatch(v -> v == null || v instanceof String)
                            ? new Hyperlink(
                                    (String) map.get("display"),
                                    (String) map.get("address"),
                                    (String) map.get("subAddress"),
                                    (String) map.get("screenTip"))
                            : wrong("a hyperlink object", json);
                }
                yield json instanceof String ? json : wrong("a string", json);
            }
            case BINARY, OLE -> {
                if (json instanceof String s) {
                    try {
                        yield Base64.getDecoder().decode(s);
                    } catch (IllegalArgumentException e) {
                        yield wrong("base64", json);
                    }
                }
                yield wrong("a base64 string", json);
            }
        };
    }

    private static Object integer(Object json) {
        return json instanceof JsonNumber n && n.text().matches("-?\\d+")
                ? new BigDecimal(n.text())
                : wrong("an integer", json);
    }

    private static WrongKind wrong(String expected, Object json) {
        return new WrongKind("not " + expected + ": " + json);
    }

    // ---------------------------------------------------------------- reading

    /**
     * The value at the parser's current token as plain Java: a map (in document order), a list, a string, a
     * {@link JsonNumber} holding the number's text, a Boolean, or null.
     */
    private static Object tree(JsonParser p) {
        JsonToken token = p.currentToken();
        return switch (token) {
            case START_OBJECT -> {
                Map<String, Object> map = new LinkedHashMap<>();
                while (p.nextToken() == JsonToken.PROPERTY_NAME) {
                    String name = p.currentName();
                    p.nextToken();
                    map.put(name, tree(p));
                }
                yield map;
            }
            case START_ARRAY -> {
                List<Object> list = new ArrayList<>();
                while (p.nextToken() != JsonToken.END_ARRAY) {
                    list.add(tree(p));
                }
                yield list;
            }
            case VALUE_STRING -> p.getString();
            case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> new JsonNumber(p.getString());
            case VALUE_TRUE -> Boolean.TRUE;
            case VALUE_FALSE -> Boolean.FALSE;
            case VALUE_NULL -> null;
            default -> throw new Malformed("a JSON value", String.valueOf(token));
        };
    }

    private static void expect(JsonParser p, JsonToken token, String what) {
        JsonToken found = p.currentToken() == null ? p.nextToken() : p.currentToken();
        if (found != token) {
            throw new Malformed(what + " starting with " + token, String.valueOf(found));
        }
    }

    /** The output isn't shaped as 07 says, so there is nothing further to compare. */
    private static final class Malformed extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final String expected;
        final String found;

        Malformed(String expected, String found) {
            super(expected + ", found " + found, null, false, false);
            this.expected = expected;
            this.found = found;
        }
    }

    private static String text(Object value) {
        return value == null ? null : value.toString();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> cast(Map<?, ?> map) {
        return new LinkedHashMap<>((Map<String, Object>) map);
    }
}
