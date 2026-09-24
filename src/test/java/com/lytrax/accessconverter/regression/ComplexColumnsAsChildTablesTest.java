package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.complex.Attachment;
import com.healthmarketscience.jackcess.complex.ComplexDataType;
import com.healthmarketscience.jackcess.complex.ComplexValue;
import com.healthmarketscience.jackcess.complex.ComplexValueForeignKey;
import com.healthmarketscience.jackcess.complex.SingleValue;
import com.healthmarketscience.jackcess.complex.Version;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.target.BinaryMode;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.json.JsonFixture;
import com.lytrax.accessconverter.target.json.JsonOptions;
import com.lytrax.accessconverter.target.sqlite.Sqlite;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture;
import com.lytrax.accessconverter.target.sqlite.SqliteOptions;
import com.lytrax.accessconverter.value.CanonicalText;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * F-12 and F-16, 08's complex-column acceptance: attachments and multi-value columns become child tables keyed by
 * Access's complex value id in the SQL targets, and arrays in JSON. Every value is compared with what Jackcess itself
 * reads from the database, not with what the converter read: row counts, attachment data hashes, multi-values and
 * version history.
 */
class ComplexColumnsAsChildTablesTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    @TempDir
    Path dir;

    @ParameterizedTest
    @ValueSource(
            strings = {
                "jackcess/V2010/complexDataV2010.accdb",
                "jackcess/V2007/complexDataV2007.accdb",
                "jackcess/V2007/blobV2007.accdb",
                "mdb-reader/test/data/V2016/attachments.accdb"
            })
    void childTablesHoldExactlyJackcesssValues(String id) throws IOException {
        Path source = CorpusFile.get(id).file();
        ConvertOptions options = ConvertOptions.DEFAULT.withBinary(BinaryMode.INLINE, false, true);
        SqliteFixture.Converted converted = SqliteFixture.convert(
                source, dir.resolve("out.sqlite3"), OpenOptions.DEFAULT, options, SqliteOptions.DEFAULT);
        assertThat(converted.verified().matches())
                .as(converted.verified().differences().toString())
                .isTrue();

        Map<String, List<String>> expected = jackcessValues(source);
        assertThat(expected).isNotEmpty();
        try (Sqlite sqlite = converted.open()) {
            sqlite.assertIsConsistent();
            for (Map.Entry<String, List<String>> column : expected.entrySet()) {
                String[] parts = column.getKey().split("\\.", 2);
                String child = parts[0] + "_" + parts[1];
                List<String> actual = new ArrayList<>();
                for (List<Object> row : sqlite.query("SELECT * FROM \"" + child + "\"")) {
                    actual.add(childRow(row));
                }
                actual.sort(Comparator.naturalOrder());
                assertThat(actual).as(child).isEqualTo(column.getValue());
            }
        }
        assertThat(converted.issues(IssueCode.INDEX_SKIPPED_COMPLEX_COLUMN)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"jackcess/V2010/complexDataV2010.accdb", "jackcess/V2007/blobV2007.accdb"})
    void jsonInlinesTheSameValuesInEachRow(String id) throws IOException {
        Path source = CorpusFile.get(id).file();
        ConvertOptions options = ConvertOptions.DEFAULT.withBinary(BinaryMode.INLINE, false, true);
        JsonFixture.Converted converted =
                JsonFixture.convert(source, dir.resolve("out.json"), OpenOptions.DEFAULT, options, JsonOptions.DEFAULT);
        assertThat(converted.verify().matches())
                .as(converted.verify().differences().toString())
                .isTrue();

        JsonNode data = JSON.readTree(converted.output().toFile()).get("data");
        Map<String, List<List<String>>> expected = jackcessCells(source);
        for (Map.Entry<String, List<List<String>>> column : expected.entrySet()) {
            String[] parts = column.getKey().split("\\.", 2);
            List<List<String>> actual = new ArrayList<>();
            for (JsonNode row : data.get(parts[0])) {
                List<String> cell = new ArrayList<>();
                for (JsonNode item : row.get(parts[1])) {
                    cell.add(jsonItem(item));
                }
                actual.add(cell);
            }
            assertThat(actual).as(column.getKey()).isEqualTo(column.getValue());
        }
    }

    @Test
    void versionHistoryIsSkippedUnlessAskedFor() {
        Path source = CorpusFile.get("jackcess/V2010/complexDataV2010.accdb").file();
        SqliteFixture.Converted converted = SqliteFixture.convert(source, dir.resolve("default.sqlite3"));

        assertThat(converted.plan().tables())
                .extracting(t -> t.name())
                .containsExactly("Table1", "Table1_attach-data", "Table1_multi-value-data");
        assertThat(converted.issues(IssueCode.VERSION_HISTORY_SKIPPED))
                .singleElement()
                .satisfies(i -> assertThat(i.message()).contains("--include-version-history"));
    }

    // ---------------------------------------------------------------- Jackcess's own reading

    /** Per {@code Table.Column}: one line per value, {@code complexId|valueId|fields}, sorted. */
    private static Map<String, List<String>> jackcessValues(Path file) throws IOException {
        Map<String, List<String>> values = new TreeMap<>();
        try (Database db = CorpusFileOpen.open(file)) {
            for (String name : db.getTableNames()) {
                Table table = db.getTable(name);
                for (Column column : table.getColumns()) {
                    if (column.getType() != DataType.COMPLEX_TYPE || column.getComplexInfo() == null) {
                        continue;
                    }
                    ComplexDataType kind = column.getComplexInfo().getType();
                    if (kind == ComplexDataType.UNSUPPORTED) {
                        continue;
                    }
                    List<String> lines = new ArrayList<>();
                    for (Row row : table) {
                        ComplexValueForeignKey key = (ComplexValueForeignKey) row.get(column.getName());
                        if (key == null) {
                            continue;
                        }
                        for (ComplexValue value : values(key, kind)) {
                            lines.add(key.get() + "|" + value.getId().get() + "|" + fields(value));
                        }
                    }
                    lines.sort(Comparator.naturalOrder());
                    values.put(name + "." + column.getName(), lines);
                }
            }
        }
        return values;
    }

    /** Per {@code Table.Column}: each row's values in primary-key order, each value's fields, in value-id order. */
    private static Map<String, List<List<String>>> jackcessCells(Path file) throws IOException {
        Map<String, List<List<String>>> cells = new TreeMap<>();
        try (Database db = CorpusFileOpen.open(file)) {
            for (String name : db.getTableNames()) {
                Table table = db.getTable(name);
                for (Column column : table.getColumns()) {
                    if (column.getType() != DataType.COMPLEX_TYPE
                            || column.getComplexInfo() == null
                            || column.getComplexInfo().getType() == ComplexDataType.UNSUPPORTED) {
                        continue;
                    }
                    List<List<String>> rows = new ArrayList<>();
                    Cursor cursor = CursorBuilder.createCursor(table.getPrimaryKeyIndex());
                    for (Row row : cursor) {
                        ComplexValueForeignKey key = (ComplexValueForeignKey) row.get(column.getName());
                        List<String> cell = new ArrayList<>();
                        for (ComplexValue value :
                                values(key, column.getComplexInfo().getType())) {
                            cell.add(fields(value));
                        }
                        rows.add(cell);
                    }
                    cells.put(name + "." + column.getName(), rows);
                }
            }
        }
        return cells;
    }

    private static List<ComplexValue> values(ComplexValueForeignKey key, ComplexDataType kind) throws IOException {
        List<ComplexValue> values = new ArrayList<>(
                switch (kind) {
                    case ATTACHMENT -> key.getAttachments();
                    case MULTI_VALUE -> key.getMultiValues();
                    case VERSION_HISTORY -> key.getVersions();
                    case UNSUPPORTED -> List.<ComplexValue>of();
                });
        values.sort(Comparator.comparingInt(v -> v.getId().get()));
        return values;
    }

    private static String fields(ComplexValue value) throws IOException {
        return switch (value) {
            case Attachment a ->
                a.getFileName() + "|" + a.getFileType() + "|"
                        + (a.getFileData() == null ? "null" : CanonicalText.sha256(a.getFileData()));
            case SingleValue s -> String.valueOf(s.get());
            case Version v -> v.getValue() + "|" + date(v.getModifiedLocalDate());
            default -> throw new IllegalStateException("unexpected " + value);
        };
    }

    private static String date(LocalDateTime t) {
        return t == null ? "null" : t.toString();
    }

    // ---------------------------------------------------------------- the outputs

    /** A SQLite child row as Jackcess's line: ref, id, then the payload the same way. */
    private static String childRow(List<Object> row) {
        String id = String.valueOf(row.get(0));
        String ref = String.valueOf(row.get(1));
        String fields;
        if (row.size() == 9) { // an attachment: id, ref, name, type, data, size, url, timestamp, flags
            byte[] data = (byte[]) row.get(4);
            fields = row.get(2) + "|" + row.get(3) + "|" + (data == null ? "null" : CanonicalText.sha256(data));
            assertThat(row.get(5)).isEqualTo(data == null ? null : data.length);
        } else if (row.size() == 4) { // a version: id, ref, value, modified
            fields = row.get(2) + "|"
                    + (row.get(3) == null ? "null" : parsed(row.get(3).toString()));
        } else {
            fields = String.valueOf(row.get(2));
        }
        return ref + "|" + id + "|" + fields;
    }

    private static String jsonItem(JsonNode item) {
        if (item.isObject() && item.has("fileName")) {
            JsonNode data = item.get("data");
            return item.get("fileName").asString() + "|" + item.get("fileType").asString() + "|"
                    + (data.isNull()
                            ? "null"
                            : CanonicalText.sha256(Base64.getDecoder().decode(data.asString())));
        }
        if (item.isObject()) {
            return item.get("value").asString() + "|"
                    + parsed(item.get("modified").asString());
        }
        return item.asString();
    }

    private static String parsed(String dateTime) {
        return LocalDateTime.parse(dateTime.replace(' ', 'T')).toString();
    }

    /** Opens a corpus file with Jackcess directly, as the ground truth. */
    private static final class CorpusFileOpen {
        static Database open(Path file) throws IOException {
            assertThat(Files.isRegularFile(file)).isTrue();
            return new com.healthmarketscience.jackcess.DatabaseBuilder(file)
                    .setReadOnly(true)
                    .open();
        }
    }
}
