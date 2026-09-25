package io.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.DateTimeType;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import io.lytrax.accessconverter.report.Issue;
import io.lytrax.accessconverter.report.IssueCode;
import io.lytrax.accessconverter.target.json.JsonFixture;
import io.lytrax.accessconverter.target.json.JsonFixture.Converted;
import io.lytrax.accessconverter.target.json.JsonOptions;
import io.lytrax.accessconverter.target.json.JsonOptions.Hyperlinks;
import io.lytrax.accessconverter.target.json.JsonOptions.Layout;
import io.lytrax.accessconverter.target.json.JsonOptions.Rows;
import io.lytrax.accessconverter.target.json.JsonSchemaCheck;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * 07's value encoding, value by value, on a database built for it: the corpus has no hyperlink, no NaN and no Large
 * Number past 2^53. F-01 (unsigned Byte), F-07 and F-13a (Single: shortest round-trip text, NULL stays null, never
 * rounded to two decimals), F-10 (Date/Time Extended's 7 digits, Large Number), exact decimals, and text with a
 * non-ASCII letter and a slash as they are ({@code /} not escaped), and OLE values that are empty (F-13b) or not an OLE
 * package at all (F-13) as base64 of their exact bytes.
 *
 * <p>A lone surrogate can't be built this way: Jackcess stores it as U+FFFD, so {@code JsonValuesTest} covers it.
 */
class JsonValueFidelityTest {

    @TempDir
    static Path dir;

    static Path source;

    @BeforeAll
    static void build() throws IOException {
        source = dir.resolve("values.accdb");
        try (Database db = new DatabaseBuilder(source)
                .setFileFormat(Database.FileFormat.V2019)
                .create()) {
            db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
            db.setEvaluateExpressions(false);
            Table table = new TableBuilder("Values")
                    .addColumn(new ColumnBuilder("ID", DataType.LONG).setAutoNumber(true))
                    .addColumn(new ColumnBuilder("Rating", DataType.BYTE))
                    .addColumn(new ColumnBuilder("Big", DataType.BIG_INT))
                    .addColumn(new ColumnBuilder("Price", DataType.MONEY))
                    .addColumn(new ColumnBuilder("Dec", DataType.NUMERIC)
                            .setPrecision(28)
                            .setScale(10))
                    .addColumn(new ColumnBuilder("Single", DataType.FLOAT))
                    .addColumn(new ColumnBuilder("Dbl", DataType.DOUBLE))
                    .addColumn(new ColumnBuilder("When", DataType.SHORT_DATE_TIME))
                    .addColumn(new ColumnBuilder("Ext", DataType.EXT_DATE_TIME))
                    .addColumn(new ColumnBuilder("Link", DataType.MEMO).setHyperlink(true))
                    .addColumn(new ColumnBuilder("Txt", DataType.MEMO))
                    .addColumn(new ColumnBuilder("Bin", DataType.BINARY).setLength(8))
                    .addColumn(new ColumnBuilder("Ole", DataType.OLE))
                    .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                            .addColumns("ID")
                            .setPrimaryKey())
                    .toTable(db);
            table.addRow(
                    Column.AUTO_NUMBER,
                    (byte) 255,
                    9_007_199_254_740_993L,
                    new BigDecimal("922337203685477.5807"),
                    new BigDecimal("123456789012345678.0123456789"),
                    0.1f,
                    0.1 + 0.2,
                    LocalDateTime.of(2024, 1, 2, 3, 4, 5, 678_000_000),
                    LocalDateTime.of(2021, 6, 14, 22, 45, 12, 345_678_900),
                    "Google#https://google.com/a/b#",
                    "café / slash",
                    new byte[0],
                    new byte[0]);
            table.addRow(
                    Column.AUTO_NUMBER,
                    (byte) 200,
                    Long.MIN_VALUE,
                    new BigDecimal("-922337203685477.5808"),
                    new BigDecimal("-0.0000000001"),
                    Float.MIN_VALUE,
                    Double.MIN_VALUE,
                    LocalDateTime.of(100, 1, 1, 0, 0),
                    LocalDateTime.of(1, 1, 1, 0, 0),
                    "Doc#C:\\docs\\a.doc#Sheet1!A1#tip # with a hash#",
                    "😀 \"quoted\" \\back\\slash\u0001\t\n",
                    new byte[] {0, 1, (byte) 0xFF},
                    new byte[] {(byte) 0x89, 'P', 'N', 'G'});
            table.addRow(
                    Column.AUTO_NUMBER,
                    (byte) 0,
                    null,
                    null,
                    null,
                    Float.NaN,
                    Double.POSITIVE_INFINITY,
                    LocalDateTime.of(9999, 12, 31, 23, 59, 59),
                    null,
                    "plain text",
                    "",
                    null,
                    null);
            table.addRow(Column.AUTO_NUMBER, null, null, null, null, null, null, null, null, null, null, null, null);
        }
    }

    @Test
    void everyValueIsSpelledAs07Says() throws IOException {
        Converted converted = convert(JsonOptions.DEFAULT, "default.json");
        String json = Files.readString(converted.output(), StandardCharsets.UTF_8);

        assertThat(rows(json))
                .containsExactly(
                        "{\"ID\":1,\"Rating\":255,\"Big\":9007199254740993,\"Price\":922337203685477.5807,"
                                + "\"Dec\":123456789012345678.0123456789,\"Single\":0.1,\"Dbl\":0.30000000000000004,"
                                + "\"When\":\"2024-01-02T03:04:05.678\",\"Ext\":\"2021-06-14T22:45:12.3456789\","
                                + "\"Link\":\"Google#https://google.com/a/b#\",\"Txt\":\"café / slash\",\"Bin\":\"\",\"Ole\":\"\"}",
                        "{\"ID\":2,\"Rating\":200,\"Big\":-9223372036854775808,\"Price\":-922337203685477.5808,"
                                + "\"Dec\":-0.0000000001,\"Single\":1.4E-45,\"Dbl\":4.9E-324,"
                                + "\"When\":\"0100-01-01T00:00:00\",\"Ext\":\"0001-01-01T00:00:00.0000000\","
                                + "\"Link\":\"Doc#C:\\\\docs\\\\a.doc#Sheet1!A1#tip # with a hash#\","
                                + "\"Txt\":\"😀 \\\"quoted\\\" \\\\back\\\\slash\\u0001\\t\\n\",\"Bin\":\"AAH/\",\"Ole\":\"iVBORw==\"}",
                        "{\"ID\":3,\"Rating\":0,\"Big\":null,\"Price\":null,\"Dec\":null,\"Single\":null,\"Dbl\":null,"
                                + "\"When\":\"9999-12-31T23:59:59\",\"Ext\":null,\"Link\":\"plain text\",\"Txt\":\"\","
                                + "\"Bin\":null,\"Ole\":null}",
                        "{\"ID\":4,\"Rating\":null,\"Big\":null,\"Price\":null,\"Dec\":null,\"Single\":null,"
                                + "\"Dbl\":null,\"When\":null,\"Ext\":null,\"Link\":null,\"Txt\":null,\"Bin\":null,\"Ole\":null}");
        // Emoji are UTF-8, not escaped surrogate pairs
        assertThat(json.getBytes(StandardCharsets.UTF_8)).containsSequence("😀".getBytes(StandardCharsets.UTF_8));
        assertThat(converted.verify().differences()).isEmpty();
        assertThat(JsonSchemaCheck.errors(converted.output())).isEmpty();
    }

    @Test
    void nonFiniteNumbersAreReported() {
        Converted converted = convert(JsonOptions.DEFAULT, "reported.json");

        assertThat(converted.issues(IssueCode.DOUBLE_NON_FINITE))
                .extracting(Issue::object, Issue::samples)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("Dbl", List.of("Infinity")),
                        org.assertj.core.groups.Tuple.tuple("Single", List.of("NaN")));
    }

    @Test
    void largeNumbersAndDecimalsCanBeStrings() throws IOException {
        Converted converted = convert(JsonOptions.DEFAULT.withStrings(), "strings.json");
        String json = Files.readString(converted.output(), StandardCharsets.UTF_8);

        assertThat(rows(json).get(0))
                .contains(
                        "\"Big\":\"9007199254740993\"",
                        "\"Price\":\"922337203685477.5807\"",
                        "\"Dec\":\"123456789012345678.0123456789\"");
        assertThat(rows(json).get(1)).contains("\"Big\":\"-9223372036854775808\"", "\"Dec\":\"-0.0000000001\"");
        assertThat(converted.verify().differences()).isEmpty();
        assertThat(JsonSchemaCheck.errors(converted.output())).isEmpty();
    }

    @Test
    void hyperlinksCanBeObjects() throws IOException {
        Converted converted = convert(JsonOptions.DEFAULT.withHyperlinks(Hyperlinks.OBJECT), "links.json");
        List<String> rows = rows(Files.readString(converted.output(), StandardCharsets.UTF_8));

        assertThat(rows.get(0))
                .contains("\"Link\":{\"display\":\"Google\",\"address\":\"https://google.com/a/b\",\"subAddress\":null,"
                        + "\"screenTip\":null}");
        assertThat(rows.get(1))
                .contains("\"Link\":{\"display\":\"Doc\",\"address\":\"C:\\\\docs\\\\a.doc\",\"subAddress\":"
                        + "\"Sheet1!A1\",\"screenTip\":\"tip # with a hash\"}");
        assertThat(rows.get(2))
                .contains("\"Link\":{\"display\":\"plain text\",\"address\":null,\"subAddress\":null,"
                        + "\"screenTip\":null}");
        assertThat(converted.table("Values").column("Link").orElseThrow().type().wireName())
                .isEqualTo("hyperlink");
        assertThat(converted.verify().differences()).isEmpty();
        assertThat(JsonSchemaCheck.errors(converted.output())).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(Rows.class)
    void theNdjsonLayoutHoldsTheSameValuesOneRowPerLine(Rows form) throws IOException {
        Converted converted =
                convert(JsonOptions.DEFAULT.withLayout(Layout.NDJSON).withRows(form), "nd-" + form);
        List<String> lines = Files.readAllLines(converted.output().resolve("Values.ndjson"), StandardCharsets.UTF_8);

        assertThat(lines).hasSize(4);
        assertThat(lines.get(0)).startsWith(form == Rows.OBJECT ? "{\"ID\":1,\"Rating\":255," : "[1,255,");
        assertThat(Files.readString(converted.output().resolve("Values.ndjson")))
                .endsWith("\n");
        assertThat(converted.verify().differences()).isEmpty();
        assertThat(JsonSchemaCheck.errors(converted.output())).isEmpty();
    }

    private static Converted convert(JsonOptions options, String name) {
        return JsonFixture.convert(source, dir.resolve(name), options);
    }

    /** The data rows of a document: one per line, as the writer lays them out. */
    private static List<String> rows(String document) {
        return document.lines()
                .map(String::strip)
                .filter(l -> l.startsWith("{\"ID\""))
                .map(l -> l.endsWith(",") ? l.substring(0, l.length() - 1) : l)
                .toList();
    }
}
