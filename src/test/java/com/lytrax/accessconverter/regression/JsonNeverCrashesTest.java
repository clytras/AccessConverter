package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.extract.ExtractOptions;
import com.lytrax.accessconverter.extract.SchemaExtractor;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.model.AccessType;
import com.lytrax.accessconverter.model.ColumnModel;
import com.lytrax.accessconverter.model.SchemaModel;
import com.lytrax.accessconverter.model.TableModel;
import com.lytrax.accessconverter.report.Issues;
import com.lytrax.accessconverter.report.Severity;
import com.lytrax.accessconverter.source.AccessSource;
import com.lytrax.accessconverter.source.RowStream;
import com.lytrax.accessconverter.target.json.JsonFixture;
import com.lytrax.accessconverter.target.json.JsonFixture.Converted;
import com.lytrax.accessconverter.target.json.JsonOptions;
import com.lytrax.accessconverter.target.json.JsonOptions.Layout;
import com.lytrax.accessconverter.target.json.JsonOptions.Rows;
import com.lytrax.accessconverter.value.CanonicalText;
import com.lytrax.accessconverter.value.OleValue;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.json.JsonFactory;

/**
 * F-13, F-13a and F-07 on the files the audit found them in: v2's JSON export crashed on the first NULL Single of
 * {@code indexCodesV2010}, on OLE values that aren't simple packages ({@code blobV2007}), and rounded every Single to
 * two decimals. (F-13b, an empty OLE value, and more Singles are in {@code JsonValueFidelityTest}.)
 */
class JsonNeverCrashesTest {

    private static final JsonFactory JSON = new JsonFactory();

    @TempDir
    Path dir;

    @Test
    void nullSinglesStayNullAndTheRestReadBackExactly() throws IOException {
        Path source = CorpusFile.get("jackcess/V2010/indexCodesV2010.accdb").file();
        Converted converted = convert(source, "indexCodes");

        long nulls = 0;
        long values = 0;
        long moreThanTwoDecimals = 0;
        for (Column column : columns(source, AccessType.FLOAT)) {
            List<String> written = column(converted, column);
            for (int i = 0; i < written.size(); i++) {
                Float expected = (Float) column.values.get(i);
                String json = written.get(i);
                if (expected == null) {
                    assertThat(json).isNull();
                    nulls++;
                    continue;
                }
                values++;
                // The number's own text, exactly Java's shortest round-trip form, never rounded (F-07)
                assertThat(json).isEqualTo(Float.toString(expected));
                assertThat(Float.parseFloat(json)).isEqualTo(expected);
                if (json.contains(".") && !json.contains("E") && json.length() - json.indexOf('.') - 1 > 2) {
                    moreThanTwoDecimals++;
                }
            }
        }
        assertThat(nulls).as("NULL Singles, where v2 crashed (F-13a)").isPositive();
        assertThat(values).isPositive();
        assertThat(moreThanTwoDecimals)
                .as("Singles v2 would have rounded (F-07)")
                .isPositive();
        assertThat(converted.verify().matches()).isTrue();
    }

    @Test
    void everyOleValueIsItsExactBytes() throws IOException {
        Path source = CorpusFile.get("jackcess/V2007/blobV2007.accdb").file();
        Converted converted = convert(source, "blob");

        long ole = 0;
        for (Column column : columns(source, AccessType.OLE)) {
            List<String> written = column(converted, column);
            assertThat(written).hasSize(column.values.size());
            for (int i = 0; i < written.size(); i++) {
                OleValue expected = (OleValue) column.values.get(i);
                if (expected == null) {
                    assertThat(written.get(i)).isNull();
                    continue;
                }
                ole++;
                byte[] decoded = Base64.getDecoder().decode(written.get(i));
                assertThat(CanonicalText.sha256(decoded)).isEqualTo(CanonicalText.sha256(expected.raw()));
            }
        }
        assertThat(ole)
                .as("OLE values, where v2 crashed on the first that wasn't a package (F-13)")
                .isPositive();
        assertThat(converted.issues().list()).noneMatch(i -> i.severity() == Severity.ERROR);
        assertThat(converted.verify().matches()).isTrue();
    }

    private Converted convert(Path source, String name) {
        return JsonFixture.convert(
                source,
                dir.resolve(name),
                JsonOptions.DEFAULT.withLayout(Layout.NDJSON).withRows(Rows.ARRAY));
    }

    /** One source column: its table, its position among the written columns, and every value, in stream order. */
    private record Column(TableModel table, ColumnModel column, List<Object> values) {}

    private static List<Column> columns(Path source, AccessType type) throws IOException {
        List<Column> found = new ArrayList<>();
        try (AccessSource db = AccessSource.open(source)) {
            SchemaModel model = SchemaExtractor.extract(db, ExtractOptions.ALL, new Issues());
            for (TableModel table : model.tables()) {
                for (ColumnModel column : table.columns()) {
                    if (column.type() != type) {
                        continue;
                    }
                    List<Object> values = new ArrayList<>();
                    RowStream rows = db.rows(table);
                    while (rows.hasNext()) {
                        values.add(rows.next()[column.ordinal()]);
                    }
                    found.add(new Column(table, column, values));
                }
            }
        }
        return found;
    }

    /** The column's values in the ndjson output as the file spells them: a number's own text, a string, or null. */
    private static List<String> column(Converted converted, Column column) throws IOException {
        var table = converted.table(column.table.name());
        int position =
                table.columns().indexOf(table.column(column.column.name()).orElseThrow());
        List<String> values = new ArrayList<>();
        for (String line : Files.readAllLines(converted.output().resolve(table.file()), StandardCharsets.UTF_8)) {
            try (JsonParser p = JSON.createParser(ObjectReadContext.empty(), line)) {
                p.nextToken(); // the row's array
                for (int i = 0; i <= position; i++) {
                    p.nextToken();
                    p.skipChildren();
                }
                values.add(p.currentToken() == JsonToken.VALUE_NULL ? null : p.getString());
            }
        }
        return values;
    }
}
