package com.lytrax.accessconverter.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.healthmarketscience.jackcess.ColumnBuilder;
import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.IndexBuilder;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;
import com.healthmarketscience.jackcess.TableBuilder;
import com.healthmarketscience.jackcess.util.OleBlob;
import com.lytrax.accessconverter.fixtures.CorpusFile;
import com.lytrax.accessconverter.fixtures.GeneratedFixture;
import com.lytrax.accessconverter.report.IssueCode;
import com.lytrax.accessconverter.source.OpenOptions;
import com.lytrax.accessconverter.target.BinaryMode;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.json.JsonFixture;
import com.lytrax.accessconverter.target.json.JsonOptions;
import com.lytrax.accessconverter.target.mysql.MySqlDialect;
import com.lytrax.accessconverter.target.mysql.MySqlFixture;
import com.lytrax.accessconverter.target.sqlite.Sqlite;
import com.lytrax.accessconverter.target.sqlite.SqliteFixture;
import com.lytrax.accessconverter.target.sqlite.SqliteOptions;
import com.lytrax.accessconverter.value.CanonicalText;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * F-11 and 08's OLE acceptance: an OLE column exports its exact stored bytes in every target, whatever the content, and
 * {@code --ole-extract} only adds to them. The ground truth is Jackcess's own raw bytes.
 */
class OleRawBytesPreservedTest {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final Pattern MYSQL_ROW = Pattern.compile("^\\((\\d+), '(?:[^'\\\\]|\\\\.)*', X'([0-9A-F]*)'");

    @TempDir
    Path dir;

    /** blobV2007: 11 rows, every OLE value's SHA-256, by ID. */
    private static Map<Integer, String> jackcessOleHashes() throws IOException {
        Map<Integer, String> hashes = new TreeMap<>();
        try (Database db = new DatabaseBuilder(blob()).setReadOnly(true).open()) {
            for (Row row : db.getTable("Table1")) {
                hashes.put(row.getInt("ID"), CanonicalText.sha256(row.getBytes("ole_data")));
            }
        }
        assertThat(hashes).hasSize(11);
        return hashes;
    }

    private static Path blob() {
        return CorpusFile.get("jackcess/V2007/blobV2007.accdb").file();
    }

    @ParameterizedTest
    @EnumSource(BinaryMode.class)
    void sqliteKeepsEveryOleValueExactly(BinaryMode mode) throws IOException {
        ConvertOptions options = ConvertOptions.DEFAULT.withBinary(mode, true, false);
        Path output = dir.resolve("blob.sqlite3");
        SqliteFixture.Converted converted =
                SqliteFixture.convert(blob(), output, OpenOptions.DEFAULT, options, SqliteOptions.DEFAULT);
        assertThat(converted.verified().matches())
                .as(converted.verified().differences().toString())
                .isTrue();

        Map<Integer, String> expected = jackcessOleHashes();
        Map<Integer, String> actual = new TreeMap<>();
        Map<Integer, Long> sizes = new TreeMap<>();
        try (Sqlite sqlite = converted.open()) {
            for (List<Object> row : sqlite.query("SELECT ID, ole_data FROM Table1")) {
                int id = (Integer) row.get(0);
                switch (mode) {
                    case INLINE -> actual.put(id, CanonicalText.sha256((byte[]) row.get(1)));
                    case FILES ->
                        actual.put(id, CanonicalText.sha256(Files.readAllBytes(dir.resolve((String) row.get(1)))));
                    case OMIT -> sizes.put(id, ((Number) row.get(1)).longValue());
                }
            }
        }
        if (mode == BinaryMode.OMIT) {
            assertThat(sizes)
                    .hasSize(11)
                    .allSatisfy((id, size) -> assertThat(size).isPositive());
            assertThat(converted.issues(IssueCode.BINARY_OMITTED)).isNotEmpty();
        } else {
            assertThat(actual).isEqualTo(expected);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = BinaryMode.class,
            names = {"INLINE", "FILES"})
    void jsonKeepsEveryOleValueExactly(BinaryMode mode) throws IOException {
        for (boolean extract : new boolean[] {false, true}) {
            ConvertOptions options = ConvertOptions.DEFAULT.withBinary(mode, extract, false);
            Path output = dir.resolve("blob-" + extract + ".json");
            JsonFixture.Converted converted =
                    JsonFixture.convert(blob(), output, OpenOptions.DEFAULT, options, JsonOptions.DEFAULT);
            assertThat(converted.verify().matches())
                    .as(converted.verify().differences().toString())
                    .isTrue();

            Map<Integer, String> actual = new TreeMap<>();
            for (JsonNode row : JSON.readTree(output.toFile()).get("data").get("Table1")) {
                JsonNode ole = row.get("ole_data");
                JsonNode raw = extract ? ole.get("raw") : ole;
                byte[] bytes = mode == BinaryMode.INLINE
                        ? Base64.getDecoder().decode(raw.asString())
                        : Files.readAllBytes(dir.resolve(raw.get("file").asString()));
                actual.put(row.get("ID").asInt(), CanonicalText.sha256(bytes));
            }
            assertThat(actual).as("--ole-extract " + extract).isEqualTo(jackcessOleHashes());
        }
    }

    @Test
    void mysqlKeepsEveryOleValueExactly() throws IOException {
        for (MySqlDialect dialect : MySqlDialect.values()) {
            Path output = dir.resolve(dialect + ".sql");
            MySqlFixture.convert(blob(), output, dialect);
            Map<Integer, String> actual = new TreeMap<>();
            try (Stream<String> lines = Files.lines(output, StandardCharsets.UTF_8)) {
                lines.forEach(line -> {
                    Matcher m = MYSQL_ROW.matcher(line);
                    if (m.find()) {
                        actual.put(
                                Integer.parseInt(m.group(1)),
                                CanonicalText.sha256(HexFormat.of().parseHex(m.group(2))));
                    }
                });
            }
            assertThat(actual).as(dialect.name()).isEqualTo(jackcessOleHashes());
        }
    }

    /** 08: Doc extracts to hello.txt / hello ole, Img is raw PNG, Raw (Binary(16)) round-trips, an empty one too. */
    @Test
    void schemaFidelityFilesExtractsAsDescribed() {
        Path source = GeneratedFixture.SCHEMA_FIDELITY.path();
        ConvertOptions options = ConvertOptions.DEFAULT.withBinary(BinaryMode.INLINE, true, false);
        SqliteFixture.Converted converted = SqliteFixture.convert(
                source, dir.resolve("sf.sqlite3"), OpenOptions.DEFAULT, options, SqliteOptions.DEFAULT);
        assertThat(converted.verified().matches())
                .as(converted.verified().differences().toString())
                .isTrue();
        try (Sqlite sqlite = converted.open()) {
            List<List<Object>> rows = sqlite.query("SELECT FileID, Doc__kind, Doc__name, Doc__mime, Doc__content,"
                    + " Img__kind, Img__name, Img__mime, Img__content, quote(Raw), typeof(Doc), length(Doc)"
                    + " FROM Files ORDER BY FileID");
            assertThat(rows).hasSize(3);
            List<Object> first = rows.get(0);
            assertThat(first.subList(1, 4)).containsExactly("package", "hello.txt", null);
            assertThat(new String((byte[]) first.get(4), StandardCharsets.US_ASCII))
                    .isEqualTo("hello ole");
            assertThat(first.subList(5, 9)).containsExactly("raw", null, "image/png", null);
            assertThat(first.get(9)).isEqualTo("X'00112233445566778899AABBCCDDEEFF'");
            // The NULL row stays NULL everywhere
            assertThat(rows.get(1).subList(1, 9)).containsOnlyNulls();
            assertThat(rows.get(1).subList(9, 11)).containsExactly("NULL", "null");
            // An empty Binary is X'', not NULL; an empty OLE value is empty raw bytes, not NULL (F-13b)
            assertThat(rows.get(2).get(9)).isEqualTo("X''");
            assertThat(rows.get(2).get(10)).isEqualTo("blob");
            assertThat(rows.get(2).get(11)).isEqualTo(0);
            assertThat(rows.get(2).subList(1, 5)).containsExactly("raw", null, null, null);
        }
        assertThat(converted.issues(IssueCode.OLE_UNDECODABLE)).isEmpty();
    }

    @Test
    void anEmptyBinaryIsAnEmptyLiteralInTheDumpAndAnEmptyStringInJson() throws IOException {
        Path source = GeneratedFixture.SCHEMA_FIDELITY.path();
        Path dump = dir.resolve("sf.sql");
        MySqlFixture.convert(source, dump, MySqlDialect.MYSQL);
        assertThat(Files.readString(dump)).contains("(3, X'', X'', NULL)");

        Path json = dir.resolve("sf.json");
        JsonFixture.Converted converted = JsonFixture.convert(source, json);
        assertThat(converted.verify().matches()).isTrue();
        JsonNode files = JSON.readTree(json.toFile()).get("data").get("Files");
        assertThat(files.get(2).get("Raw").asString()).isEmpty();
        assertThat(files.get(2).get("Doc").asString()).isEmpty();
        assertThat(files.get(1).get("Raw").isNull()).isTrue();
    }

    /** 08, File-name safety, end to end: a package named ..\..\x.txt is written inside the files directory. */
    @Test
    void aHostilePackageNameStaysInsideTheFilesDirectory() throws IOException, SQLException {
        Path source = dir.resolve("hostile.accdb");
        try (Database db = new DatabaseBuilder(source)
                .setFileFormat(Database.FileFormat.V2010)
                .create()) {
            Table table = new TableBuilder("Docs")
                    .addColumn(new ColumnBuilder("ID", DataType.LONG))
                    .addColumn(new ColumnBuilder("Doc", DataType.OLE))
                    .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME)
                            .addColumns("ID")
                            .setPrimaryKey())
                    .toTable(db);
            table.addRow(1, simplePackage("..\\..\\x.txt", "hostile"));
            table.addRow(2, simplePackage("../../../x.txt", "hostile too"));
        }
        Path out = Files.createDirectories(dir.resolve("out"));
        ConvertOptions options = ConvertOptions.DEFAULT.withBinary(BinaryMode.FILES, true, false);
        SqliteFixture.Converted sqlite = SqliteFixture.convert(
                source, out.resolve("h.sqlite3"), OpenOptions.DEFAULT, options, SqliteOptions.DEFAULT);
        assertThat(sqlite.verified().matches()).isTrue();
        JsonFixture.Converted json =
                JsonFixture.convert(source, out.resolve("h.json"), OpenOptions.DEFAULT, options, JsonOptions.DEFAULT);
        assertThat(json.verify().matches()).isTrue();

        try (Sqlite db = sqlite.open()) {
            assertThat(db.strings("SELECT Doc__content FROM Docs ORDER BY ID"))
                    .containsExactly(
                            "h.sqlite3-files/Docs/Doc__content/1-x.txt", "h.sqlite3-files/Docs/Doc__content/2-x.txt");
        }
        try (Stream<Path> all = Files.walk(dir)) {
            List<Path> files = all.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().equals("x.txt")
                            || p.getFileName().toString().endsWith("-x.txt"))
                    .toList();
            assertThat(files)
                    .hasSize(4)
                    .allSatisfy(p -> assertThat(p.toString()).contains("-files"));
        }
        assertThat(dir.resolve("x.txt")).doesNotExist();
        assertThat(dir.getParent().resolve("x.txt")).doesNotExist();
    }

    private static byte[] simplePackage(String fileName, String content) throws IOException, SQLException {
        try (OleBlob blob = new OleBlob.Builder()
                .setSimplePackageBytes(content.getBytes(StandardCharsets.US_ASCII))
                .setSimplePackageFileName(fileName)
                .setSimplePackageFilePath("C:\\tmp\\" + fileName)
                .toBlob()) {
            return blob.getBytes(1, (int) blob.length());
        }
    }
}
