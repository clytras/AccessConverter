package io.lytrax.accessconverter.target.json;

import io.lytrax.accessconverter.Golden;
import io.lytrax.accessconverter.fixtures.Access97Fixture;
import io.lytrax.accessconverter.fixtures.CorpusFile;
import io.lytrax.accessconverter.fixtures.GeneratedFixture;
import io.lytrax.accessconverter.source.OpenOptions;
import io.lytrax.accessconverter.target.ConvertOptions;
import io.lytrax.accessconverter.target.json.JsonOptions.Layout;
import io.lytrax.accessconverter.target.json.JsonOptions.Rows;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Golden files for the JSON export (09, Test layers): the exact document, compared byte for byte in both locale runs
 * on three operating systems. Regenerate with {@code ./mvnw test -Dgolden.update=true} and review the diff.
 */
class JsonGoldenTest {

    @TempDir
    static Path dir;

    /** Small databases whose whole document is pinned. */
    static Stream<Case> documents() {
        return Stream.of(
                generated(GeneratedFixture.TEXT_EDGE),
                access97(Access97Fixture.GR97),
                corpus("jackcess/V2010/calcFieldV2010.accdb"),
                corpus("jackcess/V2019/extDateV2019.accdb"),
                corpus("mdb-reader/test/data/V2016/bigint.accdb"),
                corpus("jackcess/V2007/linkedV2007.accdb"),
                corpus("jackcess/V2007/unsupportedFieldsV2007.accdb"),
                corpus("mdb-reader/test/data/V2016/attachments.accdb"));
    }

    /** Databases whose header and schema section are pinned, but whose data is too large for a golden file. */
    static Stream<Case> schemas() {
        return Stream.of(
                generated(GeneratedFixture.SCHEMA_FIDELITY),
                corpus("jackcess/V1997/indexCodesV1997.mdb"),
                corpus("jackcess/V2007/blobV2007.accdb"),
                corpus("jackcess/V2010/common1V2010.accdb"));
    }

    /** The other spellings, as an ndjson directory: every file, one after the other. */
    static Stream<Case> alternatives() {
        return Stream.of(
                corpus("jackcess/V2010/calcFieldV2010.accdb"), corpus("mdb-reader/test/data/V2016/bigint.accdb"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("documents")
    void theWholeDocumentMatchesItsGoldenFile(Case database) {
        Path output = convert(database, JsonOptions.DEFAULT, ".json");
        Golden.assertMatches("json/" + database.id() + ".json", read(output));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("schemas")
    void theSchemaMatchesItsGoldenFile(Case database) {
        String document = read(convert(database, JsonOptions.DEFAULT, ".json"));
        int data = document.indexOf("\n  \"data\": {");
        Golden.assertMatches("json/" + database.id() + ".schema.json", document.substring(0, data + 1));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("alternatives")
    void theNdjsonLayoutWithEveryAlternativeSpellingMatchesItsGoldenFile(Case database) throws IOException {
        JsonOptions options = JsonOptions.DEFAULT
                .withLayout(Layout.NDJSON)
                .withRows(Rows.ARRAY)
                .withStrings();
        Path output = convert(database, options, "-ndjson");
        StringBuilder text = new StringBuilder();
        List<Path> files;
        // schema.json first, then by name as a plain string: Path's own order ignores case on Windows only
        try (Stream<Path> listed = Files.list(output)) {
            files = listed.sorted(Comparator.comparing(
                                    (Path f) -> !f.getFileName().toString().equals(JsonFormat.NDJSON_SCHEMA_FILE))
                            .thenComparing(f -> f.getFileName().toString()))
                    .toList();
        }
        for (Path file : files) {
            text.append("==> ").append(file.getFileName()).append(" <==\n").append(read(file));
        }
        Golden.assertMatches("json/" + database.id() + ".ndjson.txt", text.toString());
    }

    private static Path convert(Case database, JsonOptions options, String suffix) {
        Path output = dir.resolve(database.id().replaceAll("[^A-Za-z0-9.]", "_") + suffix);
        return JsonFixture.convert(database.file(), output, database.options(), ConvertOptions.DEFAULT, options)
                .output();
    }

    private static String read(Path file) {
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** @param id the golden file's path under {@code golden/json/}, which is the fixture's corpus id */
    record Case(String id, Path file, OpenOptions options) {
        @Override
        public String toString() {
            return id;
        }
    }

    private static Case generated(GeneratedFixture fixture) {
        return new Case("generated/" + fixture.fileName(), fixture.path(), OpenOptions.DEFAULT);
    }

    private static Case access97(Access97Fixture fixture) {
        return new Case("access97/" + fixture.file().getFileName(), fixture.file(), fixture.openOptions());
    }

    private static Case corpus(String id) {
        CorpusFile file = CorpusFile.get(id);
        return new Case(file.id(), file.file(), file.openOptions());
    }
}
