package com.lytrax.accessconverter.target.json;

import static org.assertj.core.api.Assertions.assertThat;

import com.lytrax.accessconverter.fixtures.CorpusCase;
import com.lytrax.accessconverter.report.Severity;
import com.lytrax.accessconverter.target.BinaryMode;
import com.lytrax.accessconverter.target.ConvertOptions;
import com.lytrax.accessconverter.target.json.JsonFixture.Converted;
import com.lytrax.accessconverter.target.json.JsonOptions.Hyperlinks;
import com.lytrax.accessconverter.target.json.JsonOptions.Layout;
import com.lytrax.accessconverter.target.json.JsonOptions.Rows;
import com.lytrax.accessconverter.verify.VerifyResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * 07's acceptance criteria over every database of tiers A, B and D: each exports without an error (v2 crashed on
 * non-package and empty OLE values and on NULL Singles: F-13, F-13a), the output validates against the published JSON
 * Schema, {@code verify} finds no difference from the source, and a second export is byte-identical (03,
 * Determinism).
 *
 * <p>Twice per database: the default document, and an ndjson directory with every other spelling (array rows,
 * Large Numbers and decimals as strings, hyperlinks as objects), so each option meets every value in the corpus.
 */
class JsonCorpusTest {

    @TempDir
    static Path dir;

    enum Variant {
        DOCUMENT(JsonOptions.DEFAULT, ConvertOptions.DEFAULT),
        /** Every alternative spelling, and 08's options: OLE objects, byte counts, version history. */
        NDJSON_ALTERNATIVES(
                JsonOptions.DEFAULT
                        .withLayout(Layout.NDJSON)
                        .withRows(Rows.ARRAY)
                        .withStrings()
                        .withHyperlinks(Hyperlinks.OBJECT),
                ConvertOptions.DEFAULT.withBinary(BinaryMode.OMIT, true, true));

        final JsonOptions options;
        final ConvertOptions convert;

        Variant(JsonOptions options, ConvertOptions convert) {
            this.options = options;
            this.convert = convert;
        }
    }

    static Stream<Arguments> cases() {
        return Stream.of(Variant.values())
                .flatMap(variant -> CorpusCase.databases().map(database -> Arguments.of(variant, database)));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("cases")
    void exportsValidatesAndVerifies(Variant variant, CorpusCase database) throws IOException {
        Converted first = convert(variant, database, "-1");

        assertThat(first.issues().list())
                .as("errors in the conversion report")
                .noneMatch(issue -> issue.severity() == Severity.ERROR);
        List<String> errors = JsonSchemaCheck.errors(first.output());
        assertThat(errors).as("violations of the published JSON Schema").isEmpty();
        VerifyResult verified = first.verify();
        assertThat(verified.differences()).as("verify").isEmpty();
        assertThat(verified.tables())
                .as("every table was compared")
                .hasSize(first.plan().tables().size());

        Converted second = convert(variant, database, "-2");
        assertSameOutput(first.output(), second.output());
    }

    private static Converted convert(Variant variant, CorpusCase database, String suffix) {
        String name = variant + "_" + database.id().replaceAll("[^A-Za-z0-9.]", "_") + suffix
                + (variant.options.layout() == Layout.DOCUMENT ? ".json" : "");
        return JsonFixture.convert(
                database.file(), dir.resolve(name), database.options(), variant.convert, variant.options);
    }

    static void assertSameOutput(Path first, Path second) throws IOException {
        if (!Files.isDirectory(first)) {
            assertThat(Files.mismatch(first, second)).isEqualTo(-1L);
            return;
        }
        List<String> names;
        try (Stream<Path> files = Files.list(first)) {
            names = files.map(f -> f.getFileName().toString()).sorted().toList();
        }
        try (Stream<Path> files = Files.list(second)) {
            assertThat(files.map(f -> f.getFileName().toString()).sorted().toList())
                    .isEqualTo(names);
        }
        for (String name : names) {
            assertThat(Files.mismatch(first.resolve(name), second.resolve(name)))
                    .as(name)
                    .isEqualTo(-1L);
        }
    }
}
